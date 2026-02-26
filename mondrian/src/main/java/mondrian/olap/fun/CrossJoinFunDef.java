/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2020 Hitachi Vantara and others
// Copyright (C) 2021 Topsoft
// Copyright (c) 2021-2022 Sergei Semenkov
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.DummyExp;
import mondrian.calc.ExpCompiler;
import mondrian.calc.IterCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleCursor;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractIterCalc;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.AbstractTupleCursor;
import mondrian.calc.impl.AbstractTupleIterable;
import mondrian.calc.impl.DelegatingTupleList;
import mondrian.calc.impl.ListTupleList;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ParameterExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Parameter;
import mondrian.olap.Query;
import mondrian.olap.ResultStyleException;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.Validator;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.SqlConstraintUtils;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.dependency.CrossJoinDependencyPrunerV2;
import mondrian.rolap.sql.dependency.DependencyPruningContext;
import mondrian.rolap.sql.dependency.DependencyRegistry;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.util.CancellationChecker;
import mondrian.util.CartesianProductList;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Definition of the <code>CrossJoin</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class CrossJoinFunDef extends FunDefBase {
  private static final Logger LOGGER = LogManager.getLogger( CrossJoinFunDef.class );
  // Conservative caps for local/initial rollout of nested interpreter-path
  // pruning propagation. We only materialize modest intermediate tuple sets.
  private static final long INTERPRETER_PRUNING_PROPAGATION_MAX_TUPLES = 50000L;
  private static final int INTERPRETER_PRUNING_PROPAGATION_MAX_ARITY = 8;
  private static final long INTERPRETER_LIMIT_PROBE_MAX_TUPLES = 50000L;
  private static final int INTERPRETER_CHAIN_PRUNING_MAX_ITERATIONS = 4;
  private static final Map<Query, Set<String>> LOGGED_INTERPRETER_SHAPES_BY_QUERY =
      Collections.synchronizedMap(new WeakHashMap<Query, Set<String>>());

  static final ResolverImpl Resolver = new ResolverImpl();

  static final StarCrossJoinResolver StarResolver = new StarCrossJoinResolver();

  private static int counterTag = 0;

  // used to tell the difference between crossjoin expressions.
  private final int ctag = counterTag++;

  public CrossJoinFunDef( FunDef dummyFunDef ) {
    super( dummyFunDef );
  }

  public Type getResultType( Validator validator, Exp[] args ) {
    // CROSSJOIN(<Set1>,<Set2>) has type [Hie1] x [Hie2].
    List<MemberType> list = new ArrayList<MemberType>();
    for ( Exp arg : args ) {
      final Type type = arg.getType();
      if ( type instanceof SetType ) {
        addTypes( type, list );
      } else if ( getName().equals( "*" ) ) {
        // The "*" form of CrossJoin is lenient: args can be either
        // members/tuples or sets.
        addTypes( type, list );
      } else if ( getName().equals( "()" ) ) {
        // The "()" form of CrossJoin is lenient: args can be either
        // members/tuples or sets.
        addTypes( type, list );
      } else {
        throw Util.newInternal( "arg to crossjoin must be a set" );
      }
    }
    final MemberType[] types = list.toArray( new MemberType[list.size()] );
    TupleType.checkHierarchies( types );
    final TupleType tupleType = new TupleType( types );
    return new SetType( tupleType );
  }

  /**
   * Adds a type to a list of types. If type is a {@link TupleType}, does so recursively.
   *
   * @param type
   *          Type to add to list
   * @param list
   *          List of types to add to
   */
  private static void addTypes( final Type type, List<MemberType> list ) {
    if ( type instanceof SetType ) {
      SetType setType = (SetType) type;
      addTypes( setType.getElementType(), list );
    } else if ( type instanceof TupleType ) {
      TupleType tupleType = (TupleType) type;
      for ( Type elementType : tupleType.elementTypes ) {
        addTypes( elementType, list );
      }
    } else if ( type instanceof MemberType ) {
      list.add( (MemberType) type );
    } else {
      throw Util.newInternal( "Unexpected type: " + type );
    }
  }

  public Calc compileCall( final ResolvedFunCall call, ExpCompiler compiler ) {
    // What is the desired return type?
    for ( ResultStyle r : compiler.getAcceptableResultStyles() ) {
      switch ( r ) {
        case ITERABLE:
        case ANY:
          // Consumer wants ITERABLE or ANY
          return compileCallIterable( call, compiler );
        case LIST:
          // Consumer wants (immutable) LIST
          return compileCallImmutableList( call, compiler );
        case MUTABLE_LIST:
          // Consumer MUTABLE_LIST
          return compileCallMutableList( call, compiler );
      }
    }
    throw ResultStyleException.generate( ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY, compiler
        .getAcceptableResultStyles() );
  }

  ///////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////
  // Iterable
  ///////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////

  protected IterCalc compileCallIterable( final ResolvedFunCall call, ExpCompiler compiler ) {
    final Exp[] args = call.getArgs();
    Calc[] calcs =  new Calc[args.length];
    for (int i = 0; i < args.length; i++) {
      calcs[i] = toIter( compiler, args[i] );
    }
    return compileCallIterableArray(call, compiler, calcs);
  }

  protected IterCalc compileCallIterableArray( final ResolvedFunCall call, ExpCompiler compiler, Calc[] calcs) {
    IterCalc iterCalc = compileCallIterableLeaf(call, compiler, calcs[0], calcs[1]);

    if(calcs.length == 2){
      return iterCalc;
    }
    else {
      Calc[] nextClasls = new Calc[calcs.length - 1];
      nextClasls[0] = iterCalc;
      for (int i = 1; i < calcs.length - 1; i++) {
        nextClasls[i] = calcs[i + 1];
      }
      return compileCallIterableArray(call, compiler, nextClasls);
    }
  }

  protected IterCalc compileCallIterableLeaf( final ResolvedFunCall call, ExpCompiler compiler,
                                                final Calc  calc1, final Calc calc2) {
    Calc[] calcs = new Calc[] { calc1, calc2 };
    // The Calcs, 1 and 2, can be of type: Member or Member[] and
    // of ResultStyle: ITERABLE, LIST or MUTABLE_LIST, but
    // LIST and MUTABLE_LIST are treated the same; so
    // there are 16 possible combinations - sweet.

    // Check returned calc ResultStyles
    checkIterListResultStyles( calc1 );
    checkIterListResultStyles( calc2 );

    return new CrossJoinIterCalc( call, calcs );
  }

  private Calc toIter( ExpCompiler compiler, final Exp exp ) {
    // Want iterable, immutable list or mutable list in that order
    // It is assumed that an immutable list is easier to get than
    // a mutable list.
    final Type type = exp.getType();
    if ( type instanceof SetType ) {
      // this can return an IterCalc or ListCalc
      return compiler.compileAs( exp, null, ResultStyle.ITERABLE_LIST_MUTABLELIST );
    } else {
      // this always returns an IterCalc
      return new SetFunDef.ExprIterCalc( new DummyExp( new SetType( type ) ), new Exp[] { exp }, compiler,
          ResultStyle.ITERABLE_LIST_MUTABLELIST );
    }
  }

  class CrossJoinIterCalc extends AbstractIterCalc {
    CrossJoinIterCalc( ResolvedFunCall call, Calc[] calcs ) {
      super( call, calcs );
    }

    public TupleIterable evaluateIterable( Evaluator evaluator ) {
      ResolvedFunCall call = (ResolvedFunCall) exp;
      // Use a native evaluator, if more efficient.
      // TODO: Figure this out at compile time.
      SchemaReader schemaReader = evaluator.getSchemaReader();
      NativeEvaluator nativeEvaluator =
          schemaReader.getNativeSetEvaluator( call.getFunDef(), call.getArgs(), evaluator, this );
      if ( nativeEvaluator != null ) {
        return (TupleIterable) nativeEvaluator.execute( ResultStyle.ITERABLE );
      }

      Calc[] calcs = getCalcs();
      IterCalc calc1 = (IterCalc) calcs[0];
      IterCalc calc2 = (IterCalc) calcs[1];

      TupleIterable o1 = calc1.evaluateIterable( evaluator );
      if ( o1 instanceof TupleList ) {
        TupleList l1 = (TupleList) o1;
        l1 = nonEmptyOptimizeList( evaluator, l1, call );
        if ( l1.isEmpty() ) {
          return TupleCollections.emptyList( getType().getArity() );
        }
        o1 = l1;
      }

      TupleIterable o2 = calc2.evaluateIterable( evaluator );
      if ( o2 instanceof TupleList ) {
        TupleList l2 = (TupleList) o2;
        l2 = nonEmptyOptimizeList( evaluator, l2, call );
        if ( l2.isEmpty() ) {
          return TupleCollections.emptyList( getType().getArity() );
        }
        o2 = l2;
      }

      // Interpreter iterator path frequently still receives TupleList inputs.
      // Apply the same dependency-pruning / limit diagnostics used by the list
      // path before materializing the cartesian iterator.
      if (o1 instanceof TupleList && o2 instanceof TupleList) {
        TupleList l1 = (TupleList) o1;
        TupleList l2 = (TupleList) o2;
        final int l1SizeBeforePrune = l1.size();
        final int l2SizeBeforePrune = l2.size();
        if (evaluator instanceof RolapEvaluator) {
          final TupleList[] prunedLists =
              tryPruneInterpreterCrossJoin((RolapEvaluator) evaluator, l1, l2);
          l1 = prunedLists[0];
          l2 = prunedLists[1];
        }
        logLargeInterpreterCrossJoin(
            evaluator,
            call,
            l1SizeBeforePrune,
            l2SizeBeforePrune,
            l1,
            l2);
        Util.checkCJResultLimit((long) l1.size() * l2.size());
        o1 = l1;
        o2 = l2;
      }

      final TupleIterable[] checkedOperands =
          tryApplyInterpreterCrossJoinLimitProbe(o1, o2);
      o1 = checkedOperands[0];
      o2 = checkedOperands[1];

      final TupleIterable result = makeIterable( o1, o2 );
      if (o1 instanceof TupleList && o2 instanceof TupleList) {
        final TupleList maybeMaterialized =
            tryMaterializeInterpreterCrossJoinForPruningPropagation(
                (TupleList) o1,
                (TupleList) o2,
                result);
        if (maybeMaterialized != null) {
          return maybeMaterialized;
        }
      }
      return result;
    }

    protected TupleIterable makeIterable( final TupleIterable it1, final TupleIterable it2 ) {
      // There is no knowledge about how large either it1 ore it2
      // are or how many null members they might have, so all
      // one can do is iterate across them:
      // iterate across it1 and for each member iterate across it2

      return new AbstractTupleIterable( it1.getArity() + it2.getArity() ) {
        public TupleCursor tupleCursor() {
          return new AbstractTupleCursor( getArity() ) {
            final TupleCursor i1 = it1.tupleCursor();
            final int arity1 = i1.getArity();
            TupleCursor i2 = TupleCollections.emptyList( 1 ).tupleCursor();
            final Member[] members = new Member[arity];

            long currentIteration = 0;
            Execution execution = Locus.peek().execution;

            public boolean forward() {
              if ( i2.forward() ) {
                return true;
              }
              while ( i1.forward() ) {
                CancellationChecker.checkCancelOrTimeout( currentIteration++, execution );
                i2 = it2.tupleCursor();
                if ( i2.forward() ) {
                  return true;
                }
              }
              return false;
            }

            public List<Member> current() {
              i1.currentToArray( members, 0 );
              i2.currentToArray( members, arity1 );
              return Util.flatList( members );
            }

            @Override
            public Member member( int column ) {
              if ( column < arity1 ) {
                return i1.member( column );
              } else {
                return i2.member( column - arity1 );
              }
            }

            @Override
            public void setContext( Evaluator evaluator ) {
              i1.setContext( evaluator );
              i2.setContext( evaluator );
            }

            @Override
            public void currentToArray( Member[] members, int offset ) {
              i1.currentToArray( members, offset );
              i2.currentToArray( members, offset + arity1 );
            }
          };
        }
      };
    }
  }

  private static TupleList tryMaterializeInterpreterCrossJoinForPruningPropagation(
      TupleList left,
      TupleList right,
      TupleIterable result) {
    if (left == null || right == null || result == null) {
      return null;
    }
    final int resultArity = result.getArity();
    if (resultArity <= 0 || resultArity > INTERPRETER_PRUNING_PROPAGATION_MAX_ARITY) {
      return null;
    }
    final long tupleCount = ((long) left.size()) * ((long) right.size());
    if (tupleCount <= 0L || tupleCount > INTERPRETER_PRUNING_PROPAGATION_MAX_TUPLES) {
      return null;
    }
    final TupleList tupleList = TupleCollections.materialize(result, true);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Interpreter CrossJoin materialized for pruning propagation: tuples={}, arity={}",
          tupleList.size(),
          tupleList.getArity());
    }
    return tupleList;
  }

  ///////////////////////////////////////////////////////////////////////////
  // Immutable List
  ///////////////////////////////////////////////////////////////////////////

  protected ListCalc compileCallImmutableList( final ResolvedFunCall call, ExpCompiler compiler ) {
    final Exp[] args = call.getArgs();
    Calc[] calcs =  new Calc[args.length];
    for (int i = 0; i < args.length; i++) {
      calcs[i] = toList( compiler, args[i] );
    }
    return compileCallImmutableListArray(call, compiler, calcs);
  }

  protected ListCalc compileCallImmutableListArray( final ResolvedFunCall call, ExpCompiler compiler, Calc[] calcs ) {
    ListCalc listCalc = compileCallImmutableListLeaf(call, compiler, calcs[0], calcs[1]);

    if(calcs.length == 2){
      return listCalc;
    }
    else {
      Calc[] nextClasls = new Calc[calcs.length - 1];
      nextClasls[0] = listCalc;
      for (int i = 1; i < calcs.length - 1; i++) {
        nextClasls[i] = calcs[i + 1];
      }
      return compileCallImmutableListArray(call, compiler, nextClasls);
    }
  }

  protected ListCalc compileCallImmutableListLeaf( final ResolvedFunCall call, ExpCompiler compiler,
                                                   final Calc  calc1, final Calc calc2 ) {
    Calc[] calcs = new Calc[] { calc1, calc2 };
    // The Calcs, 1 and 2, can be of type: Member or Member[] and
    // of ResultStyle: LIST or MUTABLE_LIST.
    // Since we want an immutable list as the result, it does not
    // matter whether the Calc list are of type
    // LIST and MUTABLE_LIST - they are treated the same; so
    // there are 4 possible combinations - even sweeter.

    // Check returned calc ResultStyles
    checkListResultStyles( calc1 );
    checkListResultStyles( calc2 );

    return new ImmutableListCalc( call, calcs );
  }

  /**
   * Compiles an expression to list (or mutable list) format. Never returns null.
   *
   * @param compiler
   *          Compiler
   * @param exp
   *          Expression
   * @return Compiled expression that yields a list or mutable list
   */
  private ListCalc toList( ExpCompiler compiler, final Exp exp ) {
    // Want immutable list or mutable list in that order
    // It is assumed that an immutable list is easier to get than
    // a mutable list.
    final Type type = exp.getType();
    if ( type instanceof SetType ) {
      final Calc calc = compiler.compileAs( exp, null, ResultStyle.LIST_MUTABLELIST );
      if ( calc == null ) {
        return compiler.compileList( exp, false );
      }
      return (ListCalc) calc;
    } else {
      return new SetFunDef.SetListCalc( new DummyExp( new SetType( type ) ), new Exp[] { exp }, compiler,
          ResultStyle.LIST_MUTABLELIST );
    }
  }

  abstract class BaseListCalc extends AbstractListCalc {
    protected BaseListCalc( ResolvedFunCall call, Calc[] calcs, boolean mutable ) {
      super( call, calcs, mutable );
    }

    public TupleList evaluateList( Evaluator evaluator ) {
      ResolvedFunCall call = (ResolvedFunCall) exp;
      // Use a native evaluator, if more efficient.
      // TODO: Figure this out at compile time.
      SchemaReader schemaReader = evaluator.getSchemaReader();
      NativeEvaluator nativeEvaluator =
          schemaReader.getNativeSetEvaluator( call.getFunDef(), call.getArgs(), evaluator, this );
      if ( nativeEvaluator != null ) {
        return (TupleList) nativeEvaluator.execute( ResultStyle.LIST );
      }

      Calc[] calcs = getCalcs();
      ListCalc listCalc1 = (ListCalc) calcs[0];
      ListCalc listCalc2 = (ListCalc) calcs[1];

      TupleList l1 = listCalc1.evaluateList( evaluator );
      // check if size of first list already exceeds limit
      Util.checkCJResultLimit( l1.size() );
      TupleList l2 = listCalc2.evaluateList( evaluator );
      // check if size of second list already exceeds limit
      Util.checkCJResultLimit( l2.size() );
      final int l1SizeBeforePrune = l1.size();
      final int l2SizeBeforePrune = l2.size();

      if (evaluator instanceof RolapEvaluator) {
        final TupleList[] prunedLists =
            tryPruneInterpreterCrossJoin((RolapEvaluator) evaluator, l1, l2);
        l1 = prunedLists[0];
        l2 = prunedLists[1];
      }
      logLargeInterpreterCrossJoin(
          evaluator,
          call,
          l1SizeBeforePrune,
          l2SizeBeforePrune,
          l1,
          l2);
      // check crossjoin
      Util.checkCJResultLimit( (long) l1.size() * l2.size() );

      l1 = nonEmptyOptimizeList( evaluator, l1, call );
      if ( l1.isEmpty() ) {
        return TupleCollections.emptyList( l1.getArity() + l2.getArity() );
      }
      l2 = nonEmptyOptimizeList( evaluator, l2, call );
      if ( l2.isEmpty() ) {
        return TupleCollections.emptyList( l1.getArity() + l2.getArity() );
      }

      return makeList( l1, l2 );
    }

    protected abstract TupleList makeList( TupleList l1, TupleList l2 );
  }

  private static TupleList[] tryPruneInterpreterCrossJoin(
      RolapEvaluator evaluator,
      TupleList l1,
      TupleList l2) {
    if (evaluator == null || l1 == null || l2 == null) {
      return new TupleList[] { l1, l2 };
    }
    TupleList left = l1;
    TupleList right = l2;
    if (left.getArity() == 1) {
      final TuplePruneResult pruned =
          tryPruneUnaryAgainstTupleColumns(evaluator, left, right);
      left = pruned.unaryList;
      right = pruned.otherList;
    }
    if (right.getArity() == 1) {
      final TuplePruneResult pruned =
          tryPruneUnaryAgainstTupleColumns(evaluator, right, left);
      right = pruned.unaryList;
      left = pruned.otherList;
    }
    if (left != null
        && right != null
        && (left.getArity() > 1 || right.getArity() > 1)) {
      final TuplePairPruneResult chainAware =
          tryPruneTupleColumnsChainAware(evaluator, left, right);
      left = chainAware.leftList;
      right = chainAware.rightList;
    }
    return new TupleList[] { left, right };
  }

  private static TuplePairPruneResult tryPruneTupleColumnsChainAware(
      RolapEvaluator evaluator,
      TupleList left,
      TupleList right) {
    if (evaluator == null
        || left == null
        || right == null
        || left.isEmpty()
        || right.isEmpty()
        || (left.getArity() + right.getArity()) < 2) {
      return TuplePairPruneResult.of(left, right);
    }

    TupleList currentLeft = left;
    TupleList currentRight = right;
    for (int iteration = 0;
         iteration < INTERPRETER_CHAIN_PRUNING_MAX_ITERATIONS;
         iteration++) {
      final TuplePairPruneResult next =
          tryPruneTupleColumnsChainAwareOnce(
              evaluator,
              currentLeft,
              currentRight);
      final boolean leftChanged =
          next.leftList != currentLeft || next.leftList.size() != currentLeft.size();
      final boolean rightChanged =
          next.rightList != currentRight || next.rightList.size() != currentRight.size();
      currentLeft = next.leftList;
      currentRight = next.rightList;
      if ((!leftChanged && !rightChanged)
          || currentLeft.isEmpty()
          || currentRight.isEmpty()) {
        break;
      }
    }
    return TuplePairPruneResult.of(currentLeft, currentRight);
  }

  private static TuplePairPruneResult tryPruneTupleColumnsChainAwareOnce(
      RolapEvaluator evaluator,
      TupleList left,
      TupleList right) {
    final List<CrossJoinArg> args = new ArrayList<CrossJoinArg>();
    final List<TupleColumnLocation> locations = new ArrayList<TupleColumnLocation>();

    collectTupleColumnsAsArgs(evaluator, left, true, args, locations);
    collectTupleColumnsAsArgs(evaluator, right, false, args, locations);
    if (args.size() < 2) {
      return TuplePairPruneResult.of(left, right);
    }

    final CrossJoinArg[] prunedArgs =
        CrossJoinDependencyPrunerV2.prune(
            args.toArray(new CrossJoinArg[args.size()]),
            evaluator);
    if (prunedArgs == null || prunedArgs.length != args.size()) {
      return TuplePairPruneResult.of(left, right);
    }

    final List<ColumnAllowedKeys> leftChangedColumns =
        collectChangedTupleColumnsForSide(
            prunedArgs,
            args,
            locations,
            true);
    final List<ColumnAllowedKeys> rightChangedColumns =
        collectChangedTupleColumnsForSide(
            prunedArgs,
            args,
            locations,
            false);
    if (leftChangedColumns.isEmpty() && rightChangedColumns.isEmpty()) {
      return TuplePairPruneResult.of(left, right);
    }

    final TupleList filteredLeft = leftChangedColumns.isEmpty()
        ? left
        : filterTupleListByColumns(left, leftChangedColumns);
    final TupleList filteredRight = rightChangedColumns.isEmpty()
        ? right
        : filterTupleListByColumns(right, rightChangedColumns);
    return TuplePairPruneResult.of(filteredLeft, filteredRight);
  }

  private static void collectTupleColumnsAsArgs(
      RolapEvaluator evaluator,
      TupleList tupleList,
      boolean leftSide,
      List<CrossJoinArg> args,
      List<TupleColumnLocation> locations) {
    if (evaluator == null
        || tupleList == null
        || args == null
        || locations == null) {
      return;
    }
    for (int column = 0; column < tupleList.getArity(); column++) {
      final List<RolapMember> members = toRolapMembersColumn(tupleList, column);
      if (members == null) {
        continue;
      }
      final CrossJoinArg arg =
          MemberListCrossJoinArg.create(evaluator, members, false, false);
      if (arg instanceof MemberListCrossJoinArg) {
        args.add(arg);
        locations.add(new TupleColumnLocation(leftSide, column));
      }
    }
  }

  private static TuplePruneResult tryPruneUnaryAgainstTupleColumns(
      RolapEvaluator evaluator,
      TupleList unaryList,
      TupleList otherList) {
    if (evaluator == null
        || unaryList == null
        || otherList == null
        || unaryList.getArity() != 1
        || otherList.getArity() < 1) {
      return TuplePruneResult.of(unaryList, otherList);
    }

    TupleList currentUnary = unaryList;
    TupleList currentOther = otherList;
    for (int iteration = 0;
         iteration < INTERPRETER_CHAIN_PRUNING_MAX_ITERATIONS;
         iteration++) {
      final TuplePruneResult next =
          tryPruneUnaryAgainstTupleColumnsOnce(
              evaluator,
              currentUnary,
              currentOther);
      if (next == null) {
        break;
      }
      final boolean unaryChanged = next.unaryList != currentUnary
          || (next.unaryList != null
              && currentUnary != null
              && next.unaryList.size() != currentUnary.size());
      final boolean otherChanged = next.otherList != currentOther
          || (next.otherList != null
              && currentOther != null
              && next.otherList.size() != currentOther.size());
      currentUnary = next.unaryList;
      currentOther = next.otherList;
      if (!unaryChanged && !otherChanged) {
        break;
      }
      if ((currentUnary == null || currentUnary.isEmpty())
          || (currentOther == null || currentOther.isEmpty())) {
        break;
      }
    }
    return TuplePruneResult.of(currentUnary, currentOther);
  }

  private static TuplePruneResult tryPruneUnaryAgainstTupleColumnsOnce(
      RolapEvaluator evaluator,
      TupleList unaryList,
      TupleList otherList) {
    if (evaluator == null
        || unaryList == null
        || otherList == null
        || unaryList.getArity() != 1
        || otherList.getArity() < 1) {
      return TuplePruneResult.of(unaryList, otherList);
    }

    final TuplePruneResult exactChainPruned =
        tryPruneUnaryAgainstTupleColumnsChainAwareExact(
            evaluator,
            unaryList,
            otherList);
    if (exactChainPruned != null) {
      return exactChainPruned;
    }

    final List<RolapMember> unaryMembers = toRolapMembersColumn(unaryList, 0);
    if (unaryMembers == null) {
      return TuplePruneResult.of(unaryList, otherList);
    }
    final CrossJoinArg unaryArg =
        MemberListCrossJoinArg.create(evaluator, unaryMembers, false, false);
    if (!(unaryArg instanceof MemberListCrossJoinArg)) {
      return TuplePruneResult.of(unaryList, otherList);
    }

    final List<CrossJoinArg> args = new ArrayList<CrossJoinArg>();
    final List<Integer> tupleArgColumns = new ArrayList<Integer>();
    args.add(unaryArg);
    for (int column = 0; column < otherList.getArity(); column++) {
      final List<RolapMember> dependentMembers =
          toRolapMembersColumn(otherList, column);
      if (dependentMembers == null) {
        continue;
      }
      final CrossJoinArg dependentArg =
          MemberListCrossJoinArg.create(
              evaluator,
              dependentMembers,
              false,
              false);
      if (dependentArg instanceof MemberListCrossJoinArg) {
        args.add(dependentArg);
        tupleArgColumns.add(column);
      }
    }

    if (args.size() < 2) {
      return TuplePruneResult.of(unaryList, otherList);
    }

    final CrossJoinArg[] prunedArgs =
        CrossJoinDependencyPrunerV2.prune(
            args.toArray(new CrossJoinArg[args.size()]),
            evaluator);
    if (prunedArgs == null
        || prunedArgs.length == 0
        || !(prunedArgs[0] instanceof MemberListCrossJoinArg)) {
      return TuplePruneResult.of(unaryList, otherList);
    }

    final MemberListCrossJoinArg prunedUnaryArg =
        (MemberListCrossJoinArg) prunedArgs[0];
    final boolean unaryChanged =
        prunedUnaryArg.getMembers().size() != unaryList.size();

    TupleList filteredOtherList = otherList;
    final List<ColumnAllowedKeys> changedColumns =
        collectChangedTupleColumns(prunedArgs, args, tupleArgColumns);
    if (!changedColumns.isEmpty()) {
      filteredOtherList = filterTupleListByColumns(otherList, changedColumns);
    }

    final boolean otherChanged = filteredOtherList.size() != otherList.size();
    if (!unaryChanged && !otherChanged) {
      return TuplePruneResult.of(unaryList, otherList);
    }
    return TuplePruneResult.of(
        unaryChanged ? toUnaryTupleList(prunedUnaryArg.getMembers()) : unaryList,
        filteredOtherList);
  }

  private static TuplePruneResult tryPruneUnaryAgainstTupleColumnsChainAwareExact(
      RolapEvaluator evaluator,
      TupleList unaryList,
      TupleList otherList) {
    if (evaluator == null
        || unaryList == null
        || otherList == null
        || unaryList.getArity() != 1
        || otherList.getArity() < 2
        || unaryList.isEmpty()
        || otherList.isEmpty()) {
      return null;
    }

    final List<RolapMember> unaryMembers = toRolapMembersColumn(unaryList, 0);
    if (unaryMembers == null || unaryMembers.isEmpty()) {
      return null;
    }
    final RolapLevel dependentLevel = getConcreteLevel(unaryMembers);
    if (dependentLevel == null) {
      return null;
    }
    boolean hasConcreteUnaryMembers = false;
    for (RolapMember member : unaryMembers) {
      if (member == null) {
        return null;
      }
      if (member.isCalculated() || member.isAll()) {
        continue;
      }
      hasConcreteUnaryMembers = true;
      if (!dependentLevel.equals(member.getLevel())) {
        return null;
      }
    }
    if (!hasConcreteUnaryMembers) {
      return null;
    }

    final DependencyPruningContext context =
        DependencyPruningContext.fromEvaluator(evaluator);
    if (context == null
        || context.getPolicy() == DependencyRegistry.DependencyPruningPolicy.OFF) {
      return null;
    }
    final DependencyRegistry registry = context.getRegistry();
    if (registry == null) {
      return null;
    }
    final DependencyRegistry.LevelDependencyDescriptor descriptor =
        registry.getLevelDescriptor(dependentLevel.getUniqueName());
    if (descriptor == null || descriptor.getRules().isEmpty()) {
      return null;
    }

    final List<ChainDeterminantColumn> chainColumns =
        collectApplicableChainDeterminantColumns(
            otherList,
            context,
            descriptor);
    if (chainColumns.size() < 2) {
      return null;
    }

    final Set<ChainSignature> tupleConcreteSignatures =
        collectTupleConcreteChainSignatures(otherList, chainColumns);
    if (tupleConcreteSignatures.isEmpty()) {
      return null;
    }

    final List<RolapMember> retainedUnaryMembers =
        new ArrayList<RolapMember>(unaryMembers.size());
    final Set<ChainSignature> retainedSignatures =
        new HashSet<ChainSignature>(unaryMembers.size());
    for (RolapMember unaryMember : unaryMembers) {
      if (unaryMember == null) {
        return null;
      }
      if (unaryMember.isCalculated() || unaryMember.isAll()) {
        // Preserve subtotal/calc scaffolding rows on unary side.
        retainedUnaryMembers.add(unaryMember);
        continue;
      }
      final ChainSignature signature =
          deriveChainSignatureForDependentMember(
              unaryMember,
              chainColumns);
      if (signature == null) {
        return null;
      }
      if (tupleConcreteSignatures.contains(signature)) {
        retainedUnaryMembers.add(unaryMember);
        retainedSignatures.add(signature);
      }
    }
    if (retainedUnaryMembers.isEmpty()
        || retainedUnaryMembers.size() >= unaryMembers.size()) {
      return null;
    }
    if (retainedSignatures.isEmpty()) {
      return null;
    }

    final TupleList filteredOther =
        filterTupleListByChainSignatures(otherList, chainColumns, retainedSignatures);
    final TupleList filteredUnary = toUnaryTupleList(retainedUnaryMembers);
    return TuplePruneResult.of(filteredUnary, filteredOther);
  }

  private static List<ChainDeterminantColumn> collectApplicableChainDeterminantColumns(
      TupleList otherList,
      DependencyPruningContext context,
      DependencyRegistry.LevelDependencyDescriptor dependentDescriptor) {
    final List<ChainDeterminantColumn> result =
        new ArrayList<ChainDeterminantColumn>(otherList.getArity());
    if (otherList == null
        || context == null
        || dependentDescriptor == null) {
      return result;
    }
    for (int column = 0; column < otherList.getArity(); column++) {
      final RolapLevel determinantLevel = levelForTupleColumn(otherList, column);
      if (determinantLevel == null) {
        continue;
      }
      final DependencyRegistry.CompiledDependencyRule rule =
          findApplicableChainRule(
              dependentDescriptor,
              determinantLevel,
              context);
      if (rule == null) {
        continue;
      }
      result.add(new ChainDeterminantColumn(column, determinantLevel, rule));
    }
    return result;
  }

  private static DependencyRegistry.CompiledDependencyRule findApplicableChainRule(
      DependencyRegistry.LevelDependencyDescriptor dependentDescriptor,
      RolapLevel determinantLevel,
      DependencyPruningContext context) {
    if (dependentDescriptor == null
        || determinantLevel == null
        || context == null) {
      return null;
    }
    for (DependencyRegistry.CompiledDependencyRule rule : dependentDescriptor.getRules()) {
      if (rule == null
          || !rule.isValidated()
          || rule.isAmbiguousJoinPath()
          || rule.getDeterminantLevelName() == null
          || !rule.getDeterminantLevelName().equals(determinantLevel.getUniqueName())) {
        continue;
      }
      if (rule.requiresTimeFilter() && !context.hasRequiredTimeFilter()) {
        continue;
      }
      return rule;
    }
    return null;
  }

  private static Set<ChainSignature> collectTupleConcreteChainSignatures(
      TupleList tupleList,
      List<ChainDeterminantColumn> chainColumns) {
    final Set<ChainSignature> signatures = new HashSet<ChainSignature>();
    if (tupleList == null || chainColumns == null || chainColumns.isEmpty()) {
      return signatures;
    }
    for (int row = 0; row < tupleList.size(); row++) {
      final ChainSignature sig =
          buildTupleRowChainSignature(tupleList, row, chainColumns, false);
      if (sig != null) {
        signatures.add(sig);
      }
    }
    return signatures;
  }

  private static ChainSignature deriveChainSignatureForDependentMember(
      RolapMember dependentMember,
      List<ChainDeterminantColumn> chainColumns) {
    if (dependentMember == null || chainColumns == null || chainColumns.isEmpty()) {
      return null;
    }
    final String[] keys = new String[chainColumns.size()];
    for (int i = 0; i < chainColumns.size(); i++) {
      final ChainDeterminantColumn col = chainColumns.get(i);
      final Object key = deriveDeterminantKeyFromDependentMember(
          dependentMember,
          col.determinantLevel,
          col.rule);
      if (key == null) {
        return null;
      }
      keys[i] = String.valueOf(key);
    }
    return new ChainSignature(keys);
  }

  private static Object deriveDeterminantKeyFromDependentMember(
      RolapMember dependentMember,
      RolapLevel determinantLevel,
      DependencyRegistry.CompiledDependencyRule rule) {
    if (dependentMember == null
        || determinantLevel == null
        || rule == null
        || rule.getMappingType() == null) {
      return null;
    }
    if (rule.getMappingType() == DependencyRegistry.DependencyMappingType.PROPERTY) {
      final String propertyName = rule.getMappingProperty();
      if (propertyName == null || propertyName.isEmpty()) {
        return null;
      }
      return dependentMember.getPropertyValue(propertyName);
    }
    if (rule.getMappingType() == DependencyRegistry.DependencyMappingType.ANCESTOR) {
      RolapMember current = dependentMember;
      while (current != null && !determinantLevel.equals(current.getLevel())) {
        current = current.getParentMember();
      }
      return current == null ? null : current.getKey();
    }
    return null;
  }

  private static TupleList filterTupleListByChainSignatures(
      TupleList tupleList,
      List<ChainDeterminantColumn> chainColumns,
      Set<ChainSignature> allowedSignatures) {
    if (tupleList == null
        || tupleList.isEmpty()
        || chainColumns == null
        || chainColumns.isEmpty()
        || allowedSignatures == null
        || allowedSignatures.isEmpty()) {
      return tupleList;
    }
    final TupleList filtered =
        TupleCollections.createList(tupleList.getArity(), tupleList.size());
    final Member[] tuple = new Member[tupleList.getArity()];
    for (int row = 0; row < tupleList.size(); row++) {
      final ChainSignature signature =
          buildTupleRowChainSignature(tupleList, row, chainColumns, true);
      final boolean keep =
          signature == ChainSignature.WILDCARD_ROW
              || (signature != null && allowedSignatures.contains(signature));
      if (!keep) {
        continue;
      }
      for (int c = 0; c < tuple.length; c++) {
        tuple[c] = tupleList.get(c, row);
      }
      filtered.addTuple(tuple);
    }
    return filtered.size() < tupleList.size() ? filtered : tupleList;
  }

  private static ChainSignature buildTupleRowChainSignature(
      TupleList tupleList,
      int row,
      List<ChainDeterminantColumn> chainColumns,
      boolean preserveWildcardRows) {
    if (tupleList == null
        || chainColumns == null
        || chainColumns.isEmpty()
        || row < 0
        || row >= tupleList.size()) {
      return null;
    }
    final String[] keys = new String[chainColumns.size()];
    for (int i = 0; i < chainColumns.size(); i++) {
      final ChainDeterminantColumn col = chainColumns.get(i);
      final Member member = tupleList.get(col.column, row);
      if (member == null) {
        return null;
      }
      if (member.isAll() || member.isCalculated()) {
        return preserveWildcardRows ? ChainSignature.WILDCARD_ROW : null;
      }
      if (!(member instanceof RolapMember)) {
        return null;
      }
      final Object key = ((RolapMember) member).getKey();
      if (key == null) {
        return null;
      }
      keys[i] = String.valueOf(key);
    }
    return new ChainSignature(keys);
  }

  private static RolapLevel getConcreteLevel(List<RolapMember> members) {
    if (members == null || members.isEmpty()) {
      return null;
    }
    for (RolapMember member : members) {
      if (member == null || member.isCalculated() || member.isAll()) {
        continue;
      }
      if (member.getLevel() instanceof RolapLevel) {
        return (RolapLevel) member.getLevel();
      }
    }
    return null;
  }

  private static RolapLevel levelForTupleColumn(TupleList list, int column) {
    if (list == null || column < 0 || column >= list.getArity()) {
      return null;
    }
    for (int row = 0; row < list.size(); row++) {
      final Member member = list.get(column, row);
      if (member == null || member.isCalculated() || member.isAll()) {
        continue;
      }
      if (member.getLevel() instanceof RolapLevel) {
        return (RolapLevel) member.getLevel();
      }
      return null;
    }
    return null;
  }

  private static List<RolapMember> toRolapMembersColumn(
      TupleList list,
      int column) {
    if (list == null || list.getArity() <= column || column < 0) {
      return null;
    }
    final List<RolapMember> members = new ArrayList<RolapMember>(list.size());
    for (int i = 0; i < list.size(); i++) {
      final Member member = list.get(column, i);
      if (!(member instanceof RolapMember)) {
        return null;
      }
      final RolapMember rolapMember = (RolapMember) member;
      if (rolapMember.isNull()) {
        return null;
      }
      members.add(rolapMember);
    }
    return members;
  }

  private static TupleList toUnaryTupleList(List<RolapMember> members) {
    final TupleList tupleList =
        TupleCollections.createList(1, members == null ? 0 : members.size());
    if (members == null) {
      return tupleList;
    }
    for (RolapMember member : members) {
      tupleList.addTuple(member);
    }
    return tupleList;
  }

  private TupleIterable[] tryApplyInterpreterCrossJoinLimitProbe(
      TupleIterable left,
      TupleIterable right) {
    final int resultLimit = MondrianProperties.instance().ResultLimit.get();
    if (resultLimit <= 0 || left == null || right == null) {
      return new TupleIterable[] { left, right };
    }
    if (left instanceof TupleList && right instanceof TupleList) {
      // Exact check is already performed above in the TupleList pruning branch.
      return new TupleIterable[] { left, right };
    }

    TupleIterable l = left;
    TupleIterable r = right;
    if (left instanceof TupleList && !(right instanceof TupleList)) {
      final TupleList probedRight =
          tryMaterializeForInterpreterLimitProbe(
              right,
              maxOtherSideTuplesBeforeLimit(((TupleList) left).size(), resultLimit));
      if (probedRight != null) {
        r = probedRight;
        Util.checkCJResultLimit(((long) ((TupleList) left).size()) * ((long) probedRight.size()));
      }
    } else if (right instanceof TupleList && !(left instanceof TupleList)) {
      final TupleList probedLeft =
          tryMaterializeForInterpreterLimitProbe(
              left,
              maxOtherSideTuplesBeforeLimit(((TupleList) right).size(), resultLimit));
      if (probedLeft != null) {
        l = probedLeft;
        Util.checkCJResultLimit(((long) probedLeft.size()) * ((long) ((TupleList) right).size()));
      }
    }
    return new TupleIterable[] { l, r };
  }

  private static long maxOtherSideTuplesBeforeLimit(int knownSideSize, int resultLimit) {
    if (knownSideSize <= 0 || resultLimit <= 0) {
      return 0L;
    }
    final long threshold = (long) resultLimit / (long) knownSideSize;
    // Need one extra tuple to prove the product exceeds the limit.
    final long probe = threshold + 1L;
    return Math.max(1L, Math.min(probe, INTERPRETER_LIMIT_PROBE_MAX_TUPLES));
  }

  private static TupleList tryMaterializeForInterpreterLimitProbe(
      TupleIterable iterable,
      long maxTuplesToMaterialize) {
    if (iterable == null || maxTuplesToMaterialize <= 0L) {
      return null;
    }
    final int arity = iterable.getArity();
    if (arity <= 0 || arity > INTERPRETER_PRUNING_PROPAGATION_MAX_ARITY) {
      return null;
    }
    final int cap = (int) Math.min(Integer.MAX_VALUE, maxTuplesToMaterialize);
    final TupleList tupleList = TupleCollections.createList(arity, Math.min(cap, 256));
    final TupleCursor cursor = iterable.tupleCursor();
    int count = 0;
    while (count < cap && cursor.forward()) {
      final Member[] tuple = new Member[arity];
      for (int i = 0; i < arity; i++) {
        tuple[i] = cursor.member(i);
      }
      tupleList.addTuple(tuple);
      count++;
    }
    return tupleList;
  }

  private static void logLargeInterpreterCrossJoin(
      Evaluator evaluator,
      ResolvedFunCall call,
      int leftSizeBeforePrune,
      int rightSizeBeforePrune,
      TupleList leftAfterPrune,
      TupleList rightAfterPrune) {
    if (!LOGGER.isWarnEnabled()
        || leftAfterPrune == null
        || rightAfterPrune == null) {
      return;
    }

    final long beforeProduct =
        ((long) Math.max(leftSizeBeforePrune, 0))
            * ((long) Math.max(rightSizeBeforePrune, 0));
    final long afterProduct =
        ((long) leftAfterPrune.size()) * ((long) rightAfterPrune.size());
    final long threshold = getCrossJoinShapeLogThreshold();
    if (beforeProduct < threshold && afterProduct < threshold) {
      return;
    }

    final String levelsLeft = tupleListLevelSignature(leftAfterPrune);
    final String levelsRight = tupleListLevelSignature(rightAfterPrune);
    if (!shouldLogInterpreterCrossJoinShape(evaluator, call, levelsLeft, levelsRight)) {
      return;
    }

    LOGGER.warn(
        "Interpreter CrossJoin shape {}: left={}x{}, right={}x{}, productBefore={}, productAfter={} (levelsLeft={}, levelsRight={})",
        call == null ? "CrossJoin" : call.getFunName(),
        leftSizeBeforePrune,
        leftAfterPrune.size(),
        rightSizeBeforePrune,
        rightAfterPrune.size(),
        beforeProduct,
        afterProduct,
        levelsLeft,
        levelsRight);
  }

  private static boolean shouldLogInterpreterCrossJoinShape(
      Evaluator evaluator,
      ResolvedFunCall call,
      String levelsLeft,
      String levelsRight) {
    if (evaluator == null || evaluator.getQuery() == null) {
      return true;
    }
    final Query query = evaluator.getQuery();
    final String key =
        (call == null ? "CrossJoin" : call.getFunName())
            + '|'
            + String.valueOf(levelsLeft)
            + '|'
            + String.valueOf(levelsRight);
    synchronized (LOGGED_INTERPRETER_SHAPES_BY_QUERY) {
      Set<String> keys = LOGGED_INTERPRETER_SHAPES_BY_QUERY.get(query);
      if (keys == null) {
        keys = new HashSet<String>();
        LOGGED_INTERPRETER_SHAPES_BY_QUERY.put(query, keys);
      }
      return keys.add(key);
    }
  }

  private static long getCrossJoinShapeLogThreshold() {
    final int resultLimit = MondrianProperties.instance().ResultLimit.get();
    if (resultLimit > 0) {
      final long quarter = Math.max(1L, resultLimit / 4L);
      return Math.min((long) resultLimit, Math.max(10000L, quarter));
    }
    return 250000L;
  }

  private static String tupleListLevelSignature(TupleList list) {
    if (list == null) {
      return "<null>";
    }
    if (list.getArity() <= 0) {
      return "[]";
    }
    final StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int c = 0; c < list.getArity(); c++) {
      if (c > 0) {
        sb.append(", ");
      }
      sb.append(levelNameForColumn(list, c));
    }
    sb.append(']');
    return sb.toString();
  }

  private static String levelNameForColumn(TupleList list, int column) {
    if (list == null || column < 0 || column >= list.getArity()) {
      return "?";
    }
    for (int row = 0; row < list.size(); row++) {
      final Member member = list.get(column, row);
      if (member != null && member.getLevel() != null) {
        return member.getLevel().getUniqueName();
      }
    }
    return "?";
  }

  private static List<ColumnAllowedKeys> collectChangedTupleColumns(
      CrossJoinArg[] prunedArgs,
      List<CrossJoinArg> originalArgs,
      List<Integer> tupleArgColumns) {
    if (prunedArgs == null
        || originalArgs == null
        || tupleArgColumns == null
        || prunedArgs.length != originalArgs.size()
        || tupleArgColumns.isEmpty()) {
      return Arrays.<ColumnAllowedKeys>asList();
    }
    final List<ColumnAllowedKeys> result =
        new ArrayList<ColumnAllowedKeys>(tupleArgColumns.size());
    for (int tupleArgIndex = 0; tupleArgIndex < tupleArgColumns.size(); tupleArgIndex++) {
      final int argIndex = tupleArgIndex + 1;
      if (!(originalArgs.get(argIndex) instanceof MemberListCrossJoinArg)
          || !(prunedArgs[argIndex] instanceof MemberListCrossJoinArg)) {
        continue;
      }
      final MemberListCrossJoinArg original =
          (MemberListCrossJoinArg) originalArgs.get(argIndex);
      final MemberListCrossJoinArg pruned =
          (MemberListCrossJoinArg) prunedArgs[argIndex];
      if (pruned.getMembers().size() >= original.getMembers().size()) {
        continue;
      }
      result.add(
          new ColumnAllowedKeys(
              tupleArgColumns.get(tupleArgIndex).intValue(),
              AllowedMemberKeys.from(pruned.getMembers())));
    }
    return result;
  }

  private static List<ColumnAllowedKeys> collectChangedTupleColumnsForSide(
      CrossJoinArg[] prunedArgs,
      List<CrossJoinArg> originalArgs,
      List<TupleColumnLocation> locations,
      boolean leftSide) {
    if (prunedArgs == null
        || originalArgs == null
        || locations == null
        || prunedArgs.length != originalArgs.size()
        || locations.size() != originalArgs.size()) {
      return Arrays.<ColumnAllowedKeys>asList();
    }
    final List<ColumnAllowedKeys> result = new ArrayList<ColumnAllowedKeys>();
    for (int i = 0; i < locations.size(); i++) {
      final TupleColumnLocation location = locations.get(i);
      if (location == null || location.leftSide != leftSide) {
        continue;
      }
      if (!(originalArgs.get(i) instanceof MemberListCrossJoinArg)
          || !(prunedArgs[i] instanceof MemberListCrossJoinArg)) {
        continue;
      }
      final MemberListCrossJoinArg original =
          (MemberListCrossJoinArg) originalArgs.get(i);
      final MemberListCrossJoinArg pruned =
          (MemberListCrossJoinArg) prunedArgs[i];
      if (pruned.getMembers().size() >= original.getMembers().size()) {
        continue;
      }
      result.add(
          new ColumnAllowedKeys(
              location.column,
              AllowedMemberKeys.from(pruned.getMembers())));
    }
    return result;
  }

  private static TupleList filterTupleListByColumns(
      TupleList tupleList,
      List<ColumnAllowedKeys> changedColumns) {
    if (tupleList == null
        || changedColumns == null
        || changedColumns.isEmpty()
        || tupleList.isEmpty()) {
      return tupleList;
    }
    final TupleList filtered =
        TupleCollections.createList(tupleList.getArity(), tupleList.size());
    final Member[] tuple = new Member[tupleList.getArity()];
    for (int row = 0; row < tupleList.size(); row++) {
      boolean keep = true;
      for (ColumnAllowedKeys columnKeys : changedColumns) {
        final Member member = tupleList.get(columnKeys.column, row);
        // Preserve subtotal scaffolding tuples (All / calculated members) so
        // Excel drill-up/drill-down rows are not lost when pruning nested
        // interpreter-path crossjoins.
        if (member != null && (member.isAll() || member.isCalculated())) {
          continue;
        }
        if (!(member instanceof RolapMember)
            || !columnKeys.allowed.contains(((RolapMember) member).getKey())) {
          keep = false;
          break;
        }
      }
      if (!keep) {
        continue;
      }
      for (int c = 0; c < tuple.length; c++) {
        tuple[c] = tupleList.get(c, row);
      }
      filtered.addTuple(tuple);
    }
    return filtered.size() < tupleList.size() ? filtered : tupleList;
  }

  private static final class TuplePruneResult {
    private final TupleList unaryList;
    private final TupleList otherList;

    private TuplePruneResult(TupleList unaryList, TupleList otherList) {
      this.unaryList = unaryList;
      this.otherList = otherList;
    }

    private static TuplePruneResult of(TupleList unaryList, TupleList otherList) {
      return new TuplePruneResult(unaryList, otherList);
    }
  }

  private static final class TuplePairPruneResult {
    private final TupleList leftList;
    private final TupleList rightList;

    private TuplePairPruneResult(TupleList leftList, TupleList rightList) {
      this.leftList = leftList;
      this.rightList = rightList;
    }

    private static TuplePairPruneResult of(TupleList leftList, TupleList rightList) {
      return new TuplePairPruneResult(leftList, rightList);
    }
  }

  private static final class TupleColumnLocation {
    private final boolean leftSide;
    private final int column;

    private TupleColumnLocation(boolean leftSide, int column) {
      this.leftSide = leftSide;
      this.column = column;
    }
  }

  private static final class ColumnAllowedKeys {
    private final int column;
    private final AllowedMemberKeys allowed;

    private ColumnAllowedKeys(int column, AllowedMemberKeys allowed) {
      this.column = column;
      this.allowed = allowed;
    }
  }

  private static final class ChainDeterminantColumn {
    private final int column;
    private final RolapLevel determinantLevel;
    private final DependencyRegistry.CompiledDependencyRule rule;

    private ChainDeterminantColumn(
        int column,
        RolapLevel determinantLevel,
        DependencyRegistry.CompiledDependencyRule rule) {
      this.column = column;
      this.determinantLevel = determinantLevel;
      this.rule = rule;
    }
  }

  private static final class ChainSignature {
    private static final ChainSignature WILDCARD_ROW =
        new ChainSignature(new String[0]);
    private final String[] keys;

    private ChainSignature(String[] keys) {
      this.keys = keys == null ? new String[0] : keys;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ChainSignature)) {
        return false;
      }
      final ChainSignature other = (ChainSignature) obj;
      return Arrays.equals(keys, other.keys);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(keys);
    }
  }

  private static final class AllowedMemberKeys {
    private final Set<Object> exactKeys;
    private final Set<String> stringKeys;

    private AllowedMemberKeys(Set<Object> exactKeys, Set<String> stringKeys) {
      this.exactKeys = exactKeys;
      this.stringKeys = stringKeys;
    }

    private static AllowedMemberKeys from(List<RolapMember> members) {
      final Set<Object> exact = new HashSet<Object>();
      final Set<String> strings = new HashSet<String>();
      if (members != null) {
        for (RolapMember member : members) {
          if (member == null || member.getKey() == null) {
            continue;
          }
          final Object key = member.getKey();
          exact.add(key);
          strings.add(String.valueOf(key));
        }
      }
      return new AllowedMemberKeys(exact, strings);
    }

    private boolean contains(Object key) {
      if (key == null) {
        return false;
      }
      return exactKeys.contains(key) || stringKeys.contains(String.valueOf(key));
    }
  }

  class ImmutableListCalc extends BaseListCalc {
    ImmutableListCalc( ResolvedFunCall call, Calc[] calcs ) {
      super( call, calcs, false );
    }

    protected TupleList makeList( final TupleList l1, final TupleList l2 ) {
      final int arity = l1.getArity() + l2.getArity();
      return new DelegatingTupleList( arity, new AbstractList<List<Member>>() {
        final List<List<List<Member>>> lists = Arrays.<List<List<Member>>> asList( l1, l2 );
        final Member[] members = new Member[arity];

        final CartesianProductList cartesianProductList = new CartesianProductList<List<Member>>( lists );

        @Override
        public List<Member> get( int index ) {
          cartesianProductList.getIntoArray( index, members );
          return Util.flatListCopy( members );
        }

        @Override
        public int size() {
          return cartesianProductList.size();
        }
      } );
    }
  }

  protected ListCalc compileCallMutableList( final ResolvedFunCall call, ExpCompiler compiler ) {
    final Exp[] args = call.getArgs();
    Calc[] calcs =  new Calc[args.length];
    for (int i = 0; i < args.length; i++) {
      calcs[i] = toList( compiler, args[i] );
    }
    return compileCallMutableListArray(call, compiler, calcs);
  }

  protected ListCalc compileCallMutableListArray( final ResolvedFunCall call, ExpCompiler compiler, Calc[] calcs ) {
    ListCalc listCalc = compileCallMutableListLeaf(call, compiler, calcs[0], calcs[1]);

    if(calcs.length == 2){
      return listCalc;
    }
    else {
      Calc[] nextClasls = new Calc[calcs.length - 1];
      nextClasls[0] = listCalc;
      for (int i = 1; i < calcs.length - 1; i++) {
        nextClasls[i] = calcs[i + 1];
      }
      return compileCallMutableListArray(call, compiler, nextClasls);
    }
  }

  protected ListCalc compileCallMutableListLeaf( final ResolvedFunCall call, ExpCompiler compiler,
                                                 final Calc  calc1, final Calc calc2 ) {
    Calc[] calcs = new Calc[] { calc1, calc2 };
    // The Calcs, 1 and 2, can be of type: Member or Member[] and
    // of ResultStyle: LIST or MUTABLE_LIST.
    // Since we want an mutable list as the result, it does not
    // matter whether the Calc list are of type
    // LIST and MUTABLE_LIST - they are treated the same,
    // regardless of type, one must materialize the result list; so
    // there are 4 possible combinations - even sweeter.

    // Check returned calc ResultStyles
    checkListResultStyles( calc1 );
    checkListResultStyles( calc2 );

    return new MutableListCalc( call, calcs );
  }

  class MutableListCalc extends BaseListCalc {
    MutableListCalc( ResolvedFunCall call, Calc[] calcs ) {
      super( call, calcs, true );
    }

    @SuppressWarnings( { "unchecked" } )
    protected TupleList makeList( final TupleList l1, final TupleList l2 ) {
      final int arity = l1.getArity() + l2.getArity();
      final List<Member> members = new ArrayList<Member>( arity * l1.size() * l2.size() );
      for ( List<Member> ma1 : l1 ) {
        for ( List<Member> ma2 : l2 ) {
          members.addAll( ma1 );
          members.addAll( ma2 );
        }
      }
      return new ListTupleList( arity, members );
    }
  }

  protected TupleList nonEmptyOptimizeList( Evaluator evaluator, TupleList list, ResolvedFunCall call ) {
    int opSize = MondrianProperties.instance().CrossJoinOptimizerSize.get();
    if ( list.isEmpty() ) {
      return list;
    }
    try {
      final Object o = list.get( 0 );
      if ( o instanceof Member ) {
        // Cannot optimize high cardinality dimensions
        Dimension dimension = ( (Member) o ).getDimension();
        if ( dimension.isHighCardinality() ) {
          LOGGER.warn( MondrianResource.instance().HighCardinalityInDimension.str( dimension.getUniqueName() ) );
          return list;
        }
      }
    } catch ( IndexOutOfBoundsException ioobe ) {
      return TupleCollections.emptyList( list.getArity() );
    }
    int size = list.size();

    if ( size > opSize && evaluator.isNonEmpty() ) {
      // instead of overflow exception try to further
      // optimize nonempty(crossjoin(a,b)) ==
      // nonempty(crossjoin(nonempty(a),nonempty(b))
      final int missCount = evaluator.getMissCount();

      list = nonEmptyList( evaluator, list, call );
      size = list.size();
      // list may be empty after nonEmpty optimization
      if ( size == 0 ) {
        return TupleCollections.emptyList( list.getArity() );
      }
      final int missCount2 = evaluator.getMissCount();
      final int puntMissCountListSize = 1000;
      if ( missCount2 > missCount && size > puntMissCountListSize ) {
        // We've hit some cells which are not in the cache. They
        // registered as non-empty, but we won't really know until
        // we've populated the cache. The cartesian product is still
        // huge, so let's quit now, and try again after the cache
        // has been loaded.
        // Return an empty list short circuits higher level
        // evaluation poping one all the way to the top.
        return TupleCollections.emptyList( list.getArity() );
      }
    }
    return list;
  }

  public static TupleList mutableCrossJoin( TupleList list1, TupleList list2 ) {
    return mutableCrossJoin( Arrays.asList( list1, list2 ) );
  }

  public static TupleList mutableCrossJoin( List<TupleList> lists ) {
    long size = 1;
    int arity = 0;
    for ( TupleList list : lists ) {
      size *= (long) list.size();
      arity += list.getArity();
    }
    if ( size == 0L ) {
      return TupleCollections.emptyList( arity );
    }

    // Optimize nonempty(crossjoin(a,b)) ==
    // nonempty(crossjoin(nonempty(a),nonempty(b))

    // FIXME: If we're going to apply a NON EMPTY constraint later, it's
    // possible that the ultimate result will be much smaller.

    Util.checkCJResultLimit( size );

    // Now we can safely cast size to an integer. It still might be very
    // large - which means we're allocating a huge array which we might
    // pare down later by applying NON EMPTY constraints - which is a
    // concern.
    List<Member> result = new ArrayList<Member>( (int) size * arity );

    final Member[] partialArray = new Member[arity];
    final List<Member> partial = Arrays.asList( partialArray );
    cartesianProductRecurse( 0, lists, partial, partialArray, 0, result );
    return new ListTupleList( arity, result );
  }

  private static void cartesianProductRecurse( int i, List<TupleList> lists, List<Member> partial,
      Member[] partialArray, int partialSize, List<Member> result ) {
    final TupleList tupleList = lists.get( i );
    final int partialSizeNext = partialSize + tupleList.getArity();
    final int iNext = i + 1;
    final TupleCursor cursor = tupleList.tupleCursor();
    int currentIteration = 0;
    Execution execution = Locus.peek().execution;
    while ( cursor.forward() ) {
      CancellationChecker.checkCancelOrTimeout( currentIteration++, execution );
      cursor.currentToArray( partialArray, partialSize );
      if ( i == lists.size() - 1 ) {
        result.addAll( partial );
      } else {
        cartesianProductRecurse( iNext, lists, partial, partialArray, partialSizeNext, result );
      }
    }
  }

  /**
   * Traverses the function call tree of the non empty crossjoin function and populates the queryMeasureSet with base
   * measures
   */
  private static class MeasureVisitor extends MdxVisitorImpl {

    private final Set<Member> queryMeasureSet;
    private final ResolvedFunCallFinder finder;
    private final Set<Member> activeMeasures = new HashSet<Member>();

    /**
     * Creates a MeasureVisitor.
     *
     * @param queryMeasureSet
     *          Set of measures in query
     * @param crossJoinCall
     *          Measures referencing this call should be excluded from the list of measures found
     */
    MeasureVisitor( Set<Member> queryMeasureSet, ResolvedFunCall crossJoinCall ) {
      this.queryMeasureSet = queryMeasureSet;
      this.finder = new ResolvedFunCallFinder( crossJoinCall );
    }

    public Object visit( ParameterExpr parameterExpr ) {
      final Parameter parameter = parameterExpr.getParameter();
      final Type type = parameter.getType();
      if ( type instanceof mondrian.olap.type.MemberType ) {
        final Object value = parameter.getValue();
        if ( value instanceof Member ) {
          final Member member = (Member) value;
          process( member );
        }
      }

      return null;
    }

    public Object visit( MemberExpr memberExpr ) {
      Member member = memberExpr.getMember();
      process( member );
      return null;
    }

    private void process( final Member member ) {
      if ( member.isMeasure() ) {
        if ( member.isCalculated() ) {
          if ( activeMeasures.add( member ) ) {
            Exp exp = member.getExpression();
            finder.found = false;
            exp.accept( finder );
            if ( !finder.found ) {
              exp.accept( this );
            }
            activeMeasures.remove( member );
          }
        } else {
          queryMeasureSet.add( member );
        }
      }
    }
  }

  /**
   * This is the entry point to the crossjoin non-empty optimizer code.
   *
   * <p>
   * What one wants to determine is for each individual Member of the input parameter list, a 'List-Member', whether
   * across a slice there is any data.
   *
   * <p>
   * But what data?
   *
   * <p>
   * For Members other than those in the list, the 'non-List-Members', one wants to consider all data across the scope
   * of these other Members. For instance, if Time is not a List-Member, then one wants to consider data across All
   * Time. Or, if Customer is not a List-Member, then look at data across All Customers. The theory here, is if there is
   * no data for a particular Member of the list where all other Members not part of the list are span their complete
   * hierarchy, then there is certainly no data for Members of that Hierarchy at a more specific Level (more on this
   * below).
   *
   * <p>
   * When a Member that is a non-List-Member is part of a Hierarchy that has an All Member (hasAll="true"), then its
   * very easy to make sure that the All Member is used during the optimization. If a non-List-Member is part of a
   * Hierarchy that does not have an All Member, then one must, in fact, iterate over all top-level Members of the
   * Hierarchy!!! - otherwise a List-Member might be excluded because the optimization code was not looking everywhere.
   *
   * <p>
   * Concerning default Members for those Hierarchies for the non-List-Members, ignore them. What is wanted is either
   * the All Member or one must iterate across all top-level Members, what happens to be the default Member of the
   * Hierarchy is of no relevant.
   *
   * <p>
   * The Measures Hierarchy has special considerations. First, there is no All Measure. But, certainly one need only
   * involve Measures that are actually in the query... yes and no. For Calculated Measures one must also get all of the
   * non-Calculated Measures that make up each Calculated Measure. Thus, one ends up iterating across all Calculated and
   * non-Calculated Measures that are explicitly mentioned in the query as well as all Calculated and non-Calculated
   * Measures that are used to define the Calculated Measures in the query. Why all of these? because this represents
   * the total scope of possible Measures that might yield a non-null value for the List-Members and that is what we
   * what to find. It might be a super set, but thats ok; we just do not want to miss anything.
   *
   * <p>
   * For other Members, the default Member is used, but for Measures one should look for that data for all Measures
   * associated with the query, not just one Measure. For a dense dataset this may not be a problem or even apparent,
   * but for a sparse dataset, the first Measure may, in fact, have not data but other Measures associated with the
   * query might. Hence, the solution here is to identify all Measures associated with the query and then for each
   * Member of the list, determine if there is any data iterating across all Measures until non-null data is found or
   * the end of the Measures is reached.
   *
   * <p>
   * This is a non-optimistic implementation. This means that an element of the input parameter List is only not
   * included in the returned result List if for no combination of Measures, non-All Members (for Hierarchies that have
   * no All Members) and evaluator default Members did the element evaluate to non-null.
   *
   * @param evaluator
   *          Evaluator
   * @param list
   *          List of members or tuples
   * @param call
   *          Calling ResolvedFunCall used to determine what Measures to use
   * @return List of elements from the input parameter list that have evaluated to non-null.
   */
  protected TupleList nonEmptyList( Evaluator evaluator, TupleList list, ResolvedFunCall call ) {
    if ( list.isEmpty() ) {
      return list;
    }

    TupleList result = TupleCollections.createList( list.getArity(), ( list.size() + 2 ) >> 1 );

    // Get all of the Measures
    final Query query = evaluator.getQuery();

    final String measureSetKey = "MEASURE_SET-" + ctag;
    Set<Member> measureSet = Util.cast( (Set) query.getEvalCache( measureSetKey ) );

    final String memberSetKey = "MEMBER_SET-" + ctag;
    Set<Member> memberSet = Util.cast( (Set) query.getEvalCache( memberSetKey ) );
    // If not in query cache, then create and place into cache.
    // This information is used for each iteration so it makes
    // sense to create and cache it.
    if ( measureSet == null || memberSet == null ) {
      measureSet = new HashSet<Member>();
      memberSet = new HashSet<Member>();
      Set<Member> queryMeasureSet = query.getMeasuresMembers();
      MeasureVisitor measureVisitor = new MeasureVisitor( measureSet, call );

      // MemberExtractingVisitor will collect the dimension members
      // referenced within the measures in the query.
      // One or more measures may conflict with the members in the tuple,
      // overriding the context of the tuple member when determining
      // non-emptiness.
      MemberExtractingVisitor memVisitor = new MemberExtractingVisitor( memberSet, call, false );

      for ( Member m : queryMeasureSet ) {
        if ( m.isCalculated() ) {
          Exp exp = m.getExpression();
          exp.accept( measureVisitor );
          exp.accept( memVisitor );
        } else {
          measureSet.add( m );
        }
      }
      Formula[] formula = query.getFormulas();
      if ( formula != null ) {
        for ( Formula f : formula ) {
          if ( SqlConstraintUtils.containsValidMeasure( f.getExpression() ) ) {
            // short circuit if VM is present.
            return list;
          }
          f.accept( measureVisitor );
        }
      }
      query.putEvalCache( measureSetKey, measureSet );
      query.putEvalCache( memberSetKey, memberSet );
    }

    final String allMemberListKey = "ALL_MEMBER_LIST-" + ctag;
    List<Member> allMemberList = Util.cast( (List) query.getEvalCache( allMemberListKey ) );

    final String nonAllMembersKey = "NON_ALL_MEMBERS-" + ctag;
    Member[][] nonAllMembers = (Member[][]) query.getEvalCache( nonAllMembersKey );
    if ( nonAllMembers == null ) {
      //
      // Get all of the All Members and those Hierarchies that
      // do not have All Members.
      //
      Member[] evalMembers = evaluator.getMembers().clone();

      List<Member> listMembers = list.get( 0 );

      // Remove listMembers from evalMembers and independentSlicerMembers
      for ( Member lm : listMembers ) {
        Hierarchy h = lm.getHierarchy();
        for ( int i = 0; i < evalMembers.length; i++ ) {
          Member em = evalMembers[i];
          if ( ( em != null ) && h.equals( em.getHierarchy() ) ) {
            evalMembers[i] = null;
          }
        }
      }

      Map<Hierarchy, Set<Member>> mapOfSlicerMembers = new HashMap<Hierarchy, Set<Member>>();
      if ( evaluator instanceof RolapEvaluator ) {
        RolapEvaluator rev = (RolapEvaluator) evaluator;
        mapOfSlicerMembers = rev.getSlicerMembersByHierarchy();
      }

      // Now we have the non-List-Members, but some of them may not be
      // All Members (default Member need not be the All Member) and
      // for some Hierarchies there may not be an All Member.
      // So we create an array of Objects some elements of which are
      // All Members and others elements will be an array of all top-level
      // Members when there is not an All Member.
      SchemaReader schemaReader = evaluator.getSchemaReader();
      allMemberList = new ArrayList<Member>();
      List<Member[]> nonAllMemberList = new ArrayList<Member[]>();

      Member em;
      boolean isSlicerMember;
      for ( Member evalMember : evalMembers ) {
        em = evalMember;
        if ( em == null ) {
          // Above we might have removed some by setting them
          // to null. These are the CrossJoin axes.
          continue;
        }
        if ( em.isMeasure() ) {
          continue;
        }

        isSlicerMember = false;
        if ( mapOfSlicerMembers != null ) {
          Set<Member> members = mapOfSlicerMembers.get( em.getHierarchy() );
          if ( members != null ) {
            isSlicerMember = members.contains( em );
          }
        }

        //
        // The unconstrained members need to be replaced by the "All"
        // member based on its usage and property. This is currently
        // also the behavior of native cross join evaluation. See
        // SqlConstraintUtils.addContextConstraint()
        //
        // on slicer? | calculated? | replace with All?
        // -----------------------------------------------
        // Y | Y | Y always
        // Y | N | N
        // N | Y | N
        // N | N | Y if not "All"
        // -----------------------------------------------
        //
        if ( ( isSlicerMember && !em.isCalculated() ) || ( !isSlicerMember && em.isCalculated() ) ) {
          // If the slicer contains multiple members from this one's
          // hierarchy, add them to nonAllMemberList
          if ( isSlicerMember ) {
            Set<Member> hierarchySlicerMembers = mapOfSlicerMembers.get( em.getHierarchy() );
            if ( hierarchySlicerMembers.size() > 1 ) {
              nonAllMemberList.add( hierarchySlicerMembers.toArray( new Member[hierarchySlicerMembers.size()] ) );
            }
          }
          continue;
        }

        // If the member is not the All member;
        // or if it is a slicer member,
        // replace with the "all" member.
        if ( isSlicerMember || !em.isAll() ) {
          Hierarchy h = em.getHierarchy();
          final List<Member> rootMemberList = schemaReader.getHierarchyRootMembers( h );
          if ( h.hasAll() ) {
            // The Hierarchy has an All member
            boolean found = false;
            for ( Member m : rootMemberList ) {
              if ( m.isAll() ) {
                allMemberList.add( m );
                found = true;
                break;
              }
            }
            if ( !found ) {
              LOGGER.warn( "CrossJoinFunDef.nonEmptyListNEW: ERROR" );
            }
          } else {
            // The Hierarchy does NOT have an All member
            Member[] rootMembers = rootMemberList.toArray( new Member[rootMemberList.size()] );
            nonAllMemberList.add( rootMembers );
          }
        }
      }
      nonAllMembers = nonAllMemberList.toArray( new Member[nonAllMemberList.size()][] );

      query.putEvalCache( allMemberListKey, allMemberList );
      query.putEvalCache( nonAllMembersKey, nonAllMembers );
    }

    //
    // Determine if there is any data.
    //
    // Put all of the All Members into Evaluator
    final int savepoint = evaluator.savepoint();
    try {
      evaluator.setContext( allMemberList );
      // Iterate over elements of the input list. If for any
      // combination of
      // Measure and non-All Members evaluation is non-null, then
      // add it to the result List.
      final TupleCursor cursor = list.tupleCursor();
      int currentIteration = 0;
      Execution execution = query.getStatement().getCurrentExecution();
      while ( cursor.forward() ) {
        cursor.setContext( evaluator );
        for ( Member member : memberSet ) {
          // memberSet contains members referenced within measures.
          // Make sure that we don't incorrectly assume a context
          // that will be changed by the measure, so conservatively
          // push context to [All] for each of the associated
          // hierarchies.
          evaluator.setContext( member.getHierarchy().getAllMember() );
        }
        // Check if the MDX query was canceled.
        // Throws an exception in case of timeout is exceeded
        // see MONDRIAN-2425
        CancellationChecker.checkCancelOrTimeout( currentIteration++, execution );
        if ( tupleContainsCalcs( cursor.current() ) || checkData( nonAllMembers, nonAllMembers.length - 1, measureSet,
            evaluator ) ) {
          result.addCurrent( cursor );
        }
      }
      return result;
    } finally {
      evaluator.restore( savepoint );
    }
  }

  private boolean tupleContainsCalcs( List<Member> current ) {
    return current.stream().anyMatch( Member::isCalculated );
  }

  /**
   * Return <code>true</code> if for some combination of Members from the nonAllMembers array of Member arrays and
   * Measures from the Set of Measures evaluate to a non-null value. Even if a particular combination is non-null, all
   * combinations are tested just to make sure that the data is loaded.
   *
   * @param nonAllMembers
   *          array of Member arrays of top-level Members for Hierarchies that have no All Member.
   * @param cnt
   *          which Member array is to be processed.
   * @param measureSet
   *          Set of all that should be tested against.
   * @param evaluator
   *          the Evaluator.
   * @return True if at least one combination evaluated to non-null.
   */
  private static boolean checkData( Member[][] nonAllMembers, int cnt, Set<Member> measureSet, Evaluator evaluator ) {
    if ( cnt < 0 ) {
      // no measures found, use standard algorithm
      if ( measureSet.isEmpty() ) {
        Object value = evaluator.evaluateCurrent();
        if ( value != null && !( value instanceof Throwable ) ) {
          return true;
        }
      } else {
        // Here we evaluate across all measures just to
        // make sure that the data is all loaded
        boolean found = false;
        for ( Member measure : measureSet ) {
          evaluator.setContext( measure );
          Object value = evaluator.evaluateCurrent();
          if ( value != null && !( value instanceof Throwable ) ) {
            found = true;
          }
        }
        return found;
      }
    } else {
      boolean found = false;
      for ( Member m : nonAllMembers[cnt] ) {
        evaluator.setContext( m );
        if ( checkData( nonAllMembers, cnt - 1, measureSet, evaluator ) ) {
          found = true;
        }
      }
      return found;
    }
    return false;
  }

  private static class ResolverImpl extends ResolverBase {
    public ResolverImpl() {
      super("Crossjoin", "Crossjoin(<Set1>, <Set2>[, <Set3>...])", "Returns the cross product of two sets.",
              mondrian.olap.Syntax.Function);
    }

    public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
    {
      if (args.length < 2) {
        return null;
      } else {
        for (int i = 0; i < args.length; i++) {
          if (!validator.canConvert(
                  i, args[i], mondrian.olap.Category.Set, conversions)) {
            return null;
          }
        }

        FunDef dummy = createDummyFunDef(this, mondrian.olap.Category.Set, args);
        return new CrossJoinFunDef(dummy);
      }
    }

    protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
      return new CrossJoinFunDef(dummyFunDef);
    }
  }


  private static class StarCrossJoinResolver extends MultiResolver {
    public StarCrossJoinResolver() {
      super( "*", "<Set1> * <Set2>", "Returns the cross product of two sets.", new String[] { "ixxx", "ixmx", "ixxm",
        "ixmm" } );
    }

    public FunDef resolve( Exp[] args, Validator validator, List<Conversion> conversions ) {
      // This function only applies in contexts which require a set.
      // Elsewhere, "*" is the multiplication operator.
      // This means that [Measures].[Unit Sales] * [Gender].[M] is
      // well-defined.
      if ( validator.requiresExpression() ) {
        return null;
      }
      return super.resolve( args, validator, conversions );
    }

    protected FunDef createFunDef( Exp[] args, FunDef dummyFunDef ) {
      return new CrossJoinFunDef( dummyFunDef );
    }
  }
}

// End CrossJoinFunDef.java
