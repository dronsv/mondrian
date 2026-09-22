/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.LevelExpr;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.NamedSetExpr;
import mondrian.mdx.ParameterExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Category;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.NamedSet;
import mondrian.olap.Parameter;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.type.DimensionType;
import mondrian.olap.type.HierarchyType;
import mondrian.olap.type.LevelType;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * What the cells of a query's calculations read (#98, #99).
 *
 * <p>One walk per member summarises every cell read of its formula: the
 * hierarchies whose coordinate a read takes away from the evaluation
 * context, the stored and native-SQL measures it reaches, and whether its
 * value is NULL whenever those cells are. Native enumeration through the
 * fact, the native pre-filter and the interpreter's non-empty optimizer all
 * decide from the same summary, so they cannot disagree about a formula.
 *
 * <p>A member or tuple in value position is a cell read at its whole
 * coordinate: the branches of IIf and CASE, the element picked by Item, the
 * tuple of ValidMeasure, and a set iterated by an aggregate. A calculated
 * member in that coordinate is read through its own formula. A coordinate
 * whose type cannot name its hierarchies (a lossy common tuple type, an
 * unknown member) fails closed: every hierarchy may shift and the value is
 * not bounded by any fact.
 *
 * <p>Summaries are query-local. They are kept in the query's evaluation
 * cache and rebuilt when parameter values change; only formula facts are
 * cached, never a decision for an evaluator coordinate.
 */
public final class CellReadAnalysis {

  /** Which measures decide whether a candidate tuple is non-empty. */
  public static final class Judges {
    private enum Kind { AXIS, CONTEXT }

    /** A NON EMPTY axis: every displayed measure and the context measure. */
    public static final Judges AXIS = new Judges( Kind.AXIS );

    /** An explicit Filter condition or ranking: the measure it selects. */
    public static final Judges CONTEXT = new Judges( Kind.CONTEXT );

    private final Kind kind;

    private Judges( Kind kind ) {
      this.kind = kind;
    }

    @Override
    public String toString() {
      return kind.name();
    }
  }

  /**
   * How the interpreter's non-empty optimizer may prune candidates.
   *
   * @param prunable false when no reset of the context bounds the judges;
   *     every candidate must then be kept
   * @param resetHierarchies hierarchies to reset to All before probing, so
   *     that a cell read at another coordinate cannot be missed
   * @param leaves stored and native-SQL measures to probe
   */
  public record NonEmptyPlan(
      boolean prunable, Set<Hierarchy> resetHierarchies, Set<Member> leaves ) {
    static final NonEmptyPlan UNPRUNABLE =
        new NonEmptyPlan( false, Collections.emptySet(), Collections.emptySet() );
  }

  /** What the cells of one member's value read. */
  static final class Summary {
    /**
     * Hierarchies whose coordinate a read takes from the context: of the
     * reads that bound the value when it is bounded, else of every read.
     */
    final Set<Hierarchy> shifted;
    /** Such a read's coordinate cannot be named: any hierarchy may shift. */
    final boolean shiftsAll;
    /** Hierarchies any read of the value takes from the context. */
    final Set<Hierarchy> readShifted;
    final boolean readShiftsAll;
    /** Stored and native-SQL measures the value reads. */
    final Set<Member> leaves;
    /** Reads a stored measure. */
    final boolean stored;
    /** Reads a measure computed by native SQL over its own fact. */
    final boolean independent;
    /** The value is NULL whenever every cell it reads is NULL. */
    final boolean bounded;

    private Summary( Walker walker, Support support ) {
      this.bounded = support.bounded();
      this.shifted = Collections.unmodifiableSet(
          bounded ? support.shifted() : walker.readShifted );
      this.shiftsAll = bounded ? support.shiftsAll() : walker.readShiftsAll;
      this.readShifted = Collections.unmodifiableSet( walker.readShifted );
      this.readShiftsAll = walker.readShiftsAll;
      this.leaves = Collections.unmodifiableSet( walker.leaves );
      this.stored = walker.stored;
      this.independent = walker.independent;
    }

    private Summary( Member leaf, boolean independent ) {
      this.shifted = Collections.emptySet();
      this.shiftsAll = false;
      this.readShifted = Collections.emptySet();
      this.readShiftsAll = false;
      this.leaves = Collections.singleton( leaf );
      this.stored = !independent;
      this.independent = independent;
      this.bounded = true;
    }

    /** A summary that proves nothing: recursion or an unknown member. */
    private Summary() {
      this.shifted = Collections.emptySet();
      this.shiftsAll = true;
      this.readShifted = Collections.emptySet();
      this.readShiftsAll = true;
      this.leaves = Collections.emptySet();
      this.stored = true;
      this.independent = false;
      this.bounded = false;
    }

    /** Neither a stored fact nor a dense value: #98's fact-less measure. */
    boolean factless() {
      return independent || !stored;
    }

    /** Enumeration through the cube's fact cannot miss a non-empty cell. */
    boolean boundedByCubeFact() {
      return bounded && !independent;
    }

    boolean mayShift( Collection<Hierarchy> hierarchies ) {
      return shiftsAll || !Collections.disjoint( shifted, hierarchies );
    }
  }

  /**
   * Whether a value is NULL whenever the cells of some reads are, and where
   * those reads happen: a non-empty value implies a non-empty cell among
   * them. Other reads, such as a denominator or an IIf condition, cannot
   * make the value non-empty.
   */
  private record Support( boolean bounded, Set<Hierarchy> shifted, boolean shiftsAll ) {
    /** A NULL value: it reads nothing and is never non-empty. */
    static final Support NULL = new Support( true, Collections.emptySet(), false );
    /** A value that may be non-empty with every cell it reads empty. */
    static final Support DENSE = new Support( false, Collections.emptySet(), false );

    /** Non-empty only when both are, so either support bounds the value. */
    Support and( Support other ) {
      if ( !bounded ) {
        return other;
      }
      if ( !other.bounded ) {
        return this;
      }
      return other.narrowerThan( this ) ? other : this;
    }

    /** Non-empty when either is. */
    Support or( Support other ) {
      if ( !bounded || !other.bounded ) {
        return DENSE;
      }
      final Set<Hierarchy> union = new LinkedHashSet<>( shifted );
      union.addAll( other.shifted );
      return new Support( true, union, shiftsAll || other.shiftsAll );
    }

    private boolean narrowerThan( Support other ) {
      return !shiftsAll && ( other.shiftsAll || shifted.size() < other.shifted.size() );
    }
  }

  private static final String CACHE_KEY = "CELL_READ_ANALYSIS";
  private static final Summary UNKNOWN = new Summary();

  /** Functions whose value aggregates an expression over a set's cells. */
  private static final Set<String> AGGREGATES = names(
      "Aggregate", "Sum", "Avg", "Min", "Max", "Median", "Stdev", "StdevP",
      "Stddev", "StddevP", "Var", "VarP", "Variance", "VarianceP" );
  /** Set functions whose elements come from their first argument. */
  private static final Set<String> SUBSETS = names(
      "Filter", "Order", "Head", "Tail", "TopCount", "BottomCount",
      "TopPercent", "BottomPercent", "TopSum", "BottomSum", "Hierarchize",
      "Distinct", "Subset", "Except", "Intersect", "Unorder" );
  /** Functions that may produce a calculated member nobody named. */
  private static final Set<String> CALCULATED_SOURCES = names(
      "StrToMember", "StrToTuple", "StrToSet", "AddCalculatedMembers", "AllMembers" );
  /** Set constructors that keep an output measure visible on an axis. */
  private static final Set<String> AXIS_CONSTRUCTORS = names( "{}", "()", "Crossjoin", "*" );

  private final List<Object> parameters;
  private final Query query;
  private final Hierarchy measuresHierarchy;
  private final Set<Hierarchy> cubeHierarchies = new LinkedHashSet<>();
  private final Set<Hierarchy> subcubeHierarchies = new LinkedHashSet<>();
  private final Set<Member> outputMeasures = new LinkedHashSet<>();
  private boolean unknownOutputMeasure;
  private final boolean validMeasure;
  private final Map<Member, Summary> summaries = new IdentityHashMap<>();
  private final Set<Member> active = Collections.newSetFromMap( new IdentityHashMap<>() );
  private final Map<List<Object>, NonEmptyPlan> plans = new java.util.HashMap<>();
  private final Map<List<Object>, Boolean> factless = new java.util.HashMap<>();
  /** Element coordinates of set expressions, which do not depend on a scope. */
  private final Map<Exp, Coord> shapes = new IdentityHashMap<>();

  private CellReadAnalysis( List<Object> parameters, Query query ) {
    this.parameters = parameters;
    this.query = query;
    Hierarchy measures = null;
    for ( Dimension dimension : query.getCube().getDimensions() ) {
      if ( dimension.isMeasures() ) {
        measures = dimension.getHierarchy();
      } else {
        cubeHierarchies.addAll( java.util.Arrays.asList( dimension.getHierarchies() ) );
      }
    }
    this.measuresHierarchy = measures;
    collectOutputMeasures();
    collectSubcubeHierarchies();
    this.validMeasure = anyFormulaUsesValidMeasure();
  }

  /** An analysis without a query: fact questions about one formula only. */
  private CellReadAnalysis( Hierarchy measuresHierarchy ) {
    this.parameters = Collections.emptyList();
    this.query = null;
    this.measuresHierarchy = measuresHierarchy;
    this.validMeasure = false;
  }

  /** The analysis of the query an evaluator runs. */
  public static CellReadAnalysis of( Evaluator evaluator ) {
    final Query query = evaluator.getQuery();
    final List<Object> parameters = new ArrayList<>();
    for ( Parameter parameter : query.getParameters() ) {
      parameters.add( parameter.getValue() );
    }
    CellReadAnalysis analysis = (CellReadAnalysis) query.getEvalCache( CACHE_KEY );
    if ( analysis == null || !analysis.parameters.equals( parameters ) ) {
      analysis = new CellReadAnalysis( parameters, query );
      query.putEvalCache( CACHE_KEY, analysis );
    }
    return analysis;
  }

  /** Whether a calculated member reads no stored fact of its cube. */
  static boolean isFactless( Member measure ) {
    return new CellReadAnalysis( measure.getHierarchy() ).summary( measure ).factless();
  }

  // -- decisions for an evaluator ---------------------------------------

  /**
   * Whether member enumeration must follow dimensions instead of the fact:
   * some judge reads no stored fact, reads an independent one, or can be
   * non-empty where every cell it reads is empty.
   */
  boolean needsFactlessEnumeration( Evaluator evaluator, Judges judges ) {
    final List<Member> context = contextMeasures( evaluator );
    if ( context == null ) {
      return true;
    }
    // Asked for every member list a query reads: decide once per measure.
    return factless.computeIfAbsent( key( judges, context ), key -> {
      final Collection<Member> measures = judges( evaluator, judges );
      if ( measures == null ) {
        return true;
      }
      for ( Member measure : measures ) {
        final Summary summary = summary( measure );
        if ( summary.factless() || !summary.bounded ) {
          return true;
        }
      }
      return false;
    } );
  }

  /** Whether a NON EMPTY axis may keep a cell the cube's fact cannot prove. */
  boolean hasUnboundedNonEmptyMeasure( Evaluator evaluator ) {
    final Collection<Member> measures = judges( evaluator, Judges.AXIS );
    if ( measures == null ) {
      return true;
    }
    for ( Member measure : measures ) {
      if ( !summary( measure ).boundedByCubeFact() ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether a formula of the query can read a coordinate of a hierarchy that
   * member enumeration constrains: a candidate hierarchy, a non-All context
   * member, or a hierarchy restricted by a subselect.
   */
  boolean mayShiftCandidateContext( Evaluator evaluator, Collection<Hierarchy> candidates ) {
    final Set<Hierarchy> hierarchies = new HashSet<>( candidates );
    hierarchies.addAll( subcubeHierarchies );
    for ( Member member : evaluator.getMembers() ) {
      if ( !member.isMeasure() && !member.isAll() ) {
        hierarchies.add( member.getHierarchy() );
      }
    }
    for ( Member measure : query.getMeasuresMembers() ) {
      if ( summary( measure ).mayShift( hierarchies ) ) {
        return true;
      }
    }
    final List<Member> context = contextMeasures( evaluator );
    if ( context == null ) {
      return true;
    }
    for ( Member measure : context ) {
      if ( summary( measure ).mayShift( hierarchies ) ) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param levels the enumerated levels, or null when the caller cannot name
   *     them and every hierarchy is a candidate
   */
  boolean mayShiftContext( Evaluator evaluator, Level[] levels ) {
    if ( levels == null ) {
      return mayShiftCandidateContext( evaluator, cubeHierarchies );
    }
    final Set<Hierarchy> hierarchies = new HashSet<>();
    for ( Level level : levels ) {
      if ( level != null && !level.getDimension().isMeasures() ) {
        hierarchies.add( level.getHierarchy() );
      }
    }
    return mayShiftCandidateContext( evaluator, hierarchies );
  }

  /**
   * How the interpreter's optimizer may prune candidates for the judges: it
   * probes their stored and native leaves with every hierarchy they may
   * shift reset to All. It must keep every candidate when a judge is not
   * bounded by its leaves, when a shift cannot be named or reset, or when a
   * formula uses ValidMeasure.
   */
  public NonEmptyPlan nonEmptyPlan( Evaluator evaluator, Judges judges ) {
    final List<Member> context = contextMeasures( evaluator );
    if ( context == null ) {
      return NonEmptyPlan.UNPRUNABLE;
    }
    return plans.computeIfAbsent( key( judges, context ), key -> computePlan( evaluator, judges ) );
  }

  /** A decision depends on the judges and on the context measures only. */
  private static List<Object> key( Judges judges, List<Member> context ) {
    final List<Object> key = new ArrayList<>( context.size() + 1 );
    key.add( judges.kind );
    for ( Member member : context ) {
      key.add( new IdentityKey( member ) );
    }
    return key;
  }

  private NonEmptyPlan computePlan( Evaluator evaluator, Judges judges ) {
    final Collection<Member> measures = judges( evaluator, judges );
    if ( validMeasure || measures == null ) {
      return NonEmptyPlan.UNPRUNABLE;
    }
    final Set<Hierarchy> resets = new LinkedHashSet<>();
    for ( Member measure : measures ) {
      final Summary summary = summary( measure );
      if ( summary.shiftsAll || !summary.bounded ) {
        return NonEmptyPlan.UNPRUNABLE;
      }
      resets.addAll( summary.shifted );
    }
    for ( Hierarchy hierarchy : resets ) {
      if ( !hierarchy.hasAll() ) {
        return NonEmptyPlan.UNPRUNABLE;
      }
    }
    // Every leaf a query measure reads may keep a candidate: probing more
    // leaves than the judges read keeps more, never fewer. The context
    // measure is probed only as a judge: without any leaf the optimizer
    // evaluates it anyway.
    final Set<Member> leaves = new LinkedHashSet<>();
    for ( Collection<Member> roots : List.of( query.getMeasuresMembers(), measures ) ) {
      for ( Member root : roots ) {
        leaves.addAll( summary( root ).leaves );
      }
    }
    return new NonEmptyPlan( true, Collections.unmodifiableSet( resets ),
        Collections.unmodifiableSet( leaves ) );
  }

  /**
   * The measures whose cells decide whether a candidate is non-empty, or
   * null when the query cannot name them.
   */
  List<Member> judges( Evaluator evaluator, Judges judges ) {
    final List<Member> context = contextMeasures( evaluator );
    if ( context == null ) {
      return null;
    }
    if ( judges.kind == Judges.Kind.CONTEXT ) {
      return context;
    }
    if ( unknownOutputMeasure ) {
      return null;
    }
    final Set<Member> measures = new LinkedHashSet<>( outputMeasures );
    measures.addAll( context );
    return new ArrayList<>( measures );
  }

  /** The context measure; the measures a compound slicer aggregates. */
  static List<Member> contextMeasures( Evaluator evaluator ) {
    final Member[] members = evaluator.getMembers();
    if ( members == null || members.length == 0 ) {
      return null;
    }
    final Member measure = members[ 0 ];
    if ( measure instanceof RolapResult.CompoundSlicerRolapMember ) {
      if ( !( evaluator instanceof RolapEvaluator rolapEvaluator ) ) {
        return null;
      }
      final Set<Member> slicerMeasures =
          rolapEvaluator.getSlicerMembersByHierarchy().get( measure.getHierarchy() );
      return slicerMeasures == null || slicerMeasures.isEmpty()
          ? null : new ArrayList<>( slicerMeasures );
    }
    return Collections.singletonList( measure );
  }

  // -- formula summaries ------------------------------------------------

  Summary summary( Member member ) {
    if ( member instanceof RolapStoredMeasure ) {
      return new Summary( member, false );
    }
    if ( isNativeSql( member ) ) {
      return new Summary( member, true );
    }
    if ( !member.isCalculated() || member.getExpression() == null ) {
      return UNKNOWN;
    }
    Summary summary = summaries.get( member );
    if ( summary == null ) {
      if ( !active.add( member ) ) {
        // A recursive formula: what the recursion reads is unknown here.
        return UNKNOWN;
      }
      try {
        final Walker walker = new Walker();
        final Support support = walker.value(
            member.getExpression(),
            new Scope( null, Collections.singleton( member.getHierarchy().getDefaultMember() ), false ) );
        summary = new Summary( walker, support );
      } finally {
        active.remove( member );
      }
      summaries.put( member, summary );
    }
    return summary;
  }

  private static boolean isNativeSql( Member member ) {
    // With native SQL off the annotations are inert and the formula runs.
    return NativeSqlConfig.isGloballyEnabled()
        && MeasureExecutionKind.forMember( member ) == MeasureExecutionKind.CALCULATED_NATIVE_SQL;
  }

  /** Coordinates an expression positions, over all of its alternatives. */
  private static final class Coord {
    final Set<Member> measures = new LinkedHashSet<>();
    /** Some alternative leaves the measure to the evaluation context. */
    boolean contextual;
    /** Some alternative selects a measure the walk cannot name. */
    boolean unknownMeasure;
    final Set<Hierarchy> shifted = new LinkedHashSet<>();
    boolean shiftsAll;
    final Set<Member> calculated = new LinkedHashSet<>();
    /** Some alternative may be a calculated member nobody named. */
    boolean unknownCalculated;

    /** The current context: no measure, no shift. */
    static Coord current() {
      final Coord coord = new Coord();
      coord.contextual = true;
      return coord;
    }

    /** No coordinate at all, such as a NULL member: reads nothing. */
    static Coord none() {
      return new Coord();
    }

    /** Either this coordinate or the other one. */
    Coord or( Coord other ) {
      measures.addAll( other.measures );
      contextual |= other.contextual;
      return merge( other );
    }

    /** This coordinate overridden by the members of the other one. */
    Coord and( Coord other ) {
      measures.addAll( other.measures );
      contextual &= other.contextual;
      return merge( other );
    }

    private Coord merge( Coord other ) {
      unknownMeasure |= other.unknownMeasure;
      shifted.addAll( other.shifted );
      shiftsAll |= other.shiftsAll;
      calculated.addAll( other.calculated );
      unknownCalculated |= other.unknownCalculated;
      return this;
    }

    Coord copy() {
      final Coord copy = new Coord().or( this );
      copy.contextual = contextual;
      return copy;
    }
  }

  /**
   * Where reads happen: the coordinates of any enclosing iteration, and the
   * measures a read that names none takes from its context.
   */
  private record Scope( Coord iteration, Set<Member> context, boolean unknownContext ) {
    /** Reads inside an expression evaluated for each element of a set. */
    Scope iterating( Coord set ) {
      final Coord at = iteration == null ? set.copy() : iteration.copy().and( set );
      final Set<Member> measures = new LinkedHashSet<>( set.measures );
      if ( set.contextual ) {
        measures.addAll( context );
      }
      return new Scope( at, measures, set.unknownMeasure || set.contextual && unknownContext );
    }
  }

  /** Accumulates what one formula reads. */
  private final class Walker {
    final Set<Hierarchy> readShifted = new LinkedHashSet<>();
    final Set<Member> leaves = new LinkedHashSet<>();
    boolean readShiftsAll;
    boolean stored;
    boolean independent;
    private final NodeVisitor nodes = new NodeVisitor();
    /** Calculated members and named sets being expanded. */
    private final Set<Object> expanding;

    Walker() {
      this( new HashSet<>() );
    }

    private Walker( Set<Object> expanding ) {
      this.expanding = expanding;
    }

    /** The node an expression dispatches to; itself when it cannot say. */
    private Exp node( Exp expression ) {
      try {
        return expression.accept( nodes ) instanceof Exp node ? node : expression;
      } catch ( UnsupportedOperationException e ) {
        return expression;
      }
    }

    /** A read the walk cannot see through: any measure, any coordinate. */
    private Support unknown() {
      readShiftsAll = true;
      stored = true;
      return Support.DENSE;
    }

    /** Analyses a scalar value. */
    Support value( Exp expression, Scope scope ) {
      final Exp e = node( expression );
      if ( isCoordinate( e.getType() ) ) {
        return read( coordinate( e, scope ), scope );
      }
      if ( e instanceof Literal literal ) {
        return literal.getValue() == null ? Support.NULL : Support.DENSE;
      }
      if ( e instanceof ParameterExpr parameterExpr ) {
        final Parameter parameter = parameterExpr.getParameter();
        return parameter.getValue() == null
            ? value( parameter.getDefaultExp(), scope ) : Support.DENSE;
      }
      if ( e instanceof ResolvedFunCall call ) {
        return valueOf( call, scope );
      }
      return unknown();
    }

    private Support valueOf( ResolvedFunCall call, Scope scope ) {
      final String name = call.getFunName();
      final int count = call.getArgCount();
      if ( is( name, "()" ) && count == 1 ) {
        return value( call.getArg( 0 ), scope );
      }
      if ( is( name, "*" ) ) {
        // NULL times anything is NULL: either factor bounds the product.
        final Support left = value( call.getArg( 0 ), scope );
        return left.and( value( call.getArg( 1 ), scope ) );
      }
      if ( is( name, "/" ) ) {
        // The default division semantics return Infinity for a NULL denominator.
        final Support numerator = value( call.getArg( 0 ), scope );
        value( call.getArg( 1 ), scope );
        return numerator;
      }
      if ( is( name, "+" ) || is( name, "-" ) || is( name, "CoalesceEmpty" ) ) {
        Support support = Support.NULL;
        for ( Exp arg : call.getArgs() ) {
          support = support.or( value( arg, scope ) );
        }
        return support;
      }
      if ( is( name, "IIf" ) ) {
        value( call.getArg( 0 ), scope );
        final Support left = value( call.getArg( 1 ), scope );
        return left.or( value( call.getArg( 2 ), scope ) );
      }
      if ( is( name, "_CaseTest" ) || is( name, "_CaseMatch" ) ) {
        int i = 0;
        if ( is( name, "_CaseMatch" ) ) {
          value( call.getArg( i++ ), scope );
        }
        Support support = Support.NULL;
        for ( ; i + 1 < count; i += 2 ) {
          value( call.getArg( i ), scope );
          support = support.or( value( call.getArg( i + 1 ), scope ) );
        }
        // Without ELSE an unmatched CASE is NULL.
        return i < count ? support.or( value( call.getArg( i ), scope ) ) : support;
      }
      if ( AGGREGATES.contains( name ) ) {
        // The overloads keep the operand's Member category, but the compiler
        // evaluates the second argument as a scalar for each element.
        final Coord set = set( call.getArg( 0 ), scope );
        return count == 1 ? read( set, scope ) : value( call.getArg( 1 ), scope.iterating( set ) );
      }
      if ( is( name, "Count" ) ) {
        final Coord set = set( call.getArg( 0 ), scope );
        if ( count == 2 && call.getArg( 1 ) instanceof Literal flag
            && "EXCLUDEEMPTY".equalsIgnoreCase( String.valueOf( flag.getValue() ) ) ) {
          read( set, scope );
        }
        // Zero is not empty.
        return Support.DENSE;
      }
      if ( is( name, "Value" ) ) {
        return read( coordinate( call.getArg( 0 ), scope ), scope );
      }
      if ( is( name, "ValidMeasure" ) ) {
        return read( validMeasureCoordinate( coordinate( call.getArg( 0 ), scope ) ), scope );
      }
      // Comparisons, IsEmpty, strings and metadata can be non-NULL from nothing.
      operands( call, scope );
      return Support.DENSE;
    }

    /** ValidMeasure resets the hierarchies its measures' cubes do not join. */
    private Coord validMeasureCoordinate( Coord coord ) {
      if ( coord.contextual || coord.unknownMeasure ) {
        coord.shiftsAll = true;
      }
      for ( Member measure : coord.measures ) {
        if ( measure instanceof RolapStoredMeasure stored ) {
          for ( Hierarchy hierarchy : cubeHierarchies ) {
            if ( !( hierarchy instanceof RolapHierarchy rolapHierarchy )
                || stored.getCube().findBaseCubeHierarchy( rolapHierarchy ) == null ) {
              coord.shifted.add( hierarchy );
            }
          }
        } else {
          coord.shiftsAll = true;
        }
      }
      return coord;
    }

    /** A cell read at a coordinate. */
    private Support read( Coord coord, Scope scope ) {
      final Coord at = scope.iteration() == null ? coord : scope.iteration().copy().and( coord );
      readShifted.addAll( at.shifted );
      readShiftsAll |= at.shiftsAll;
      final Set<Member> measures = new LinkedHashSet<>( coord.measures );
      boolean unknownMeasure = coord.unknownMeasure;
      if ( coord.contextual ) {
        measures.addAll( scope.context() );
        unknownMeasure |= scope.unknownContext();
      }
      if ( measures.isEmpty() && !unknownMeasure ) {
        return Support.NULL;
      }
      if ( unknownMeasure ) {
        unknown();
      }
      // The cell is the measure's value there, or a calculated member's.
      Support support = new Support( !unknownMeasure, at.shifted, at.shiftsAll );
      for ( Member measure : measures ) {
        support = support.or( readMeasure( measure, at ) );
      }
      for ( Member member : at.calculated ) {
        support = support.or( readCalculated( member, measures, unknownMeasure ) );
      }
      return at.unknownCalculated ? unknown() : support;
    }

    private Support readMeasure( Member measure, Coord at ) {
      final Summary summary = summary( measure );
      readShifted.addAll( summary.readShifted );
      readShiftsAll |= summary.readShiftsAll;
      leaves.addAll( summary.leaves );
      stored |= summary.stored;
      independent |= summary.independent;
      final Set<Hierarchy> shifted = new LinkedHashSet<>( at.shifted );
      shifted.addAll( summary.shifted );
      return new Support( summary.bounded, shifted, at.shiftsAll || summary.shiftsAll );
    }

    /**
     * A calculated member in a coordinate replaces the cell by its own
     * formula, evaluated with its hierarchy at the default member and the
     * read's measure current (solve order aside: both are analysed).
     */
    private Support readCalculated( Member member, Set<Member> measures, boolean unknownMeasure ) {
      readShifted.add( member.getHierarchy() );
      if ( member.getExpression() == null || !expanding.add( member ) ) {
        return unknown();
      }
      try {
        final Support formula =
            value( member.getExpression(), new Scope( null, measures, unknownMeasure ) );
        final Set<Hierarchy> shifted = new LinkedHashSet<>( formula.shifted() );
        shifted.add( member.getHierarchy() );
        return new Support( formula.bounded(), shifted, formula.shiftsAll() );
      } finally {
        expanding.remove( member );
      }
    }

    /** The coordinate a member- or tuple-valued expression positions. */
    Coord coordinate( Exp expression, Scope scope ) {
      final Exp e = node( expression );
      if ( e instanceof MemberExpr memberExpr ) {
        return member( memberExpr.getMember() );
      }
      if ( e instanceof HierarchyExpr || e instanceof DimensionExpr ) {
        // A hierarchy in a tuple stands for its current member.
        return Coord.current();
      }
      if ( e instanceof Literal literal && literal.getValue() == null ) {
        return Coord.none();
      }
      if ( e instanceof ParameterExpr parameterExpr ) {
        final Parameter parameter = parameterExpr.getParameter();
        if ( parameter.getValue() instanceof Member member ) {
          return member( member );
        }
        return parameter.getValue() == null
            ? coordinate( parameter.getDefaultExp(), scope ) : typed( e.getType() );
      }
      if ( e instanceof NamedSetExpr ) {
        return set( e, scope );
      }
      if ( e instanceof ResolvedFunCall call ) {
        return coordinateOf( call, scope );
      }
      final Coord coord = typed( null );
      unknown();
      return coord;
    }

    private Coord member( Member member ) {
      final Coord coord = Coord.current();
      if ( member.isMeasure() ) {
        coord.contextual = false;
        coord.measures.add( member );
      } else {
        coord.shifted.add( member.getHierarchy() );
        if ( member.isCalculated() ) {
          coord.calculated.add( member );
        }
      }
      return coord;
    }

    private Coord coordinateOf( ResolvedFunCall call, Scope scope ) {
      final String name = call.getFunName();
      final int count = call.getArgCount();
      if ( is( name, "()" ) ) {
        final Coord tuple = Coord.current();
        for ( Exp arg : call.getArgs() ) {
          tuple.and( coordinate( arg, scope ) );
        }
        return tuple;
      }
      if ( is( name, "CurrentMember" ) ) {
        operands( call, scope );
        return Coord.current();
      }
      if ( is( name, "DefaultMember" ) && call.getType().getHierarchy() != null ) {
        operands( call, scope );
        return member( call.getType().getHierarchy().getDefaultMember() );
      }
      if ( is( name, "IIf" ) ) {
        value( call.getArg( 0 ), scope );
        return coordinate( call.getArg( 1 ), scope ).or( coordinate( call.getArg( 2 ), scope ) );
      }
      if ( is( name, "_CaseTest" ) || is( name, "_CaseMatch" ) ) {
        final Coord branches = Coord.none();
        int i = 0;
        if ( is( name, "_CaseMatch" ) ) {
          value( call.getArg( i++ ), scope );
        }
        for ( ; i + 1 < count; i += 2 ) {
          value( call.getArg( i ), scope );
          branches.or( coordinate( call.getArg( i + 1 ), scope ) );
        }
        return i < count ? branches.or( coordinate( call.getArg( i ), scope ) ) : branches;
      }
      if ( is( name, "Item" ) ) {
        for ( int i = 1; i < count; i++ ) {
          value( call.getArg( i ), scope );
        }
        if ( call.getArg( 0 ).getType() instanceof SetType ) {
          return set( call.getArg( 0 ), scope );
        }
        // One member of a tuple: maybe not its measure.
        final Coord member = coordinate( call.getArg( 0 ), scope );
        member.contextual = true;
        return member;
      }
      final Coord coord = typed( call.getType() );
      coord.unknownCalculated |= CALCULATED_SOURCES.contains( name );
      operands( call, scope );
      return coord;
    }

    /** The coordinates of a set's elements; records the reads that build it. */
    Coord set( Exp expression, Scope scope ) {
      final Exp e = node( expression );
      if ( !( e.getType() instanceof SetType ) ) {
        return coordinate( e, scope );
      }
      if ( e instanceof NamedSetExpr namedSetExpr ) {
        return namedSet( namedSetExpr );
      }
      if ( !( e instanceof ResolvedFunCall call ) ) {
        final Coord coord = typed( e.getType() );
        if ( !( e instanceof ParameterExpr ) ) {
          unknown();
        }
        return coord;
      }
      final String name = call.getFunName();
      if ( is( name, "{}" ) || is( name, "Union" ) ) {
        final Coord elements = Coord.none();
        for ( Exp arg : call.getArgs() ) {
          elements.or( set( arg, scope ) );
        }
        return elements;
      }
      if ( is( name, "Crossjoin" ) || is( name, "*" ) || is( name, "NonEmptyCrossJoin" ) ) {
        final Coord tuples = Coord.current();
        for ( Exp arg : call.getArgs() ) {
          tuples.and( set( arg, scope ) );
        }
        if ( is( name, "NonEmptyCrossJoin" ) ) {
          read( tuples, scope );
        }
        return tuples;
      }
      if ( is( name, "NonEmpty" ) ) {
        final Coord elements = set( call.getArg( 0 ), scope );
        read( call.getArgCount() > 1
            ? elements.copy().and( set( call.getArg( 1 ), scope ) ) : elements, scope );
        return elements;
      }
      if ( SUBSETS.contains( name ) ) {
        final Coord elements = set( call.getArg( 0 ), scope );
        final Scope each = scope.iterating( elements );
        for ( int i = 1; i < call.getArgCount(); i++ ) {
          operand( call, i, each );
        }
        return elements;
      }
      final Coord coord = typed( call.getType() );
      coord.unknownCalculated |= CALCULATED_SOURCES.contains( name );
      operands( call, scope );
      return coord;
    }

    /**
     * The coordinates of a set's elements, without recording the reads that
     * build them: those happen for the call using the set, in its scope.
     */
    private Coord shape( Exp set ) {
      Coord shape = shapes.get( set );
      if ( shape == null ) {
        shape = new Walker( expanding ).set( set, new Scope( null, Collections.emptySet(), false ) );
        shapes.put( set, shape );
      }
      return shape.copy();
    }

    /**
     * A named set is evaluated once, in the query context: its members are
     * fixed, so every hierarchy of its type is a shift, and the reads that
     * built it do not depend on the cell.
     */
    private Coord namedSet( NamedSetExpr expression ) {
      final NamedSet namedSet = expression.getNamedSet();
      final Coord coord = typed( expression.getType() );
      if ( expanding.add( namedSet ) ) {
        try {
          final Coord elements = shape( namedSet.getExp() );
          coord.measures.addAll( elements.measures );
          coord.contextual = elements.contextual;
          coord.unknownMeasure = elements.unknownMeasure;
          coord.calculated.addAll( elements.calculated );
          coord.unknownCalculated |= elements.unknownCalculated;
          coord.shiftsAll |= elements.shiftsAll;
        } finally {
          expanding.remove( namedSet );
        }
      } else {
        coord.unknownCalculated = true;
      }
      return coord;
    }

    /**
     * The arguments of a call used for its structure. Scalar arguments may be
     * evaluated for each element of any set argument.
     */
    private void operands( ResolvedFunCall call, Scope scope ) {
      Coord elements = null;
      for ( Exp arg : call.getArgs() ) {
        if ( arg.getType() instanceof SetType ) {
          elements = elements == null ? shape( arg ) : elements.or( shape( arg ) );
        }
      }
      final Scope each = elements == null ? scope : scope.iterating( elements );
      for ( int i = 0; i < call.getArgCount(); i++ ) {
        operand( call, i, each );
      }
    }

    private void operand( ResolvedFunCall call, int i, Scope scope ) {
      final Exp arg = call.getArg( i );
      final int[] categories = call.getFunDef().getParameterCategories();
      final boolean scalar = categories != null && i < categories.length
          ? Category.isScalar( categories[ i ] )
          : !isCoordinate( arg.getType() ) && !( arg.getType() instanceof SetType );
      if ( scalar ) {
        value( arg, scope );
        return;
      }
      final Exp e = node( arg );
      if ( e instanceof ResolvedFunCall nested ) {
        if ( nested.getType() instanceof SetType ) {
          set( nested, scope );
        } else if ( isCoordinate( nested.getType() ) ) {
          coordinate( nested, scope );
        } else {
          operands( nested, scope );
        }
      } else if ( e instanceof Id || e instanceof UnresolvedFunCall ) {
        unknown();
      }
      // Members, levels, hierarchies, literals and named sets used for their
      // structure read no cell of this formula.
    }

    /** What a type alone says about a coordinate. */
    private Coord typed( Type type ) {
      final Coord coord = Coord.current();
      addType( type instanceof SetType setType ? setType.getElementType() : type, coord );
      return coord;
    }

    private void addType( Type type, Coord coord ) {
      if ( type instanceof TupleType tuple ) {
        for ( Type element : tuple.elementTypes ) {
          addType( element, coord );
        }
        return;
      }
      if ( type instanceof MemberType || type instanceof HierarchyType
          || type instanceof DimensionType || type instanceof LevelType ) {
        if ( measuresHierarchy == null || type.usesHierarchy( measuresHierarchy, false ) ) {
          coord.unknownMeasure = true;
        }
        for ( Hierarchy hierarchy : cubeHierarchies ) {
          if ( type.usesHierarchy( hierarchy, false ) ) {
            coord.shifted.add( hierarchy );
          }
        }
        return;
      }
      // A scalar where a member was expected: the common type lost it.
      coord.shiftsAll = true;
      coord.unknownMeasure = true;
    }
  }

  private static boolean isCoordinate( Type type ) {
    return type instanceof MemberType || type instanceof TupleType
        || type instanceof HierarchyType || type instanceof DimensionType;
  }

  private static boolean is( String name, String expected ) {
    return expected.equalsIgnoreCase( name );
  }

  private static Set<String> names( String... names ) {
    final Set<String> set = new TreeSet<>( String.CASE_INSENSITIVE_ORDER );
    Collections.addAll( set, names );
    return Collections.unmodifiableSet( set );
  }

  /** Dispatches through accept(), so an expression wrapper reaches its node. */
  private static final class NodeVisitor extends MdxVisitorImpl {
    private Object node( Exp exp ) {
      turnOffVisitChildren();
      return exp;
    }

    @Override public Object visit( UnresolvedFunCall call ) { return node( call ); }
    @Override public Object visit( ResolvedFunCall call ) { return node( call ); }
    @Override public Object visit( Id id ) { return node( id ); }
    @Override public Object visit( ParameterExpr parameterExpr ) { return node( parameterExpr ); }
    @Override public Object visit( DimensionExpr dimensionExpr ) { return node( dimensionExpr ); }
    @Override public Object visit( HierarchyExpr hierarchyExpr ) { return node( hierarchyExpr ); }
    @Override public Object visit( LevelExpr levelExpr ) { return node( levelExpr ); }
    @Override public Object visit( MemberExpr memberExpr ) { return node( memberExpr ); }
    @Override public Object visit( NamedSetExpr namedSetExpr ) { return node( namedSetExpr ); }
    @Override public Object visit( Literal literal ) { return node( literal ); }
  }

  /** Identity for keys built from members and argument arrays. */
  private record IdentityKey( Object value ) {
    @Override
    public boolean equals( Object o ) {
      return o instanceof IdentityKey other && other.value == value;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode( value );
    }
  }

  // -- query structure --------------------------------------------------

  private void collectOutputMeasures() {
    final Set<NamedSet> activeSets = new HashSet<>();
    final MdxVisitorImpl visitor = new MdxVisitorImpl() {
      @Override
      public Object visit( MemberExpr expression ) {
        if ( expression.getMember().isMeasure() ) {
          outputMeasures.add( expression.getMember() );
        }
        return null;
      }

      @Override
      public Object visit( NamedSetExpr expression ) {
        final NamedSet set = expression.getNamedSet();
        if ( expression.getType().usesHierarchy( measuresHierarchy, false ) && activeSets.add( set ) ) {
          set.getExp().accept( this );
          activeSets.remove( set );
        }
        turnOffVisitChildren();
        return null;
      }

      @Override
      public Object visit( ParameterExpr expression ) {
        final Object value = expression.getParameter().getValue();
        if ( value instanceof Member member ) {
          visit( new MemberExpr( member ) );
        } else {
          unknownOutputMeasure |= expression.getType().usesHierarchy( measuresHierarchy, false );
        }
        return null;
      }

      @Override
      public Object visit( ResolvedFunCall call ) {
        if ( call.getType() instanceof mondrian.olap.type.ScalarType
            || !call.getType().usesHierarchy( measuresHierarchy, false ) ) {
          // Scalar Filter/Order dependencies cannot supply an output
          // Measures coordinate, even when their formulas read measures.
          turnOffVisitChildren();
        } else if ( !AXIS_CONSTRUCTORS.contains( call.getFunName() ) ) {
          unknownOutputMeasure = true;
        }
        return null;
      }
    };
    for ( QueryAxis axis : query.getAxes() ) {
      axis.getSet().accept( visitor );
    }
    if ( query.getSlicerAxis() != null ) {
      query.getSlicerAxis().getSet().accept( visitor );
    }
  }

  /**
   * Hierarchies a subselect restricts, from its axes, including nested
   * subselects and their slicers. Subcube axes can retain unresolved ASTs:
   * they are named, never evaluated, while their own predicates are built.
   */
  private void collectSubcubeHierarchies() {
    if ( query.getSubcube() == null ) {
      return;
    }
    final MdxVisitorImpl visitor = new MdxVisitorImpl() {
      @Override
      public Object visit( MemberExpr expression ) {
        if ( !expression.getMember().isMeasure() && !expression.getMember().isAll() ) {
          subcubeHierarchies.add( expression.getMember().getHierarchy() );
        }
        return null;
      }

      @Override
      public Object visit( Id id ) {
        final String name = id.toString();
        boolean matched = name.startsWith( "[Measures]." );
        for ( Hierarchy hierarchy : cubeHierarchies ) {
          if ( name.equals( hierarchy.getUniqueName() )
              || name.startsWith( hierarchy.getUniqueName() + "." ) ) {
            subcubeHierarchies.add( hierarchy );
            matched = true;
          }
        }
        if ( !matched ) {
          subcubeHierarchies.addAll( cubeHierarchies );
        }
        return null;
      }

      @Override
      public Object visit( ResolvedFunCall call ) {
        for ( Hierarchy hierarchy : cubeHierarchies ) {
          if ( call.getType().usesHierarchy( hierarchy, false ) ) {
            subcubeHierarchies.add( hierarchy );
          }
        }
        return null;
      }

      @Override
      public Object visit( UnresolvedFunCall call ) {
        if ( !AXIS_CONSTRUCTORS.contains( call.getFunName() ) ) {
          subcubeHierarchies.addAll( cubeHierarchies );
        }
        return null;
      }
    };
    for ( Exp expression : query.getSubcube().getAxisExps() ) {
      expression.accept( visitor );
    }
  }

  private boolean anyFormulaUsesValidMeasure() {
    final Formula[] formulas = query.getFormulas();
    if ( formulas != null ) {
      for ( Formula formula : formulas ) {
        if ( SqlConstraintUtils.containsValidMeasure( formula.getExpression() ) ) {
          return true;
        }
      }
    }
    return false;
  }
}

// End CellReadAnalysis.java
