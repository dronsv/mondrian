/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

/**
 * Computes a Filter(set, condition) in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeFilter extends RolapNativeSet {

  public RolapNativeFilter() {
    super.setEnabled( MondrianProperties.instance().EnableNativeFilter.get() );
  }

  static class FilterConstraint extends SetConstraint {
    Exp filterExpr;
    private final RolapStoredMeasure storedMeasure;

    public FilterConstraint(
        CrossJoinArg[] args,
        RolapEvaluator evaluator,
        Exp filterExpr,
        RolapStoredMeasure storedMeasure ) {
      super( args, evaluator, true );
      this.filterExpr = filterExpr;
      this.storedMeasure = storedMeasure;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Overriding isJoinRequired() for native filters because we have to force a join to the fact table if the filter
     * expression references a measure.
     */
    protected boolean isJoinRequired() {
      // Use a visitor and check all member expressions.
      // If any of them is a measure, we will have to
      // force the join to the fact table. If it is something
      // else then we don't really care. It will show up in
      // the evaluator as a non-all member and trigger the
      // join when we call RolapNativeSet.isJoinRequired().
      final AtomicBoolean mustJoin = new AtomicBoolean( false );
      filterExpr.accept( new MdxVisitorImpl() {
        public Object visit( MemberExpr memberExpr ) {
          if ( memberExpr.getMember().isMeasure() ) {
            mustJoin.set( true );
            return null;
          }
          return super.visit( memberExpr );
        }
      } );
      return mustJoin.get() || ( getEvaluator().isNonEmpty() && super.isJoinRequired() );
    }

    public void addConstraint( SqlQuery sqlQuery, RolapCube baseCube, AggStar aggStar ) {
      final RolapEvaluator evaluator = (RolapEvaluator) getEvaluator();
      final int savepoint = evaluator.savepoint();
      try {
        overrideNativeContext( evaluator );

        // Use aggregate table to generate filter condition
        RolapNativeSql sql =
            new RolapNativeSql( sqlQuery, aggStar, evaluator, args[0].getLevel() );
        String filterSql = sql.generateFilterCondition( filterExpr );
        if ( filterSql != null ) {
          sqlQuery.addHaving( filterSql );
        }

        if ( evaluator.isNonEmpty() || isJoinRequired() ) {
          // only apply context constraint if non empty, or
          // if a join is required to fulfill the filter condition
          super.addConstraint( sqlQuery, baseCube, aggStar );
        }
      } finally {
        evaluator.restore( savepoint );
      }
    }

    private void overrideNativeContext( RolapEvaluator evaluator ) {
      overrideContextForNativeFilter(
          evaluator,
          args,
          storedMeasure );
    }

    public boolean isSuported( DataSource ds ) {
      Evaluator evaluator = this.getEvaluator();
      SqlQuery testQuery = SqlQuery.newQuery( ds, "testQuery" );
      SqlTupleReader sqlTupleReader = new SqlTupleReader( this );
      final RolapCube cube = resolveConstraintBaseCube();
      if ( cube == null ) {
        return false;
      }

      Role role = evaluator.getSchemaReader().getRole();
      RolapSchemaReader reader = new RolapSchemaReader( role, evaluator.getSchemaReader().getSchema() );

      for ( CrossJoinArg arg : args ) {
        addLevel( sqlTupleReader, reader, arg );
      }

      this.addConstraint( testQuery, cube, sqlTupleReader.chooseAggStar( this, evaluator, cube ) );
      return testQuery.isSupported();
    }

    RolapCube resolveConstraintBaseCube() {
      return resolveConstraintBaseCube( getEvaluator(), filterExpr );
    }

    static RolapCube resolveConstraintBaseCube(
        Evaluator evaluator,
        Exp filterExpr )
    {
      final RolapCube cube = evaluator == null
        ? null
        : (RolapCube) evaluator.getCube();
      if ( cube == null || !cube.isVirtual() ) {
        return cube;
      }
      final RolapCube filterMeasureCube = resolveMeasureBaseCube( filterExpr );
      if ( filterMeasureCube != null ) {
        return filterMeasureCube;
      }
      final Query query = evaluator.getQuery();
      if ( query == null ) {
        return null;
      }
      final List<RolapCube> baseCubes = query.getBaseCubes();
      if ( baseCubes == null || baseCubes.isEmpty() ) {
        return null;
      }
      return baseCubes.get( 0 );
    }

    static RolapCube resolveMeasureBaseCube( Exp expression ) {
      if ( expression == null ) {
        return null;
      }
      final RolapCube[] resolvedCube = { null };
      final boolean[] ambiguous = { false };
      expression.accept( new MdxVisitorImpl() {
        public Object visit( MemberExpr memberExpr ) {
          final Member member = memberExpr.getMember();
          final RolapCube memberCube = resolveMemberBaseCube( member );
          if ( memberCube == null ) {
            return super.visit( memberExpr );
          }
          if ( resolvedCube[ 0 ] == null ) {
            resolvedCube[ 0 ] = memberCube;
          } else if ( resolvedCube[ 0 ] != memberCube ) {
            ambiguous[ 0 ] = true;
          }
          return super.visit( memberExpr );
        }
      } );
      return ambiguous[ 0 ] ? null : resolvedCube[ 0 ];
    }

    static RolapStoredMeasure resolveStoredMeasureFromExpression( Exp expression ) {
      if ( expression == null ) {
        return null;
      }
      final RolapStoredMeasure[] resolvedMeasure = { null };
      final RolapCube[] resolvedCube = { null };
      final boolean[] ambiguous = { false };
      expression.accept( new MdxVisitorImpl() {
        public Object visit( MemberExpr memberExpr ) {
          final Member member = memberExpr.getMember();
          if ( !( member instanceof RolapStoredMeasure ) || member.isCalculated() ) {
            return super.visit( memberExpr );
          }
          final RolapStoredMeasure storedMeasure =
              (RolapStoredMeasure) member;
          final RolapCube measureCube = storedMeasure.getCube();
          if ( resolvedMeasure[ 0 ] == null ) {
            resolvedMeasure[ 0 ] = storedMeasure;
            resolvedCube[ 0 ] = measureCube;
          } else if ( resolvedCube[ 0 ] != measureCube ) {
            ambiguous[ 0 ] = true;
          }
          return super.visit( memberExpr );
        }
      } );
      return ambiguous[ 0 ] ? null : resolvedMeasure[ 0 ];
    }

    static RolapCube resolveMemberBaseCube( Member member ) {
      if ( member instanceof RolapStoredMeasure ) {
        return ( (RolapStoredMeasure) member ).getCube();
      }
      if ( member instanceof RolapCalculatedMember ) {
        return ( (RolapCalculatedMember) member ).getBaseCube();
      }
      return null;
    }

    private void addLevel( TupleReader tr, RolapSchemaReader schemaReader, CrossJoinArg arg ) {
      RolapLevel level = arg.getLevel();
      if ( level == null ) {
        // Level can be null if the CrossJoinArg represent
        // an empty set.
        // This is used to push down the "1 = 0" predicate
        // into the emerging CJ so that the entire CJ can
        // be natively evaluated.
        tr.incrementEmptySets();
        return;
      }

      RolapHierarchy hierarchy = level.getHierarchy();
      MemberReader mr = schemaReader.getMemberReader( hierarchy );
      MemberBuilder mb = mr.getMemberBuilder();
      Util.assertTrue( mb != null, "MemberBuilder not found" );

      tr.addLevelMembers( level, mb, null );
    }

    public Object getCacheKey() {
      List<Object> key = new ArrayList<Object>();
      key.add( super.getCacheKey() );
      // Note required to use string in order for caching to work
      if ( filterExpr != null ) {
        key.add( filterExpr.toString() );
      }
      key.add( getEvaluator().isNonEmpty() );

      if ( this.getEvaluator() instanceof RolapEvaluator ) {
        key.add( ( (RolapEvaluator) this.getEvaluator() ).getSlicerMembers() );
      }

      return key;
    }
  }

  protected boolean restrictMemberTypes() {
    return true;
  }

  boolean isValidContext( RolapEvaluator evaluator, CrossJoinArg[] cjArgs ) {
    if ( evaluator == null ) {
      return false;
    }
    if ( evaluator.getCube() instanceof RolapCube
      && ( (RolapCube) evaluator.getCube() ).isVirtual() ) {
      return FilterConstraint.isValidContext(
        evaluator,
        false,
        collectLevels( cjArgs ),
        restrictMemberTypes() );
    }
    return FilterConstraint.isValidContext(
      evaluator,
      restrictMemberTypes() );
  }

  static Level[] collectLevels( CrossJoinArg[] cjArgs ) {
    if ( cjArgs == null || cjArgs.length == 0 ) {
      return new Level[0];
    }
    final List<Level> levels = new ArrayList<Level>( cjArgs.length );
    for ( CrossJoinArg cjArg : cjArgs ) {
      if ( cjArg == null || cjArg.getLevel() == null ) {
        continue;
      }
      levels.add( cjArg.getLevel() );
    }
    return levels.toArray( new Level[levels.size()] );
  }

  private static void overrideContextForNativeFilter(
      RolapEvaluator evaluator,
      CrossJoinArg[] cargs,
      RolapStoredMeasure storedMeasure ) {
    SchemaReader schemaReader = evaluator.getSchemaReader();
    for ( CrossJoinArg carg : cargs ) {
      RolapLevel level = carg.getLevel();
      if ( level != null ) {
        RolapHierarchy hierarchy = level.getHierarchy();

        final Member contextMember;
        if ( hierarchy.hasAll()
            || schemaReader.getRole().getAccess( hierarchy ) == Access.ALL ) {
          contextMember =
              schemaReader.substitute( hierarchy.getAllMember() );
        } else {
          contextMember = new RestrictedMemberReader.MultiCardinalityDefaultMember(
              hierarchy.getMemberReader().getRootMembers().get( 0 ) );
        }
        evaluator.setContext( contextMember );
      }
    }
    if ( storedMeasure != null ) {
      evaluator.setContext( storedMeasure );
    }
  }

  /**
   * Classifies each calculated member in the evaluator context to decide
   * whether native filter evaluation is safe.
   *
   * <p>Three tiers:
   * <ol>
   *   <li><b>Measures</b> — safe if
   *       {@link SqlConstraintUtils#resolveStoredMeasureCarrier} can extract a
   *       stored measure from a single base cube.  The resolved measure is
   *       substituted by {@link #overrideContextForNativeFilter} before SQL
   *       generation.</li>
   *   <li><b>Expandable dimension calc members</b> (Aggregate, parentheses, +)
   *       — already handled by
   *       {@link SqlConstraintUtils#expandSupportedCalculatedMembers}.</li>
   *   <li><b>Non-expandable dimension calculated members</b>
   *       <ul>
   *         <li>3a: on a hierarchy covered by a CrossJoinArg —
   *             {@link #overrideContextForNativeFilter} resets it to All.
   *             Safe.</li>
   *         <li>3b: on a hierarchy <em>not</em> covered — acts as a slicer
   *             constraint that cannot be expressed in SQL.  Unsafe.</li>
   *       </ul>
   *   </li>
   * </ol>
   *
   * @param contextMembers non-all members from the evaluator context
   * @param cjArgs         crossjoin arguments whose hierarchies will be reset
   *                       to All during native constraint generation
   * @return true if any member makes native evaluation unsafe
   */
  static boolean hasUnsafeCalculatedMembers(
      Member[] contextMembers,
      CrossJoinArg[] cjArgs )
  {
    // Collect hierarchies covered by crossjoin args — these get reset
    // to All by overrideContextForNativeFilter before SQL is generated.
    final Set<Hierarchy> crossJoinHierarchies = new HashSet<Hierarchy>();
    for ( CrossJoinArg cjArg : cjArgs ) {
      if ( cjArg != null && cjArg.getLevel() != null ) {
        crossJoinHierarchies.add( cjArg.getLevel().getHierarchy() );
      }
    }

    for ( Member member : contextMembers ) {
      if ( member == null || !member.isCalculated() ) {
        continue;
      }

      // Tier 1: Calculated measure
      if ( member.isMeasure() ) {
        final RolapStoredMeasure resolved =
            SqlConstraintUtils.resolveStoredMeasureCarrier( member );
        if ( resolved == null ) {
          LOGGER.debug(
              "hasUnsafeCalculatedMembers: rejecting — calc measure {} "
                  + "cannot be resolved to a single stored measure",
              member.getUniqueName() );
          return true;
        }
        LOGGER.debug(
            "hasUnsafeCalculatedMembers: allowing calc measure {} "
                + "(resolved to {})",
            member.getUniqueName(),
            resolved.getUniqueName() );
        continue;
      }

      // Tier 2: Expandable dimension calculated member (Aggregate, (), +)
      if ( SqlConstraintUtils.isSupportedCalculatedMember( member ) ) {
        LOGGER.debug(
            "hasUnsafeCalculatedMembers: allowing expandable calc member {}",
            member.getUniqueName() );
        continue;
      }

      // Tier 3: Non-expandable dimension calculated member
      if ( crossJoinHierarchies.contains( member.getHierarchy() ) ) {
        // 3a: on a CJ hierarchy — will be reset to All
        LOGGER.debug(
            "hasUnsafeCalculatedMembers: allowing non-expandable calc member "
                + "{} on CJ hierarchy {}",
            member.getUniqueName(),
            member.getHierarchy().getUniqueName() );
        continue;
      }

      // 3b: not on a CJ hierarchy — can't express in SQL
      LOGGER.debug(
          "hasUnsafeCalculatedMembers: rejecting — non-expandable calc member "
              + "{} on non-CJ hierarchy {}",
          member.getUniqueName(),
          member.getHierarchy().getUniqueName() );
      return true;
    }
    return false;
  }

  NativeEvaluator createEvaluator( RolapEvaluator evaluator, FunDef fun, Exp[] args ) {
    if ( !isEnabled() ) {
      return null;
    }
    // is this "Filter(<set>, <numeric expr>)"
    String funName = fun.getName();
    if ( !"Filter".equalsIgnoreCase( funName ) ) {
      return null;
    }

    if ( args.length != 2 ) {
      return null;
    }

    // extract the set expression
    List<CrossJoinArg[]> allArgs = crossJoinArgFactory().checkCrossJoinArg( evaluator, args[0] );

    // checkCrossJoinArg returns a list of CrossJoinArg arrays. The first
    // array is the CrossJoin dimensions. The second array, if any,
    // contains additional constraints on the dimensions. If either the
    // list or the first array is null, then native cross join is not
    // feasible.
    if ( allArgs == null || allArgs.isEmpty() || allArgs.get( 0 ) == null ) {
      return null;
    }

    CrossJoinArg[] cjArgs = allArgs.get( 0 );
    if ( isPreferInterpreter( cjArgs, false ) ) {
      return null;
    }
    if ( !isValidContext( evaluator, cjArgs ) ) {
      return null;
    }

    // extract "order by" expression
    SchemaReader schemaReader = evaluator.getSchemaReader();
    DataSource ds = schemaReader.getDataSource();

    // generate the WHERE condition
    // Need to generate where condition here to determine whether
    // or not the filter condition can be created. The filter
    // condition could change to use an aggregate table later in evaluation
    SqlQuery sqlQuery = SqlQuery.newQuery( ds, "NativeFilter" );
    RolapNativeSql sql = new RolapNativeSql( sqlQuery, null, evaluator, cjArgs[0].getLevel() );
    final Exp filterExpr = args[1];
    String filterExprStr = sql.generateFilterCondition( filterExpr );
    if ( filterExprStr == null ) {
      return null;
    }

    // Check whether the evaluator context contains calculated members that
    // would be unsafe for native SQL generation.  This matters primarily for
    // virtual cubes, where isValidContext (above) does not check calc members.
    // For non-virtual cubes, isValidContext already rejects unsupported calc
    // members via SqlContextConstraint strict mode.
    // See hasUnsafeCalculatedMembers javadoc for the three-tier classification.
    if ( hasUnsafeCalculatedMembers( evaluator.getNonAllMembers(), cjArgs ) ) {
      return null;
    }

    final RolapStoredMeasure filterStoredMeasure =
        FilterConstraint.resolveStoredMeasureFromExpression( filterExpr );
    LOGGER.info(
        "NativeFilter carrier resolution: filterMeasure={}, sqlMeasure={}, cube={}, predicateShape={}",
        filterStoredMeasure == null ? null : filterStoredMeasure.getUniqueName(),
        sql.getStoredMeasure() == null ? null : sql.getStoredMeasure().getUniqueName(),
        evaluator.getCube() == null ? null : evaluator.getCube().getName(),
        filterExpr == null ? null : filterExpr.getClass().getSimpleName() );
    // Now construct the TupleConstraint that contains both the CJ
    // dimensions and the additional filter on them.
    CrossJoinArg[] combinedArgs = cjArgs;
    if ( allArgs.size() == 2 ) {
      CrossJoinArg[] predicateArgs = allArgs.get( 1 );
      if ( predicateArgs != null ) {
        // Combined the CJ and the additional predicate args.
        combinedArgs = Util.appendArrays( cjArgs, predicateArgs );
      }
    }

    FilterConstraint constraint =
        new FilterConstraint(
            combinedArgs,
            evaluator,
            filterExpr,
            filterStoredMeasure != null
                ? filterStoredMeasure
                : sql.getStoredMeasure() );

    if ( !constraint.isSuported( ds ) ) {
      return null;
    }

    LOGGER.debug( "using native filter" );
    return new SetEvaluator( cjArgs, schemaReader, constraint );
  }
}

// End RolapNativeFilter.java
