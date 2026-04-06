package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.MondrianDef;
import mondrian.olap.Query;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.NotPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.sql.SqlQuery;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlConstraintUtilsSubcubePredicateTest extends TestCase {

  public void testAddSubcubeConstraintAddsWhereClauseWhenPredicateExists() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mock( RolapCube.class );
    StarPredicate predicate = mock( StarPredicate.class );

    when( query.getSubcubePredicates( baseCube ) ).thenReturn( predicate );
    doAnswer( invocation -> {
      StringBuilder where = (StringBuilder) invocation.getArguments()[1];
      where.append( "brand = 'Zimniy Buket'" );
      return null;
    } ).when( predicate ).toSql( any( SqlQuery.class ), any( StringBuilder.class ) );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery ).addWhere( "brand = 'Zimniy Buket'" );
  }

  public void testAddSubcubeConstraintSkipsMissingPredicate() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mock( RolapCube.class );

    when( query.getSubcubePredicates( baseCube ) ).thenReturn( null );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery, never() ).addWhere( anyString() );
  }

  public void testAddSubcubeConstraintDropsUnavailableTupleColumnsInDimensionOnlyQuery() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mockCubeWithFactRelation( "fact" );
    MondrianDef.Relation productRelation = mockRelation( "product" );
    MondrianDef.Relation periodRelation = mockRelation( "period" );
    StarPredicate predicate = new AndPredicate( Arrays.<StarPredicate>asList(
        mockColumnPredicate( productRelation, 1, "manufacturer" ),
        mockColumnPredicate( periodRelation, 2, "quarter" ) ) );

    when( query.getSubcubePredicates( baseCube ) ).thenReturn( predicate );
    when( sqlQuery.containsRelation( eq( productRelation ) ) ).thenReturn( true );
    when( sqlQuery.containsRelation( eq( periodRelation ) ) ).thenReturn( false );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery ).addWhere( "manufacturer" );
  }

  public void testAddSubcubeConstraintAddsMissingFactReachableTables() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    MondrianDef.Relation factRelation = mockRelation( "fact" );
    RolapCube baseCube = mockCubeWithFactRelation( factRelation );
    MondrianDef.Relation productRelation = mockRelation( "product" );
    MondrianDef.Relation periodRelation = mockRelation( "period" );
    RolapStar.Table productTable = mockTable( productRelation );
    RolapStar.Table periodTable = mockTable( periodRelation );
    StarPredicate predicate = new AndPredicate( Arrays.<StarPredicate>asList(
        mockColumnPredicate( productTable, 1, "manufacturer" ),
        mockColumnPredicate( periodTable, 2, "quarter" ) ) );

    when( query.getSubcubePredicates( baseCube ) ).thenReturn( predicate );
    when( sqlQuery.containsRelation( eq( factRelation ) ) ).thenReturn( true );
    when( sqlQuery.containsRelation( eq( productRelation ) ) ).thenReturn( false );
    when( sqlQuery.containsRelation( eq( periodRelation ) ) ).thenReturn( false );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( productTable ).addToFrom( sqlQuery, false, true );
    verify( periodTable ).addToFrom( sqlQuery, false, true );
    verify( sqlQuery ).addWhere( "(manufacturer and quarter)" );
  }

  public void testAddSubcubeConstraintHandlesNotPredicate() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mockCubeWithFactRelation( "fact" );
    MondrianDef.Relation productRelation = mockRelation( "product" );

    // NOT (manufacturer = 'Mars') — simulates -{[Mfr].[Mars]} in subselect
    StarColumnPredicate inner = mockColumnPredicate( productRelation, 1, "manufacturer = 'Mars'" );
    NotPredicate notPredicate = new NotPredicate( inner );
    when( query.getSubcubePredicates( baseCube ) ).thenReturn( notPredicate );
    when( sqlQuery.containsRelation( eq( productRelation ) ) ).thenReturn( true );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery ).addWhere( "NOT (manufacturer = 'Mars')" );
  }

  public void testScopeNotPredicateDropsUnavailableColumns() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mockCubeWithFactRelation( "fact" );
    MondrianDef.Relation productRelation = mockRelation( "product" );

    // NOT predicate where the column's table is not in the SQL query
    // and fact table is also not present — should be dropped entirely
    StarColumnPredicate inner = mockColumnPredicate( productRelation, 1, "manufacturer" );
    NotPredicate notPredicate = new NotPredicate( inner );
    when( query.getSubcubePredicates( baseCube ) ).thenReturn( notPredicate );
    when( sqlQuery.containsRelation( eq( productRelation ) ) ).thenReturn( false );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery, never() ).addWhere( anyString() );
  }

  public void testScopeNotPredicateWrappingOrPredicate() {
    SqlQuery sqlQuery = mock( SqlQuery.class );
    Query query = mock( Query.class );
    RolapCube baseCube = mockCubeWithFactRelation( "fact" );
    MondrianDef.Relation productRelation = mockRelation( "product" );

    // NOT (col = 'A' OR col = 'B') — from -{[Cat].[A], [Cat].[B]}
    StarColumnPredicate predA = mockColumnPredicate( productRelation, 1, "col = 'A'" );
    StarColumnPredicate predB = mockColumnPredicate( productRelation, 1, "col = 'B'" );
    OrPredicate or = new OrPredicate( Arrays.<StarPredicate>asList( predA, predB ) );
    NotPredicate notPredicate = new NotPredicate( or );
    when( query.getSubcubePredicates( baseCube ) ).thenReturn( notPredicate );
    when( sqlQuery.containsRelation( eq( productRelation ) ) ).thenReturn( true );

    SqlConstraintUtils.addSubcubeConstraint( sqlQuery, query, baseCube );

    verify( sqlQuery ).addWhere( "NOT ((col = 'A' or col = 'B'))" );
  }

  private static RolapCube mockCubeWithFactRelation( String alias ) {
    return mockCubeWithFactRelation( mockRelation( alias ) );
  }

  private static RolapCube mockCubeWithFactRelation( MondrianDef.Relation factRelation ) {
    RolapCube baseCube = mock( RolapCube.class );
    RolapStar star = mock( RolapStar.class );
    RolapStar.Table factTable = mock( RolapStar.Table.class );
    when( baseCube.getStar() ).thenReturn( star );
    when( star.getFactTable() ).thenReturn( factTable );
    when( factTable.getRelation() ).thenReturn( factRelation );
    return baseCube;
  }

  private static MondrianDef.Relation mockRelation( String alias ) {
    MondrianDef.Relation relation = mock( MondrianDef.Relation.class );
    when( relation.getAlias() ).thenReturn( alias );
    return relation;
  }

  private static RolapStar.Table mockTable( MondrianDef.Relation relation ) {
    RolapStar.Table table = mock( RolapStar.Table.class );
    when( table.getRelation() ).thenReturn( relation );
    return table;
  }

  private static StarColumnPredicate mockColumnPredicate(
      MondrianDef.Relation relation,
      int bitPosition,
      String sql )
  {
    return mockColumnPredicate( mockTable( relation ), bitPosition, sql );
  }

  private static StarColumnPredicate mockColumnPredicate(
      RolapStar.Table table,
      int bitPosition,
      String sql )
  {
    RolapStar.Column column = mock( RolapStar.Column.class );
    when( column.getBitPosition() ).thenReturn( bitPosition );
    when( column.getTable() ).thenReturn( table );

    BitKey bitKey = BitKey.Factory.makeBitKey( 8 );
    bitKey.set( bitPosition );

    StarColumnPredicate predicate = mock( StarColumnPredicate.class );
    when( predicate.getConstrainedColumn() ).thenReturn( column );
    when( predicate.getConstrainedColumnList() )
        .thenReturn( Collections.singletonList( column ) );
    when( predicate.getConstrainedColumnBitKey() ).thenReturn( bitKey );
    doAnswer( invocation -> {
      StringBuilder where = (StringBuilder) invocation.getArguments()[1];
      where.append( sql );
      return null;
    } ).when( predicate ).toSql( any( SqlQuery.class ), any( StringBuilder.class ) );
    return predicate;
  }
}
