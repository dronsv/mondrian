/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SqlTupleReaderNullStarKeyColumnTest {

  @Test public void testDetectsMixedAggregateAndBaseFactPlan() {
    TupleConstraint constraint = mock( TupleConstraint.class );
    SqlTupleReader reader = new SqlTupleReader( constraint );
    SqlQuery sqlQuery = mock( SqlQuery.class );

    MondrianDef.Relation aggRelation = mock( MondrianDef.Relation.class );
    MondrianDef.Relation baseFactRelation =
      mock( MondrianDef.Relation.class );
    RolapCube baseCube = mockBaseCubeWithFactRelation( baseFactRelation );
    AggStar aggStar = mockAggStarWithFactRelation( aggRelation );

    when( sqlQuery.containsRelation( aggRelation ) ).thenReturn( true );
    when( sqlQuery.containsRelation( baseFactRelation ) ).thenReturn( true );

    assertTrue(
      reader.mixesAggregateAndBaseFact( sqlQuery, baseCube, aggStar ) );
  }

  @Test public void testDoesNotDetectAggregateOnlyPlanAsMixedFactPlan() {
    TupleConstraint constraint = mock( TupleConstraint.class );
    SqlTupleReader reader = new SqlTupleReader( constraint );
    SqlQuery sqlQuery = mock( SqlQuery.class );

    MondrianDef.Relation aggRelation = mock( MondrianDef.Relation.class );
    MondrianDef.Relation baseFactRelation =
      mock( MondrianDef.Relation.class );
    RolapCube baseCube = mockBaseCubeWithFactRelation( baseFactRelation );
    AggStar aggStar = mockAggStarWithFactRelation( aggRelation );

    when( sqlQuery.containsRelation( aggRelation ) ).thenReturn( true );
    when( sqlQuery.containsRelation( baseFactRelation ) ).thenReturn( false );

    assertFalse(
      reader.mixesAggregateAndBaseFact( sqlQuery, baseCube, aggStar ) );
  }

  @Test public void testAddLevelMemberSqlSkipsCollapsedAggPathWhenStarKeyColumnMissing() {
    TupleConstraint constraint = mock( TupleConstraint.class );
    SqlQuery sqlQuery = mock( SqlQuery.class, Answers.RETURNS_MOCKS );
    RolapCube baseCube = mock( RolapCube.class );
    RolapLevel targetLevel = mock( RolapLevel.class );
    RolapCubeLevel levelIter =
      mock( RolapCubeLevel.class, Answers.RETURNS_MOCKS );

    when( levelIter.getProperties() ).thenReturn( new RolapProperty[ 0 ] );
    when( levelIter.getKeyExp() )
      .thenReturn( mock( MondrianDef.Expression.class ) );
    when( levelIter.getOrdinalExp() )
      .thenReturn( mock( MondrianDef.Expression.class ) );
    when( levelIter.getParentExp() ).thenReturn( null );
    doReturn( null ).when( levelIter ).getStarKeyColumn();

    RolapHierarchy hierarchy =
      mock( RolapHierarchy.class, Answers.RETURNS_MOCKS );
    when( targetLevel.getHierarchy() ).thenReturn( hierarchy );
    when( targetLevel.getDepth() ).thenReturn( 0 );
    when( hierarchy.getLevels() ).thenReturn( new RolapLevel[] { levelIter } );

    JdbcSchema.Table dbTable =
      mock( JdbcSchema.Table.class, Answers.RETURNS_MOCKS );
    when( dbTable.getColumnUsages( any() ) ).thenReturn( mock( Iterator.class ) );
    RolapStar star = mock( RolapStar.class );
    when( star.getColumnCount() ).thenReturn( 0 );
    AggStar aggStar =
      spy( AggStar.makeAggStar( star, dbTable, mock( MessageRecorder.class ), 10 ) );

    SqlTupleReader reader = new SqlTupleReader( constraint );
    reader.addLevelMemberSql(
      sqlQuery,
      targetLevel,
      baseCube,
      SqlTupleReader.WhichSelect.LAST,
      aggStar );
  }

  private RolapCube mockBaseCubeWithFactRelation(
    MondrianDef.Relation relation )
  {
    RolapCube cube = mock( RolapCube.class );
    RolapStar star = mock( RolapStar.class );
    RolapStar.Table factTable = mock( RolapStar.Table.class );
    when( cube.getStar() ).thenReturn( star );
    when( star.getFactTable() ).thenReturn( factTable );
    when( factTable.getRelation() ).thenReturn( relation );
    return cube;
  }

  private AggStar mockAggStarWithFactRelation(
    MondrianDef.Relation relation )
  {
    AggStar aggStar = mock( AggStar.class );
    AggStar.FactTable factTable = mock( AggStar.FactTable.class );
    when( aggStar.getFactTable() ).thenReturn( factTable );
    when( factTable.getRelation() ).thenReturn( relation );
    return aggStar;
  }
}
