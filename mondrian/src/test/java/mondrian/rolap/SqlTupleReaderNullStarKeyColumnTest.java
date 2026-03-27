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

import junit.framework.TestCase;
import mondrian.olap.MondrianDef;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import org.mockito.Answers;

import java.util.Iterator;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SqlTupleReaderNullStarKeyColumnTest extends TestCase {

  public void testAddLevelMemberSqlSkipsCollapsedAggPathWhenStarKeyColumnMissing() {
    TupleConstraint constraint = mock( TupleConstraint.class );
    SqlQuery sqlQuery = mock( SqlQuery.class, Answers.RETURNS_MOCKS.get() );
    RolapCube baseCube = mock( RolapCube.class );
    RolapLevel targetLevel = mock( RolapLevel.class );
    RolapCubeLevel levelIter =
      mock( RolapCubeLevel.class, Answers.RETURNS_MOCKS.get() );

    when( levelIter.getProperties() ).thenReturn( new RolapProperty[ 0 ] );
    when( levelIter.getKeyExp() )
      .thenReturn( mock( MondrianDef.Expression.class ) );
    when( levelIter.getOrdinalExp() )
      .thenReturn( mock( MondrianDef.Expression.class ) );
    when( levelIter.getParentExp() ).thenReturn( null );
    doReturn( null ).when( levelIter ).getStarKeyColumn();

    RolapHierarchy hierarchy =
      mock( RolapHierarchy.class, Answers.RETURNS_MOCKS.get() );
    when( targetLevel.getHierarchy() ).thenReturn( hierarchy );
    when( targetLevel.getDepth() ).thenReturn( 0 );
    when( hierarchy.getLevels() ).thenReturn( new RolapLevel[] { levelIter } );

    JdbcSchema.Table dbTable =
      mock( JdbcSchema.Table.class, Answers.RETURNS_MOCKS.get() );
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
}
