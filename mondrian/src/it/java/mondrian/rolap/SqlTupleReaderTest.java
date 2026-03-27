/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.Evaluator;
import mondrian.olap.MondrianDef;
import mondrian.olap.Query;
import mondrian.recorder.MessageRecorder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import org.mockito.Answers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.eq;

/**
 * Created by Dmitriy Stepanov on 20.01.18.
 */
public class SqlTupleReaderTest extends TestCase {


  public void testAddLevelMemberSql() throws Exception {
    TupleConstraint constraint = mock( TupleConstraint.class );
    SqlQuery sqlQuery = mock( SqlQuery.class, Answers.RETURNS_MOCKS.get() );
    RolapCube baseCube = mock( RolapCube.class );
    RolapLevel targetLevel = mock( RolapLevel.class );
    RolapCubeLevel levelIter = mock( RolapCubeLevel.class, Answers.RETURNS_MOCKS.get() );
    RolapProperty rolapProperty = mock( RolapProperty.class, Answers.RETURNS_MOCKS.get() );
    String propertyName = "property_1";
    when( rolapProperty.getName() ).thenReturn( propertyName );
    RolapProperty[] properties = { rolapProperty };
    when( levelIter.getProperties() ).thenReturn( properties );
    when( levelIter.getKeyExp() ).thenReturn( mock( MondrianDef.Expression.class ) );
    when( levelIter.getOrdinalExp() ).thenReturn( mock( MondrianDef.Expression.class ) );
    when( levelIter.getParentExp() ).thenReturn( null );
    RolapHierarchy hierarchy = mock( RolapHierarchy.class, Answers.RETURNS_MOCKS.get() );
    when( targetLevel.getHierarchy() ).thenReturn( hierarchy );
    when( hierarchy.getLevels() ).thenReturn( new RolapLevel[] { levelIter } );
    SqlTupleReader.WhichSelect whichSelect = SqlTupleReader.WhichSelect.LAST;
    JdbcSchema.Table dbTable = mock( JdbcSchema.Table.class, Answers.RETURNS_MOCKS.get() );
    when( dbTable.getColumnUsages( any() ) ).thenReturn( mock( Iterator.class ) );
    RolapStar star = mock( RolapStar.class );
    when( star.getColumnCount() ).thenReturn( 1 );
    AggStar aggStar = spy( AggStar.makeAggStar( star, dbTable, mock( MessageRecorder.class ), 10 ) );
    AggStar.Table.Column column = mock( AggStar.Table.Column.class, Answers.RETURNS_MOCKS.get() );
    doReturn( column ).when( aggStar ).lookupColumn( 0 );
    RolapStar.Column starColumn = mock( RolapStar.Column.class, Answers.RETURNS_MOCKS.get() );
    when( starColumn.getBitPosition() ).thenReturn( 0 );
    doReturn( starColumn ).when( levelIter ).getStarKeyColumn();
    AggStar.FactTable factTable =
      (AggStar.FactTable) createInstance( "mondrian.rolap.aggmatcher.AggStar$FactTable",
        new Class[] { mondrian.rolap.aggmatcher.AggStar.class, JdbcSchema.Table.class },
        new Object[] { aggStar, dbTable }, AggStar.FactTable.class.getClassLoader() );
    factTable = spy( factTable );
    Map<String, MondrianDef.Expression> propertiesAgg = new HashMap<>();
    propertiesAgg.put( propertyName, mock( MondrianDef.Expression.class ) );
    Class[] constructorArgsClasses =
      { mondrian.rolap.aggmatcher.AggStar.Table.class, String.class, MondrianDef.Expression.class, int.class,
        RolapStar.Column.class, boolean.class,
        MondrianDef.Expression.class, MondrianDef.Expression.class, Map.class };
    Object[] constructorArgs =
      { factTable, "name", mock( MondrianDef.Expression.class ), 0, starColumn, true,
        mock( MondrianDef.Expression.class ), null,
        propertiesAgg };
    AggStar.Table.Level aggStarLevel =
      (AggStar.Table.Level) createInstance( "mondrian.rolap.aggmatcher.AggStar$Table$Level", constructorArgsClasses,
        constructorArgs, AggStar.Table.Level.class.getClassLoader() );
    when( aggStar.lookupLevel( 0 ) ).thenReturn( aggStarLevel );
    doReturn( factTable ).when( column ).getTable();
    SqlTupleReader reader = new SqlTupleReader( constraint );
    reader.addLevelMemberSql( sqlQuery, targetLevel, baseCube, whichSelect, aggStar );
    verify( factTable ).addToFrom( any(), eq( false ), eq( true ) );
  }

  public void testResolveMemberProjectionHierarchyKeepsQueryHierarchyForVirtualCube() throws Exception {
    TupleConstraint constraint = mock( TupleConstraint.class );
    Evaluator evaluator = mock( Evaluator.class );
    Query query = mock( Query.class );
    RolapCube queryCube = mock( RolapCube.class );
    when( constraint.getEvaluator() ).thenReturn( evaluator );
    when( evaluator.getQuery() ).thenReturn( query );
    when( query.getCube() ).thenReturn( queryCube );
    when( queryCube.isVirtual() ).thenReturn( true );

    RolapCube baseCube = mock( RolapCube.class );
    RolapCubeHierarchy targetHierarchy =
      mock( RolapCubeHierarchy.class, Answers.RETURNS_MOCKS.get() );
    RolapHierarchy baseHierarchy = mock( RolapHierarchy.class );
    RolapLevel targetLevel = mock( RolapLevel.class );

    when( targetLevel.isAll() ).thenReturn( false );
    when( targetLevel.getHierarchy() ).thenReturn( targetHierarchy );
    when( targetHierarchy.getCube() ).thenReturn( queryCube );
    when( baseCube.findBaseCubeHierarchy( targetHierarchy ) )
      .thenReturn( baseHierarchy );

    SqlTupleReader reader = new SqlTupleReader( constraint );
    Method method =
      SqlTupleReader.class.getDeclaredMethod(
        "resolveMemberProjectionHierarchy",
        RolapLevel.class,
        RolapCube.class );
    method.setAccessible( true );

    RolapHierarchy resolved =
      (RolapHierarchy) method.invoke( reader, targetLevel, baseCube );

    assertSame( targetHierarchy, resolved );
  }

  public void testResolveTargetHierarchyUsesBaseCubeHierarchyForRegularCube() throws Exception {
    TupleConstraint constraint = mock( TupleConstraint.class );
    Evaluator evaluator = mock( Evaluator.class );
    Query query = mock( Query.class );
    RolapCube queryCube = mock( RolapCube.class );
    when( constraint.getEvaluator() ).thenReturn( evaluator );
    when( evaluator.getQuery() ).thenReturn( query );
    when( query.getCube() ).thenReturn( queryCube );
    when( queryCube.isVirtual() ).thenReturn( false );

    RolapCube baseCube = mock( RolapCube.class );
    RolapCubeHierarchy targetHierarchy =
      mock( RolapCubeHierarchy.class, Answers.RETURNS_MOCKS.get() );
    RolapHierarchy baseHierarchy = mock( RolapHierarchy.class );
    RolapLevel targetLevel = mock( RolapLevel.class );

    when( targetLevel.isAll() ).thenReturn( false );
    when( targetLevel.getHierarchy() ).thenReturn( targetHierarchy );
    when( targetHierarchy.getCube() ).thenReturn( queryCube );
    when( baseCube.findBaseCubeHierarchy( targetHierarchy ) )
      .thenReturn( baseHierarchy );

    SqlTupleReader reader = new SqlTupleReader( constraint );
    Method method =
      SqlTupleReader.class.getDeclaredMethod(
        "resolveTargetHierarchy",
        RolapLevel.class,
        RolapCube.class );
    method.setAccessible( true );

    RolapHierarchy resolved =
      (RolapHierarchy) method.invoke( reader, targetLevel, baseCube );

    assertSame( baseHierarchy, resolved );
  }

  private Object createInstance( String className, Class[] constructorArgsClasses, Object[] constructorArgs,
                                 ClassLoader classLoader )

    throws Exception {
    Class cl = classLoader.loadClass( className );
    Constructor constructor = cl.getDeclaredConstructor( constructorArgsClasses );
    constructor.setAccessible( true );
    return constructor.newInstance( constructorArgs );
  }


}
