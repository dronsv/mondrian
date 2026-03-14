/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.*;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultReuseCacheProjectionTest extends TestCase {

  public void testTryBuildMeasureProjectionMapSupportsSubset() {
    final Query sourceQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]", "[Measures].[C]" ) );
    final Query targetQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[C]" ) );

    final int[] map =
      ResultReuseCache.tryBuildMeasureProjectionMap(
        sourceQuery,
        targetQuery );

    assertNotNull( map );
    assertEquals( 2, map.length );
    assertEquals( 0, map[0] );
    assertEquals( 2, map[1] );
  }

  public void testTryBuildMeasureProjectionMapRejectsDifferentShape() {
    final Query sourceQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]" ) );
    final Query targetQuery =
      query(
        true,
        measureSet( "[Measures].[A]" ) );

    final int[] map =
      ResultReuseCache.tryBuildMeasureProjectionMap(
        sourceQuery,
        targetQuery );

    assertNull( map );
  }

  public void testAcquireProjectedResultMapsColumnCoordinates() {
    final Query sourceQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]", "[Measures].[C]" ) );
    final Query targetQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[C]" ) );
    final TrackingResult sourceResult =
      new TrackingResult( sourceQuery, 3, 2 );
    final ResultReuseCache cache = new ResultReuseCache();

    Result ownerHandle =
      cache.putAndAcquire(
        "source",
        sourceResult,
        1L,
        1_000L,
        32,
        120_000 );
    ownerHandle.close();

    final ResultReuseCache.ProjectionAcquireResult acquire =
      cache.tryAcquireMeasureProjectionWithReason(
        "target",
        targetQuery,
        1L,
        1_000L,
        32,
        120_000 );
    final Result projected = acquire.result();

    assertNotNull( projected );
    assertEquals( "source", acquire.sourceKey() );
    assertTrue( acquire.detail().contains( "subsetCandidateEntries=1" ) );
    assertTrue( acquire.detail().contains( "bestExtraMeasures=1" ) );
    assertEquals( 2, projected.getAxes()[0].getPositions().size() );

    projected.getCell( new int[] { 1, 0 } );
    assertNotNull( sourceResult.lastPos );
    assertEquals( 2, sourceResult.lastPos[0] );
    assertEquals( 0, sourceResult.lastPos[1] );
    projected.close();
  }

  public void testProjectionMissReasonNoOtherEntries() {
    final Query targetQuery =
      query(
        false,
        measureSet( "[Measures].[A]" ) );
    final ResultReuseCache cache = new ResultReuseCache();

    final ResultReuseCache.ProjectionAcquireResult acquire =
      cache.tryAcquireMeasureProjectionWithReason(
        "target",
        targetQuery,
        1L,
        1_000L,
        32,
        120_000 );

    assertNull( acquire.result() );
    assertEquals(
      ResultReuseCache.PROJECTION_MISS_NO_OTHER_ENTRIES,
      acquire.missReason() );
    assertTrue( acquire.detail().contains( "examinedEntries=0" ) );
  }

  public void testProjectionMissReasonTargetIneligible() {
    final Query targetQuery =
      query(
        false,
        new UnresolvedFunCall(
          "CrossJoin",
          Syntax.Function,
          new Exp[] {
            measureSet( "[Measures].[A]" ),
            measureSet( "[Measures].[B]" )
          } ) );
    final ResultReuseCache cache = new ResultReuseCache();

    final ResultReuseCache.ProjectionAcquireResult acquire =
      cache.tryAcquireMeasureProjectionWithReason(
        "target",
        targetQuery,
        1L,
        1_000L,
        32,
        120_000 );

    assertNull( acquire.result() );
    assertEquals(
      ResultReuseCache.PROJECTION_MISS_TARGET_INELIGIBLE,
      acquire.missReason() );
    assertTrue( acquire.detail().contains( "targetMeasures=-1" ) );
  }

  public void testProjectionMissReasonShapeMismatch() {
    final Query sourceQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]" ) );
    final Query targetQuery =
      query(
        true,
        measureSet( "[Measures].[A]" ) );
    final TrackingResult sourceResult =
      new TrackingResult( sourceQuery, 2, 2 );
    final ResultReuseCache cache = new ResultReuseCache();

    Result ownerHandle =
      cache.putAndAcquire(
        "source",
        sourceResult,
        1L,
        1_000L,
        32,
        120_000 );
    ownerHandle.close();

    final ResultReuseCache.ProjectionAcquireResult acquire =
      cache.tryAcquireMeasureProjectionWithReason(
        "target",
        targetQuery,
        1L,
        1_000L,
        32,
        120_000 );

    assertNull( acquire.result() );
    assertEquals(
      ResultReuseCache.PROJECTION_MISS_SHAPE_MISMATCH,
      acquire.missReason() );
    assertTrue( acquire.detail().contains( "shapeMatchedEntries=0" ) );
  }

  public void testProjectionPrefersMostRecentSupersetWhenWidthTies() {
    final Query sourceQueryOne =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]", "[Measures].[C]" ) );
    final Query sourceQueryTwo =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[C]", "[Measures].[D]" ) );
    final Query targetQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[C]" ) );
    final ResultReuseCache cache = new ResultReuseCache();

    Result ownerHandle =
      cache.putAndAcquire(
        "source-1",
        new TrackingResult( sourceQueryOne, 3, 2 ),
        1L,
        1_000L,
        32,
        120_000 );
    ownerHandle.close();
    ownerHandle =
      cache.putAndAcquire(
        "source-2",
        new TrackingResult( sourceQueryTwo, 3, 2 ),
        1L,
        1_001L,
        32,
        120_000 );
    ownerHandle.close();

    final ResultReuseCache.ProjectionAcquireResult acquire =
      cache.tryAcquireMeasureProjectionWithReason(
        "target",
        targetQuery,
        1L,
        1_002L,
        32,
        120_000 );

    assertNotNull( acquire.result() );
    assertEquals( "source-2", acquire.sourceKey() );
    assertTrue( acquire.detail().contains( "subsetCandidateEntries=2" ) );
    acquire.result().close();
  }

  public void testProjectionMissReasonNotMeasureSubset() {
    final Query sourceQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]" ) );
    final Query targetQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[C]" ) );
    final TrackingResult sourceResult =
      new TrackingResult( sourceQuery, 2, 2 );
    final ResultReuseCache cache = new ResultReuseCache();

    Result ownerHandle =
      cache.putAndAcquire(
        "source",
        sourceResult,
        1L,
        1_000L,
        32,
        120_000 );
    ownerHandle.close();

    final ResultReuseCache.ProjectionAcquireResult acquire =
      cache.tryAcquireMeasureProjectionWithReason(
        "target",
        targetQuery,
        1L,
        1_000L,
        32,
        120_000 );

    assertNull( acquire.result() );
    assertEquals(
      ResultReuseCache.PROJECTION_MISS_NOT_MEASURE_SUBSET,
      acquire.missReason() );
    assertTrue( acquire.detail().contains( "subsetCandidateEntries=0" ) );
  }

  public void testBuildExactQuerySignatureIgnoresFallbackWhitespaceWhenQueryPresent() {
    final Query query =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]" ) );

    final String left =
      ResultReuseCache.buildExactQuerySignature(
        query,
        "select {[Measures].[A], [Measures].[B]} on columns from [Sales]" );
    final String right =
      ResultReuseCache.buildExactQuerySignature(
        query,
        "  SELECT   { [Measures].[A] , [Measures].[B] }  ON COLUMNS FROM [Sales]  " );

    assertEquals( left, right );
  }

  public void testBuildExactQuerySignatureFallbackNormalizesWhitespace() {
    final String left =
      ResultReuseCache.buildExactQuerySignature(
        null,
        "select {[Measures].[A]} on columns from [Sales]" );
    final String right =
      ResultReuseCache.buildExactQuerySignature(
        null,
        "  select   {[Measures].[A]}   on columns   from   [Sales]  " );

    assertEquals( left, right );
  }

  public void testBuildExactQuerySignatureChangesWithAxisShape() {
    final Query sourceQuery =
      query(
        false,
        measureSet( "[Measures].[A]", "[Measures].[B]" ) );
    final Query targetQuery =
      query(
        true,
        measureSet( "[Measures].[A]", "[Measures].[B]" ) );

    final String sourceSignature =
      ResultReuseCache.buildExactQuerySignature( sourceQuery, "ignored" );
    final String targetSignature =
      ResultReuseCache.buildExactQuerySignature( targetQuery, "ignored" );

    assertFalse( sourceSignature.equals( targetSignature ) );
  }

  private Query query( boolean nonEmptyColumns, Exp measureSet ) {
    final Exp rowsSet = tupleSet( "[Store].[All Stores]" );

    final QueryAxis columnsAxis = mock( QueryAxis.class );
    when( columnsAxis.getSet() ).thenReturn( measureSet );
    when( columnsAxis.isNonEmpty() ).thenReturn( nonEmptyColumns );
    when( columnsAxis.isOrdered() ).thenReturn( false );
    when( columnsAxis.getDimensionProperties() ).thenReturn( new Id[0] );

    final QueryAxis rowsAxis = mock( QueryAxis.class );
    when( rowsAxis.isNonEmpty() ).thenReturn( true );
    when( rowsAxis.isOrdered() ).thenReturn( false );
    when( rowsAxis.getDimensionProperties() ).thenReturn( new Id[0] );
    when( rowsAxis.getSet() ).thenReturn( rowsSet );

    final Query query = mock( Query.class );
    final Cube cube = mock( Cube.class );
    when( cube.getName() ).thenReturn( "Sales" );
    when( query.getCube() ).thenReturn( cube );
    when( query.getAxes() ).thenReturn( new QueryAxis[] { columnsAxis, rowsAxis } );
    when( query.getSlicerAxis() ).thenReturn( null );
    when( query.getFormulas() ).thenReturn( new Formula[0] );
    when( query.getCellProperties() ).thenReturn( new QueryPart[0] );
    return query;
  }

  private Exp measureSet( String... uniqueNames ) {
    final Exp[] args = new Exp[uniqueNames.length];
    for ( int i = 0; i < uniqueNames.length; i++ ) {
      final Member member = mock( Member.class );
      when( member.isMeasure() ).thenReturn( true );
      when( member.getUniqueName() ).thenReturn( uniqueNames[i] );
      args[i] = new MemberExpr( member );
    }
    return new UnresolvedFunCall( "{}", Syntax.Braces, args );
  }

  private Exp tupleSet( String... uniqueNames ) {
    final Exp[] args = new Exp[uniqueNames.length];
    for ( int i = 0; i < uniqueNames.length; i++ ) {
      final Member member = mock( Member.class );
      when( member.isMeasure() ).thenReturn( false );
      when( member.getUniqueName() ).thenReturn( uniqueNames[i] );
      args[i] = new MemberExpr( member );
    }
    return new UnresolvedFunCall( "{}", Syntax.Braces, args );
  }

  private static final class TrackingResult implements Result {
    private final Query query;
    private final Axis[] axes;
    private int[] lastPos;

    private TrackingResult( Query query, int columnCount, int rowCount ) {
      this.query = query;
      this.axes = new Axis[] {
        axisWithPositions( columnCount ),
        axisWithPositions( rowCount )
      };
    }

    public Query getQuery() {
      return query;
    }

    public Axis[] getAxes() {
      return axes;
    }

    public Axis getSlicerAxis() {
      return axisWithPositions( 0 );
    }

    public Cell getCell( int[] pos ) {
      this.lastPos = pos == null ? null : pos.clone();
      final Cell cell = mock( Cell.class );
      when( cell.getValue() ).thenReturn( String.valueOf( this.lastPos ) );
      return cell;
    }

    public void print( PrintWriter pw ) {
      // no-op
    }

    public void close() {
      // no-op
    }
  }

  private static Axis axisWithPositions( int count ) {
    final List<Position> positions = new ArrayList<Position>( count );
    for ( int i = 0; i < count; i++ ) {
      positions.add( new SimplePosition() );
    }
    return new Axis() {
      public List<Position> getPositions() {
        return Collections.unmodifiableList( positions );
      }
    };
  }

  private static final class SimplePosition
    extends ArrayList<Member>
    implements Position {
  }
}
