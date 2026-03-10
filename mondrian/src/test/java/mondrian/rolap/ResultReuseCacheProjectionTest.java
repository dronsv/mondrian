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

    final Result projected =
      cache.tryAcquireMeasureProjection(
        "target",
        targetQuery,
        1L,
        1_000L,
        32,
        120_000 );

    assertNotNull( projected );
    assertEquals( 2, projected.getAxes()[0].getPositions().size() );

    projected.getCell( new int[] { 1, 0 } );
    assertNotNull( sourceResult.lastPos );
    assertEquals( 2, sourceResult.lastPos[0] );
    assertEquals( 0, sourceResult.lastPos[1] );
    projected.close();
  }

  private Query query( boolean nonEmptyColumns, Exp measureSet ) {
    final QueryAxis columnsAxis = mock( QueryAxis.class );
    when( columnsAxis.getSet() ).thenReturn( measureSet );
    when( columnsAxis.isNonEmpty() ).thenReturn( nonEmptyColumns );
    when( columnsAxis.isOrdered() ).thenReturn( false );
    when( columnsAxis.getDimensionProperties() ).thenReturn( new Id[0] );

    final QueryAxis rowsAxis = mock( QueryAxis.class );
    when( rowsAxis.isNonEmpty() ).thenReturn( true );
    when( rowsAxis.isOrdered() ).thenReturn( false );
    when( rowsAxis.getDimensionProperties() ).thenReturn( new Id[0] );

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
