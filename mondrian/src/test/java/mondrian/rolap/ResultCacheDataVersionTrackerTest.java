/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ResultCacheDataVersionTrackerTest extends TestCase {

  public void testCurrentTokenDisabledWhenSqlIsNotConfigured() {
    final ResultCacheDataVersionTracker tracker =
      new ResultCacheDataVersionTracker();
    final RolapConnection connection = mock( RolapConnection.class );
    final DataSource dataSource = mock( DataSource.class );
    final Util.PropertyList connectInfo = new Util.PropertyList();
    when( connection.getConnectInfo() ).thenReturn( connectInfo );
    when( connection.getDataSource() ).thenReturn( dataSource );

    final long token =
      tracker.currentToken( connection, mock( ResultReuseCache.class ), 1_000L );

    assertEquals( 0L, token );
    verifyZeroInteractions( dataSource );
  }

  public void testCurrentTokenBumpsAndClearsCacheOnVersionChange()
    throws Exception {
    final ResultCacheDataVersionTracker tracker =
      new ResultCacheDataVersionTracker();
    final ResultReuseCache cache = mock( ResultReuseCache.class );
    final RolapConnection connection = mock( RolapConnection.class );
    final DataSource dataSource = mock( DataSource.class );
    final Connection jdbcConnection = mock( Connection.class );
    final PreparedStatement statement = mock( PreparedStatement.class );
    final ResultSet resultSet = mock( ResultSet.class );

    final Util.PropertyList connectInfo = new Util.PropertyList();
    connectInfo.put( ResultCacheDataVersionTracker.PROP_SQL, "select version" );
    connectInfo.put( ResultCacheDataVersionTracker.PROP_POLL_MS, "500" );
    connectInfo.put( ResultCacheDataVersionTracker.PROP_TIMEOUT_SEC, "2" );
    connectInfo.put( RolapConnectionProperties.Jdbc.name(), "jdbc:clickhouse://example/test" );
    connectInfo.put( RolapConnectionProperties.JdbcUser.name(), "user1" );

    when( connection.getConnectInfo() ).thenReturn( connectInfo );
    when( connection.getDataSource() ).thenReturn( dataSource );
    when( dataSource.getConnection() ).thenReturn( jdbcConnection );
    when( jdbcConnection.prepareStatement( "select version" ) )
      .thenReturn( statement );
    when( statement.executeQuery() ).thenReturn( resultSet );
    when( resultSet.next() ).thenReturn( true, true, true );
    when( resultSet.getObject( 1 ) ).thenReturn( "v1", "v1", "v2" );

    final long token0 = tracker.currentToken( connection, cache, 0L );
    final long token1 = tracker.currentToken( connection, cache, 1_000L );
    final long token2 = tracker.currentToken( connection, cache, 2_000L );

    assertEquals( 0L, token0 );
    assertEquals( 0L, token1 );
    assertEquals( 1L, token2 );
    verify( cache, times( 1 ) ).clear();
  }

  public void testComposeCacheVersion() {
    final long v11 =
      ResultCacheDataVersionTracker.composeCacheVersion( 1, 1 );
    final long v21 =
      ResultCacheDataVersionTracker.composeCacheVersion( 2, 1 );
    final long v12 =
      ResultCacheDataVersionTracker.composeCacheVersion( 1, 2 );

    assertTrue( v11 != v21 );
    assertTrue( v11 != v12 );
  }

  public void testParseBoundedInt() {
    assertEquals(
      5_000,
      ResultCacheDataVersionTracker.parseBoundedInt(
        null,
        5_000,
        100,
        10_000 ) );
    assertEquals(
      100,
      ResultCacheDataVersionTracker.parseBoundedInt(
        "1",
        5_000,
        100,
        10_000 ) );
    assertEquals(
      10_000,
      ResultCacheDataVersionTracker.parseBoundedInt(
        "50000",
        5_000,
        100,
        10_000 ) );
    assertEquals(
      5_000,
      ResultCacheDataVersionTracker.parseBoundedInt(
        "bad-value",
        5_000,
        100,
        10_000 ) );
  }
}

