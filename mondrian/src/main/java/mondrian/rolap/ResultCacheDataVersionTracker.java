/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

/**
 * Tracks an external data-version signal and invalidates the shared
 * server-side result cache when the signal changes.
 *
 * <p>The version is read from a SQL query configured in connection properties:
 * {@code ResultCacheVersionSql}. If not set, this tracker is disabled.</p>
 */
class ResultCacheDataVersionTracker {
  private static final Logger LOGGER =
    LogManager.getLogger( ResultCacheDataVersionTracker.class );

  static final String PROP_SQL = "ResultCacheVersionSql";
  static final String PROP_POLL_MS = "ResultCacheVersionPollMs";
  static final String PROP_TIMEOUT_SEC = "ResultCacheVersionQueryTimeoutSec";

  private static final int DEFAULT_POLL_MS = 5000;
  private static final int DEFAULT_TIMEOUT_SEC = 2;
  private static final int MIN_POLL_MS = 250;
  private static final int MAX_POLL_MS = 3_600_000;
  private static final int MIN_TIMEOUT_SEC = 1;
  private static final int MAX_TIMEOUT_SEC = 60;
  private static final long ERROR_LOG_INTERVAL_MS = 60_000L;

  private final ConcurrentMap<String, VersionState> states =
    new ConcurrentHashMap<String, VersionState>();

  long currentToken( RolapConnection connection, ResultReuseCache cache ) {
    return currentToken( connection, cache, System.currentTimeMillis() );
  }

  long currentToken(
    RolapConnection connection,
    ResultReuseCache cache,
    long nowMillis ) {
    final Util.PropertyList connectInfo = connection.getConnectInfo();
    final String sql = trimToNull( connectInfo.get( PROP_SQL ) );
    if ( sql == null ) {
      return 0L;
    }
    final int pollMs =
      parseBoundedInt(
        connectInfo.get( PROP_POLL_MS ),
        DEFAULT_POLL_MS,
        MIN_POLL_MS,
        MAX_POLL_MS );
    final int timeoutSec =
      parseBoundedInt(
        connectInfo.get( PROP_TIMEOUT_SEC ),
        DEFAULT_TIMEOUT_SEC,
        MIN_TIMEOUT_SEC,
        MAX_TIMEOUT_SEC );
    final String stateKey = buildStateKey( connectInfo, sql );
    final VersionState state = getOrCreateState( stateKey );
    return state.currentToken(
      connection.getDataSource(),
      sql,
      pollMs,
      timeoutSec,
      nowMillis,
      cache,
      stateKey );
  }

  private VersionState getOrCreateState( String stateKey ) {
    VersionState state = states.get( stateKey );
    if ( state != null ) {
      return state;
    }
    final VersionState newState = new VersionState();
    final VersionState existing = states.putIfAbsent( stateKey, newState );
    return existing == null ? newState : existing;
  }

  private static String buildStateKey(
    Util.PropertyList connectInfo,
    String sql ) {
    final String jdbcConnectionUuid =
      trimToNull( connectInfo.get( RolapConnectionProperties.JdbcConnectionUuid.name() ) );
    final String dataSourceName =
      trimToNull( connectInfo.get( RolapConnectionProperties.DataSource.name() ) );
    final String jdbcUrl =
      trimToNull( connectInfo.get( RolapConnectionProperties.Jdbc.name() ) );
    final String jdbcUser =
      trimToNull( connectInfo.get( RolapConnectionProperties.JdbcUser.name() ) );
    final StringBuilder key = new StringBuilder( 256 );
    if ( jdbcConnectionUuid != null ) {
      key.append( "uuid:" ).append( jdbcConnectionUuid );
    } else if ( dataSourceName != null ) {
      key.append( "ds:" ).append( dataSourceName );
    } else {
      key
        .append( "jdbc:" ).append( nullSafe( jdbcUrl ) )
        .append( "|user:" ).append( nullSafe( jdbcUser ) );
    }
    key.append( "|sql:" ).append( sql );
    return key.toString();
  }

  static long composeCacheVersion( long schemaVersion, long dataVersionToken ) {
    return ( ( schemaVersion & 0xffffffffL ) << 32 )
      | ( dataVersionToken & 0xffffffffL );
  }

  static int parseBoundedInt(
    String value,
    int defaultValue,
    int minValue,
    int maxValue ) {
    if ( value == null ) {
      return defaultValue;
    }
    try {
      final int parsed = Integer.parseInt( value.trim() );
      if ( parsed < minValue ) {
        return minValue;
      }
      if ( parsed > maxValue ) {
        return maxValue;
      }
      return parsed;
    } catch ( NumberFormatException ignored ) {
      return defaultValue;
    }
  }

  private static String queryVersion(
    DataSource dataSource,
    String sql,
    int timeoutSec ) throws SQLException {
    if ( dataSource == null ) {
      throw new SQLException( "DataSource is null for ResultCacheVersionSql execution" );
    }
    try ( Connection connection = dataSource.getConnection();
          PreparedStatement statement = connection.prepareStatement( sql ) ) {
      statement.setQueryTimeout( timeoutSec );
      try ( ResultSet rs = statement.executeQuery() ) {
        if ( !rs.next() ) {
          throw new SQLException( "ResultCacheVersionSql returned no rows" );
        }
        final Object value = rs.getObject( 1 );
        if ( value == null ) {
          throw new SQLException( "ResultCacheVersionSql returned null" );
        }
        return String.valueOf( value ).trim();
      }
    }
  }

  private static String trimToNull( String value ) {
    if ( value == null ) {
      return null;
    }
    final String trimmed = value.trim();
    return trimmed.length() == 0 ? null : trimmed;
  }

  private static String nullSafe( String value ) {
    return value == null ? "" : value;
  }

  private static final class VersionState {
    private String lastVersion;
    private long token;
    private long nextCheckAtMillis;
    private long lastErrorLogAtMillis;

    synchronized long currentToken(
      DataSource dataSource,
      String sql,
      int pollMs,
      int timeoutSec,
      long nowMillis,
      ResultReuseCache cache,
      String stateKey ) {
      if ( nowMillis < nextCheckAtMillis ) {
        return token;
      }
      nextCheckAtMillis = nowMillis + pollMs;
      try {
        final String currentVersion =
          trimToNull( queryVersion( dataSource, sql, timeoutSec ) );
        if ( currentVersion == null ) {
          return token;
        }
        if ( lastVersion == null ) {
          lastVersion = currentVersion;
          return token;
        }
        if ( !lastVersion.equals( currentVersion ) ) {
          final String previousVersion = lastVersion;
          lastVersion = currentVersion;
          token++;
          if ( cache != null ) {
            cache.clear();
          }
          LOGGER.info(
            "Result cache invalidated by external version change. key={}, oldVersion={}, newVersion={}",
            stateKey,
            previousVersion,
            currentVersion );
        }
      } catch ( Exception e ) {
        if ( nowMillis - lastErrorLogAtMillis >= ERROR_LOG_INTERVAL_MS ) {
          lastErrorLogAtMillis = nowMillis;
          LOGGER.warn(
            "Result cache version probe failed for key={} (sql='{}'): {}",
            stateKey,
            sql,
            e.getMessage() );
        }
      }
      return token;
    }
  }
}
