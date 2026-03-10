/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared server-side MDX result cache for exact-query reuse.
 *
 * <p>Entries are scoped by full cache key (caller composes key with security
 * and schema context), bounded by LRU and TTL. Values are reference-counted so
 * an entry can be evicted without breaking already issued handles.</p>
 */
public class ResultReuseCache {
  private static final Logger LOGGER =
    LogManager.getLogger( ResultReuseCache.class );

  private final LinkedHashMap<String, CacheEntry> entries =
    new LinkedHashMap<String, CacheEntry>( 16, 0.75f, true );

  public synchronized Result tryAcquire(
    String key,
    long schemaVersion,
    long nowMillis,
    int maxEntries,
    int ttlMillis ) {
    if ( !isEnabled( maxEntries, ttlMillis ) ) {
      clearInternal();
      return null;
    }
    prune( schemaVersion, nowMillis, maxEntries, ttlMillis );
    final CacheEntry entry = entries.get( key );
    if ( entry == null ) {
      return null;
    }
    return entry.acquire();
  }

  public synchronized Result putAndAcquire(
    String key,
    Result result,
    long schemaVersion,
    long nowMillis,
    int maxEntries,
    int ttlMillis ) {
    if ( !isEnabled( maxEntries, ttlMillis ) ) {
      return result;
    }
    prune( schemaVersion, nowMillis, maxEntries, ttlMillis );
    final CacheEntry old = entries.remove( key );
    if ( old != null ) {
      old.releaseOwnerRef();
    }
    final CacheEntry entry = new CacheEntry( result, schemaVersion, nowMillis );
    entries.put( key, entry );
    trimOverflow( maxEntries );
    return entry.acquire();
  }

  public synchronized void clear() {
    clearInternal();
  }

  private boolean isEnabled( int maxEntries, int ttlMillis ) {
    return maxEntries > 0 && ttlMillis > 0;
  }

  private void prune(
    long schemaVersion,
    long nowMillis,
    int maxEntries,
    int ttlMillis ) {
    final Iterator<Map.Entry<String, CacheEntry>> iterator =
      entries.entrySet().iterator();
    while ( iterator.hasNext() ) {
      final CacheEntry entry = iterator.next().getValue();
      final boolean wrongSchema = entry.schemaVersion != schemaVersion;
      final boolean expired = nowMillis - entry.createdAtMillis > ttlMillis;
      if ( wrongSchema || expired ) {
        iterator.remove();
        entry.releaseOwnerRef();
      }
    }
    trimOverflow( maxEntries );
  }

  private void trimOverflow( int maxEntries ) {
    while ( entries.size() > maxEntries ) {
      final Iterator<Map.Entry<String, CacheEntry>> iterator =
        entries.entrySet().iterator();
      if ( !iterator.hasNext() ) {
        return;
      }
      final CacheEntry entry = iterator.next().getValue();
      iterator.remove();
      entry.releaseOwnerRef();
    }
  }

  private void clearInternal() {
    final Iterator<Map.Entry<String, CacheEntry>> iterator =
      entries.entrySet().iterator();
    while ( iterator.hasNext() ) {
      final CacheEntry entry = iterator.next().getValue();
      iterator.remove();
      entry.releaseOwnerRef();
    }
  }

  private static final class CacheEntry {
    private final Result result;
    private final long schemaVersion;
    private final long createdAtMillis;

    // One owner ref is kept while entry stays in cache.
    private int refs = 1;
    private boolean ownerReleased;

    private CacheEntry( Result result, long schemaVersion, long createdAtMillis ) {
      this.result = result;
      this.schemaVersion = schemaVersion;
      this.createdAtMillis = createdAtMillis;
    }

    private synchronized Result acquire() {
      if ( refs <= 0 ) {
        return null;
      }
      refs++;
      return new CachedResultHandle( this );
    }

    private synchronized void releaseOwnerRef() {
      if ( ownerReleased ) {
        return;
      }
      ownerReleased = true;
      releaseOneRef();
    }

    private synchronized void releaseHandleRef() {
      releaseOneRef();
    }

    private void releaseOneRef() {
      refs--;
      if ( refs <= 0 ) {
        try {
          result.close();
        } catch ( RuntimeException e ) {
          LOGGER.debug( "Failed to close cached result handle", e );
        }
      }
    }
  }

  private static final class CachedResultHandle implements Result {
    private final Result result;
    private CacheEntry entry;
    private boolean closed;

    private CachedResultHandle( CacheEntry entry ) {
      this.result = entry.result;
      this.entry = entry;
    }

    private Result delegate() {
      return result;
    }

    public Query getQuery() {
      return delegate().getQuery();
    }

    public Axis[] getAxes() {
      return delegate().getAxes();
    }

    public Axis getSlicerAxis() {
      return delegate().getSlicerAxis();
    }

    public Cell getCell( int[] pos ) {
      return delegate().getCell( pos );
    }

    public void print( PrintWriter pw ) {
      delegate().print( pw );
    }

    public synchronized void close() {
      if ( closed ) {
        return;
      }
      closed = true;
      final CacheEntry e = this.entry;
      this.entry = null;
      if ( e != null ) {
        e.releaseHandleRef();
      }
    }
  }
}

// End ResultReuseCache.java
