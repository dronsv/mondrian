/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.olap.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public synchronized Result tryAcquireMeasureProjection(
    String key,
    Query targetQuery,
    long schemaVersion,
    long nowMillis,
    int maxEntries,
    int ttlMillis ) {
    if ( !isEnabled( maxEntries, ttlMillis ) ) {
      clearInternal();
      return null;
    }
    prune( schemaVersion, nowMillis, maxEntries, ttlMillis );
    final ProjectionQuerySignature targetSignature =
      ProjectionQuerySignature.of( targetQuery );
    if ( targetSignature == null ) {
      return null;
    }
    CacheEntry bestEntry = null;
    int[] bestMap = null;
    int bestExtraMeasures = Integer.MAX_VALUE;
    for ( Map.Entry<String, CacheEntry> mapEntry : entries.entrySet() ) {
      if ( key != null && key.equals( mapEntry.getKey() ) ) {
        continue;
      }
      final CacheEntry entry = mapEntry.getValue();
      final ProjectionQuerySignature sourceSignature =
        entry.projectionSignature();
      final int[] map =
        tryBuildMeasureProjectionMap(
          sourceSignature,
          targetSignature );
      if ( map == null ) {
        continue;
      }
      final int extraMeasures =
        sourceSignature.measureUniqueNames.size()
          - targetSignature.measureUniqueNames.size();
      if ( extraMeasures < bestExtraMeasures ) {
        bestEntry = entry;
        bestMap = map;
        bestExtraMeasures = extraMeasures;
      }
    }
    if ( bestEntry == null || bestMap == null ) {
      return null;
    }
    return bestEntry.acquireProjected( targetQuery, bestMap );
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

  static int[] tryBuildMeasureProjectionMap(
    Query sourceQuery,
    Query targetQuery ) {
    return tryBuildMeasureProjectionMap(
      ProjectionQuerySignature.of( sourceQuery ),
      ProjectionQuerySignature.of( targetQuery ) );
  }

  private static int[] tryBuildMeasureProjectionMap(
    ProjectionQuerySignature sourceSignature,
    ProjectionQuerySignature targetSignature ) {
    if ( sourceSignature == null || targetSignature == null ) {
      return null;
    }
    if ( !sourceSignature.shapeSignature.equals( targetSignature.shapeSignature ) ) {
      return null;
    }
    final List<String> sourceMeasures = sourceSignature.measureUniqueNames;
    final List<String> targetMeasures = targetSignature.measureUniqueNames;
    if ( sourceMeasures.size() <= targetMeasures.size() ) {
      return null;
    }
    final Map<String, Integer> sourceMeasureIndex = new HashMap<String, Integer>();
    for ( int i = 0; i < sourceMeasures.size(); i++ ) {
      sourceMeasureIndex.put( sourceMeasures.get( i ), i );
    }
    final int[] map = new int[targetMeasures.size()];
    for ( int i = 0; i < targetMeasures.size(); i++ ) {
      final Integer sourceIndex = sourceMeasureIndex.get( targetMeasures.get( i ) );
      if ( sourceIndex == null ) {
        return null;
      }
      map[i] = sourceIndex;
    }
    return map;
  }

  private static final class ProjectionQuerySignature {
    private final String shapeSignature;
    private final List<String> measureUniqueNames;

    private ProjectionQuerySignature(
      String shapeSignature,
      List<String> measureUniqueNames ) {
      this.shapeSignature = shapeSignature;
      this.measureUniqueNames = measureUniqueNames;
    }

    private static ProjectionQuerySignature of( Query query ) {
      if ( query == null ) {
        return null;
      }
      final List<String> measures = extractMeasureUniqueNames( query );
      if ( measures == null || measures.isEmpty() ) {
        return null;
      }
      final String shapeSignature = buildShapeSignature( query );
      if ( shapeSignature == null || shapeSignature.length() == 0 ) {
        return null;
      }
      return new ProjectionQuerySignature( shapeSignature, measures );
    }

    private static List<String> extractMeasureUniqueNames( Query query ) {
      final QueryAxis[] axes = query.getAxes();
      if ( axes == null || axes.length == 0 || axes[0] == null ) {
        return null;
      }
      final Exp columnSet = axes[0].getSet();
      if ( columnSet == null ) {
        return null;
      }
      final List<String> out = new ArrayList<String>();
      if ( !collectMeasureUniqueNames( columnSet, out ) || out.isEmpty() ) {
        return null;
      }
      final Set<String> uniq = new HashSet<String>( out );
      if ( uniq.size() != out.size() ) {
        return null;
      }
      return Collections.unmodifiableList( out );
    }

    private static boolean collectMeasureUniqueNames(
      Exp exp,
      List<String> out ) {
      if ( exp instanceof MemberExpr ) {
        final Member member = ( (MemberExpr) exp ).getMember();
        if ( member == null || !member.isMeasure() || member.getUniqueName() == null ) {
          return false;
        }
        out.add( member.getUniqueName() );
        return true;
      }
      if ( exp instanceof FunCall ) {
        final FunCall call = (FunCall) exp;
        if ( "{}".equals( call.getFunName() ) ) {
          if ( call.getArgCount() == 0 ) {
            return false;
          }
          for ( Exp arg : call.getArgs() ) {
            if ( !collectMeasureUniqueNames( arg, out ) ) {
              return false;
            }
          }
          return true;
        }
        if ( "()".equals( call.getFunName() ) && call.getArgCount() == 1 ) {
          return collectMeasureUniqueNames( call.getArg( 0 ), out );
        }
      }
      return false;
    }

    private static String buildShapeSignature( Query query ) {
      final QueryAxis[] axes = query.getAxes();
      if ( axes == null || axes.length == 0 || axes[0] == null ) {
        return null;
      }
      final StringBuilder sb = new StringBuilder( 512 );
      final Cube cube = query.getCube();
      sb.append( "cube=" )
        .append( cube == null || cube.getName() == null ? "" : cube.getName() )
        .append( '|' );
      sb.append( "axisCount=" ).append( axes.length ).append( '|' );
      sb.append( "axis0.nonEmpty=" ).append( axes[0].isNonEmpty() ).append( '|' );
      sb.append( "axis0.ordered=" ).append( axes[0].isOrdered() ).append( '|' );
      sb.append( "axis0.dimProps=" )
        .append( unparseIds( axes[0].getDimensionProperties() ) )
        .append( '|' );
      for ( int i = 1; i < axes.length; i++ ) {
        if ( axes[i] == null ) {
          return null;
        }
        sb.append( "axis" ).append( i ).append( '=' )
          .append( unparseQueryPart( axes[i] ) )
          .append( '|' );
      }
      final QueryAxis slicerAxis = query.getSlicerAxis();
      sb.append( "slicer=" )
        .append( slicerAxis == null ? "" : unparseQueryPart( slicerAxis ) )
        .append( '|' );
      final Formula[] formulas = query.getFormulas();
      if ( formulas != null ) {
        sb.append( "formulas=" );
        for ( Formula formula : formulas ) {
          if ( formula != null ) {
            sb.append( unparseQueryPart( formula ) );
          }
          sb.append( ';' );
        }
        sb.append( '|' );
      }
      final QueryPart[] cellProps = query.getCellProperties();
      if ( cellProps != null ) {
        sb.append( "cellProps=" );
        for ( QueryPart cellProp : cellProps ) {
          if ( cellProp != null ) {
            sb.append( unparseQueryPart( cellProp ) );
          }
          sb.append( ';' );
        }
      }
      return sb.toString();
    }

    private static String unparseIds( Id[] ids ) {
      if ( ids == null || ids.length == 0 ) {
        return "";
      }
      final StringBuilder sb = new StringBuilder();
      for ( int i = 0; i < ids.length; i++ ) {
        if ( i > 0 ) {
          sb.append( ',' );
        }
        if ( ids[i] != null ) {
          sb.append( unparseQueryPart( ids[i] ) );
        }
      }
      return sb.toString();
    }

    private static String unparseQueryPart( QueryPart part ) {
      if ( part == null ) {
        return "";
      }
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter( sw );
      part.unparse( pw );
      pw.flush();
      return sw.toString();
    }
  }

  private static final class CacheEntry {
    private final Result result;
    private final long schemaVersion;
    private final long createdAtMillis;
    private ProjectionQuerySignature projectionSignature;

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

    private synchronized Result acquireProjected(
      Query projectedQuery,
      int[] columnAxisMap ) {
      if ( refs <= 0 ) {
        return null;
      }
      refs++;
      return new ProjectedResultHandle( this, projectedQuery, columnAxisMap );
    }

    private ProjectionQuerySignature projectionSignature() {
      if ( projectionSignature == null ) {
        projectionSignature = ProjectionQuerySignature.of( result.getQuery() );
      }
      return projectionSignature;
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

  private static final class ProjectedResultHandle implements Result {
    private final Result result;
    private final Query query;
    private final int[] columnAxisMap;
    private final Axis[] axes;
    private CacheEntry entry;
    private boolean closed;

    private ProjectedResultHandle(
      CacheEntry entry,
      Query query,
      int[] columnAxisMap ) {
      this.result = entry.result;
      this.entry = entry;
      this.query = query;
      this.columnAxisMap = columnAxisMap.clone();
      this.axes = projectAxes( result.getAxes(), this.columnAxisMap );
    }

    private static Axis[] projectAxes( Axis[] sourceAxes, int[] columnAxisMap ) {
      if ( sourceAxes == null || sourceAxes.length == 0 ) {
        return sourceAxes;
      }
      final Axis sourceColumnsAxis = sourceAxes[0];
      if ( sourceColumnsAxis == null ) {
        return sourceAxes;
      }
      final List<Position> sourcePositions = sourceColumnsAxis.getPositions();
      if ( sourcePositions == null ) {
        return sourceAxes;
      }
      final List<Position> projectedPositions =
        new ArrayList<Position>( columnAxisMap.length );
      for ( int sourceIndex : columnAxisMap ) {
        if ( sourceIndex < 0 || sourceIndex >= sourcePositions.size() ) {
          return sourceAxes;
        }
        projectedPositions.add( sourcePositions.get( sourceIndex ) );
      }
      final Axis[] projectedAxes = sourceAxes.clone();
      projectedAxes[0] = new ProjectedAxis( projectedPositions );
      return projectedAxes;
    }

    private Result delegate() {
      return result;
    }

    public Query getQuery() {
      return query == null ? delegate().getQuery() : query;
    }

    public Axis[] getAxes() {
      return axes;
    }

    public Axis getSlicerAxis() {
      return delegate().getSlicerAxis();
    }

    public Cell getCell( int[] pos ) {
      if ( pos == null || pos.length == 0 ) {
        return delegate().getCell( pos );
      }
      final int projectedColumn = pos[0];
      if ( projectedColumn < 0 || projectedColumn >= columnAxisMap.length ) {
        return delegate().getCell( pos );
      }
      final int[] mappedPos = pos.clone();
      mappedPos[0] = columnAxisMap[projectedColumn];
      return delegate().getCell( mappedPos );
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

  private static final class ProjectedAxis implements Axis {
    private final List<Position> positions;

    private ProjectedAxis( List<Position> positions ) {
      this.positions =
        Collections.unmodifiableList(
          new ArrayList<Position>( positions ) );
    }

    public List<Position> getPositions() {
      return positions;
    }
  }
}

// End ResultReuseCache.java
