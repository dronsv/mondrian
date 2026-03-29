/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// Copyright (C) 2025 Sergei Semenkov
// All Rights Reserved.
//
// jhyde, 30 August, 2001
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.rolap.SqlStatement.Type;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.server.Locus;
import mondrian.server.Session;
import mondrian.util.Pair;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;

/**
 * <code>RolapAggregationManager</code> manages all {@link Aggregation}s
 * in the system. It is a singleton class.
 *
 * @author jhyde
 * @since 30 August, 2001
 */
public class AggregationManager extends RolapAggregationManager {
    /**
     * Optional caller-supplied filter for aggregate candidates.
     */
    public interface AggStarFilter {
        boolean allows(AggStar aggStar);
    }

    private static final MondrianProperties properties =
        MondrianProperties.instance();

    private static final Logger LOGGER =
        LogManager.getLogger(AggregationManager.class);

    public SegmentCacheManager cacheMgr;

    private MondrianServer server;

    /**
     * Creates the AggregationManager.
     */
    public AggregationManager(MondrianServer server) {
        this.server = server;
        if (properties.EnableCacheHitCounters.get()) {
            LOGGER.error(
                "Property " + properties.EnableCacheHitCounters.getPath()
                + " is obsolete; ignored.");
        }
        this.cacheMgr = new SegmentCacheManager(server);
    }

    /**
     * Returns the log4j logger.
     *
     * @return Logger
     */
    public final Logger getLogger() {
        return LOGGER;
    }

    /**
     * Returns or creates the singleton.
     *
     * @deprecated No longer a singleton, and will be removed in mondrian-4.
     *   Use {@link mondrian.olap.MondrianServer#getAggregationManager()}.
     *   To get a server, call
     *   {@link mondrian.olap.MondrianServer#forConnection(mondrian.olap.Connection)},
     *   passing in a null connection if you absolutely must.
     */
    public static synchronized AggregationManager instance() {
        return
            MondrianServer.forId(null).getAggregationManager();
    }

    /**
     * Called by FastBatchingCellReader.load where the
     * RolapStar creates an Aggregation if needed.
     *
     * @param cacheMgr Cache manager
     * @param cellRequestCount Number of missed cells that led to this request
     * @param measures Measures to load
     * @param columns this is the CellRequest's constrained columns
     * @param aggregationKey this is the CellRequest's constraint key
     * @param predicates Array of constraints on each column
     * @param groupingSetsCollector grouping sets collector
     * @param segmentFutures List of futures into which each statement will
     *     place a list of the segments it has loaded, when it completes
     */
    public static void loadAggregation(
        SegmentCacheManager cacheMgr,
        int cellRequestCount,
        List<RolapStar.Measure> measures,
        RolapStar.Column[] columns,
        AggregationKey aggregationKey,
        StarColumnPredicate[] predicates,
        GroupingSetsCollector groupingSetsCollector,
        List<Future<Map<Segment, SegmentWithData>>> segmentFutures,
        StarPredicate subcubePredicate)
    {
        RolapStar star = measures.get(0).getStar();
        Aggregation aggregation =
            star.lookupOrCreateAggregation(aggregationKey);

        // try to eliminate unnecessary constraints
        // for Oracle: prevent an IN-clause with more than 1000 elements
        predicates = aggregation.optimizePredicates(columns, predicates);
        aggregation.load(
            cacheMgr, cellRequestCount, columns, measures, predicates,
            groupingSetsCollector, segmentFutures, subcubePredicate);
    }

    /**
     * Returns an API with which to explicitly manage the contents of the cache.
     *
     * @param connection Server whose cache to control
     * @param pw Print writer, for tracing
     * @return CacheControl API
     */
    public CacheControl getCacheControl(
        RolapConnection connection,
        final PrintWriter pw)
    {
        return new CacheControlImpl(connection) {
            protected void flushNonUnion(final CellRegion region) {
                SegmentCacheManager segmentCacheManager = getCacheMgr(connection);
                final SegmentCacheManager.FlushResult result =
                    segmentCacheManager.execute(
                        new SegmentCacheManager.FlushCommand(
                            Locus.peek(),
                            segmentCacheManager,
                            region,
                            this));
                final List<Future<Boolean>> futures =
                    new ArrayList<Future<Boolean>>();
                for (Callable<Boolean> task : result.tasks) {
                    futures.add(segmentCacheManager.cacheExecutor.submit(task));
                }
                for (Future<Boolean> future : futures) {
                    Util.discard(Util.safeGet(future, "Flush cache"));
                }
            }

            public void flush(final CellRegion region) {
                if (pw != null) {
                    pw.println("Cache state before flush:");
                    printCacheState(pw, region);
                    pw.println();
                }
                super.flush(region);
                if (pw != null) {
                    pw.println("Cache state after flush:");
                    printCacheState(pw, region);
                    pw.println();
                }
            }

            public void trace(final String message) {
                if (pw != null) {
                    pw.println(message);
                }
            }

            public boolean isTraceEnabled() {
                return pw != null;
            }
        };
    }

    public Object getCellFromCache(CellRequest request) {
        return getCellFromCache(request, null);
    }

    public Object getCellFromCache(CellRequest request, PinSet pinSet) {
        // NOTE: This method used to check both local (thread/statement) cache
        // and global cache (segments in JVM, shared between statements). Now it
        // only looks in local cache. This can be done without acquiring any
        // locks, because the local cache is thread-local. If a segment that
        // matches this cell-request in global cache, a call to
        // SegmentCacheManager will copy it into local cache.
        final RolapStar.Measure measure = request.getMeasure();
        return measure.getStar().getCellFromCache(request, pinSet);
    }

    public Object getCellFromAllCaches(CellRequest request, RolapConnection rolapConnection) {
        final RolapStar.Measure measure = request.getMeasure();
        return measure.getStar().getCellFromAllCaches(request, rolapConnection);
    }

    public String getDrillThroughSql(
        final DrillThroughCellRequest request,
        final StarPredicate starPredicateSlicer,
        List<OlapElement> fields,
        final boolean countOnly)
    {
        DrillThroughQuerySpec spec =
            new DrillThroughQuerySpec(
                request,
                starPredicateSlicer,
                fields,
                countOnly);
        Pair<String, List<SqlStatement.Type>> pair = spec.generateSqlQuery();

        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                "DrillThroughSQL: "
                + pair.left
                + Util.nl);
        }

        return pair.left;
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     * Called by Segment.load.
     *
     * @return A pair consisting of a SQL statement and a list of suggested
     *     types of columns
     */
    public static Pair<String, List<SqlStatement.Type>> generateSql(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList)
    {
        final RolapStar star = groupingSetsList.getStar();
        BitKey levelBitKey = groupingSetsList.getDefaultLevelBitKey();
        BitKey measureBitKey = groupingSetsList.getDefaultMeasureBitKey();
        SortedMap<Integer, SortedSet<String>> constrainedLevelNames =
            groupingSetsList.getDefaultConstrainedLevelNamesByBitPosition();
        final boolean hasSubcubePredicate =
            hasSubcubePredicate(groupingSetsList.getDefaultSegments());
        final StarPredicate sharedSubcubePredicate =
            getSharedSubcubePredicate(groupingSetsList.getDefaultSegments());
        final NormalizedSubcubePredicate normalizedSubcubePredicate =
            normalizeSubcubePredicate(groupingSetsList.getDefaultSegments());
        final List<StarPredicate> nonSubcubeCompoundPredicates =
            stripSharedSubcubePredicate(
                compoundPredicateList,
                sharedSubcubePredicate);
        final BitKey effectiveLevelBitKey =
            normalizedSubcubePredicate == null
                ? levelBitKey
                : levelBitKey.or(normalizedSubcubePredicate.getBitKey());
        final SortedMap<Integer, SortedSet<String>>
            effectiveConstrainedLevelNames =
                normalizedSubcubePredicate == null
                    ? constrainedLevelNames
                    : mergeConstrainedLevelNames(
                        constrainedLevelNames,
                        normalizedSubcubePredicate.getConstrainedLevelNames());

        final boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get();
        final int compoundPredicateCount =
            nonSubcubeCompoundPredicates == null
                ? 0
                : nonSubcubeCompoundPredicates.size();
        final String aggBypassReason =
            getAggBypassReason(
                useAggregates,
                compoundPredicateCount,
                hasSubcubePredicate,
                normalizedSubcubePredicate != null);

        if (aggBypassReason == null)
        {
            final boolean[] rollup = {false};
            AggStar aggStar =
                findAgg(
                    star,
                    effectiveLevelBitKey,
                    measureBitKey,
                    rollup,
                    effectiveConstrainedLevelNames);

            if (aggStar != null) {
                // Got a match, hot damn

                if (LOGGER.isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(256);
                    buf.append("MATCH: ");
                    buf.append(star.getFactTable().getAlias());
                    buf.append(Util.nl);
                    buf.append("   foreign=");
                    buf.append(effectiveLevelBitKey);
                    buf.append(Util.nl);
                    buf.append("   measure=");
                    buf.append(measureBitKey);
                    buf.append(Util.nl);
                    buf.append("   aggstar=");
                    buf.append(aggStar.getBitKey());
                    buf.append(Util.nl);
                    buf.append("AggStar=");
                    buf.append(aggStar.getFactTable().getName());
                    buf.append(Util.nl);
                    for (AggStar.Table.Column column
                        : aggStar.getFactTable().getColumns())
                    {
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.nl);
                    }
                    LOGGER.debug(buf.toString());
                }

                AggQuerySpec aggQuerySpec =
                    new AggQuerySpec(
                        aggStar,
                        rollup[0],
                        groupingSetsList,
                        normalizedSubcubePredicate == null
                            ? Collections.<Integer, StarColumnPredicate>
                                emptySortedMap()
                            : normalizedSubcubePredicate.getPredicatesByBitPos());
                Pair<String, List<Type>> sql = aggQuerySpec.generateSqlQuery();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "generateSqlQuery: sql="
                        + sql.left
                        + ", agg=true"
                        + ", measures=" + measureBitKey
                        + ", subcubePredicate="
                        + (sharedSubcubePredicate == null
                            ? "<none>"
                            : sharedSubcubePredicate)
                        + ", normalizedSubcubePredicate="
                        + (normalizedSubcubePredicate == null
                            ? "<none>"
                            : normalizedSubcubePredicate.describe()));
                }

                return sql;
            }

            // No match, fall through and use fact table.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "AGG NO MATCH: star=" + star.getFactTable().getAlias()
                    + ", levelBitKey=" + levelBitKey
                    + ", effectiveLevelBitKey=" + effectiveLevelBitKey
                    + ", measureBitKey=" + measureBitKey
                    + ", constrainedLevelNames=" + constrainedLevelNames
                    + ", effectiveConstrainedLevelNames="
                    + effectiveConstrainedLevelNames
                    + ", nonSubcubeCompoundPredicates="
                    + describePredicates(nonSubcubeCompoundPredicates)
                    + ", sharedSubcubePredicate="
                    + (sharedSubcubePredicate == null
                        ? "<none>"
                        : sharedSubcubePredicate)
                    + ", normalizedSubcubePredicate="
                    + (normalizedSubcubePredicate == null
                        ? "<none>"
                        : normalizedSubcubePredicate.describe()));
            }
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "AGG BYPASS: reason=" + aggBypassReason
                + ", star=" + star.getFactTable().getAlias()
                + ", levelBitKey=" + levelBitKey
                + ", effectiveLevelBitKey=" + effectiveLevelBitKey
                + ", measureBitKey=" + measureBitKey
                + ", compoundPredicates=" + compoundPredicateCount
                + ", hasSubcubePredicate=" + hasSubcubePredicate
                + ", nonSubcubeCompoundPredicates="
                + describePredicates(nonSubcubeCompoundPredicates)
                + ", sharedSubcubePredicate="
                + (sharedSubcubePredicate == null
                    ? "<none>"
                    : sharedSubcubePredicate)
                + ", normalizedSubcubePredicate="
                + (normalizedSubcubePredicate == null
                    ? "<none>"
                    : normalizedSubcubePredicate.describe()));
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("NO MATCH : ");
            sb.append(star.getFactTable().getAlias());
            sb.append(Util.nl);
            sb.append("Foreign columns bit key=");
            sb.append(effectiveLevelBitKey);
            sb.append(Util.nl);
            sb.append("Measure bit key=        ");
            sb.append(measureBitKey);
            sb.append(Util.nl);
            sb.append("Agg Stars=[");
            sb.append(Util.nl);
            for (AggStar aggStar : star.getAggStars()) {
                sb.append(aggStar.toString());
            }
            sb.append(Util.nl);
            sb.append("]");
            LOGGER.debug(sb.toString());
        }


        // Fact table query
        SegmentArrayQuerySpec spec =
            new SegmentArrayQuerySpec(groupingSetsList, compoundPredicateList);

        Pair<String, List<SqlStatement.Type>> pair = spec.generateSqlQuery();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "generateSqlQuery: sql=" + pair.left);
        }

        return pair;
    }

    static String getAggBypassReason(
        boolean useAggregates,
        int compoundPredicateCount,
        boolean hasSubcubePredicate,
        boolean hasNormalizedSubcubePredicate)
    {
        if (!useAggregates) {
            return "use_aggregates_disabled";
        }
        if (hasSubcubePredicate && !hasNormalizedSubcubePredicate) {
            return "subcube_predicate_present";
        }
        if (compoundPredicateCount > 0) {
            return "compound_predicates_present";
        }
        return null;
    }

    private static boolean hasSubcubePredicate(List<Segment> segments) {
        for (Segment segment : segments) {
            if (segment.subcubePredicate != null) {
                return true;
            }
        }
        return false;
    }

    private static StarPredicate getSharedSubcubePredicate(List<Segment> segments) {
        StarPredicate predicate = null;
        for (Segment segment : segments) {
            if (segment.subcubePredicate == null) {
                continue;
            }
            if (predicate == null) {
                predicate = segment.subcubePredicate;
            } else if (!predicate.equalConstraint(segment.subcubePredicate)) {
                return null;
            }
        }
        return predicate;
    }

    static List<StarPredicate> stripSharedSubcubePredicate(
        List<StarPredicate> compoundPredicateList,
        StarPredicate sharedSubcubePredicate)
    {
        if (compoundPredicateList == null || compoundPredicateList.isEmpty()) {
            return compoundPredicateList;
        }
        if (sharedSubcubePredicate == null) {
            return compoundPredicateList;
        }
        final List<StarPredicate> stripped =
            new ArrayList<StarPredicate>(compoundPredicateList.size());
        for (StarPredicate predicate : compoundPredicateList) {
            if (predicate != null
                && predicate.equalConstraint(sharedSubcubePredicate))
            {
                continue;
            }
            stripped.add(predicate);
        }
        return stripped;
    }

    static String describePredicates(List<StarPredicate> predicates) {
        return predicates == null ? "<null>" : predicates.toString();
    }

    private static SortedMap<Integer, SortedSet<String>>
    mergeConstrainedLevelNames(
        SortedMap<Integer, SortedSet<String>> base,
        SortedMap<Integer, SortedSet<String>> extra)
    {
        final SortedMap<Integer, SortedSet<String>> merged =
            new TreeMap<Integer, SortedSet<String>>();
        copyConstrainedLevelNames(base, merged);
        copyConstrainedLevelNames(extra, merged);
        return merged;
    }

    private static void copyConstrainedLevelNames(
        SortedMap<Integer, SortedSet<String>> from,
        SortedMap<Integer, SortedSet<String>> into)
    {
        for (Map.Entry<Integer, SortedSet<String>> entry : from.entrySet()) {
            SortedSet<String> levelNames = into.get(entry.getKey());
            if (levelNames == null) {
                levelNames = new TreeSet<String>();
                into.put(entry.getKey(), levelNames);
            }
            levelNames.addAll(entry.getValue());
        }
    }

    private static NormalizedSubcubePredicate normalizeSubcubePredicate(
        List<Segment> segments)
    {
        StarPredicate predicate = null;
        for (Segment segment : segments) {
            if (segment.subcubePredicate == null) {
                continue;
            }
            if (predicate == null) {
                predicate = segment.subcubePredicate;
            } else if (!predicate.equalConstraint(segment.subcubePredicate)) {
                return null;
            }
        }
        if (predicate == null) {
            return null;
        }
        return normalizeSubcubePredicate(predicate);
    }

    static NormalizedSubcubePredicate normalizeSubcubePredicate(
        StarPredicate predicate)
    {
        if (predicate instanceof StarColumnPredicate) {
            return NormalizedSubcubePredicate.of(
                (StarColumnPredicate) predicate);
        }
        if (predicate instanceof AndPredicate) {
            return normalizeAndPredicate((AndPredicate) predicate);
        }
        if (predicate instanceof OrPredicate) {
            return normalizeOrPredicate((OrPredicate) predicate);
        }
        return null;
    }

    private static NormalizedSubcubePredicate normalizeAndPredicate(
        AndPredicate predicate)
    {
        final NormalizedSubcubePredicate merged =
            new NormalizedSubcubePredicate();
        for (StarPredicate child : predicate.getChildren()) {
            final NormalizedSubcubePredicate childNormalized =
                normalizeSubcubePredicate(child);
            if (childNormalized == null || !merged.mergeAnd(childNormalized)) {
                return null;
            }
        }
        return merged;
    }

    private static NormalizedSubcubePredicate normalizeOrPredicate(
        OrPredicate predicate)
    {
        final List<NormalizedSubcubePredicate> children =
            new ArrayList<NormalizedSubcubePredicate>();
        for (StarPredicate child : predicate.getChildren()) {
            final NormalizedSubcubePredicate childNormalized =
                normalizeSubcubePredicate(child);
            if (childNormalized == null) {
                return null;
            }
            children.add(childNormalized);
        }
        if (children.isEmpty()) {
            return null;
        }

        final NormalizedSubcubePredicate common =
            children.get(0).copyCommonPrefix(children);
        Integer varyingBitPos = null;
        final List<StarColumnPredicate> varyingPredicates =
            new ArrayList<StarColumnPredicate>();

        for (NormalizedSubcubePredicate child : children) {
            final SortedMap<Integer, StarColumnPredicate> remainder =
                child.without(common);
            if (remainder.isEmpty()) {
                continue;
            }
            if (remainder.size() != 1) {
                return null;
            }
            final Map.Entry<Integer, StarColumnPredicate> entry =
                remainder.entrySet().iterator().next();
            if (varyingBitPos == null) {
                varyingBitPos = entry.getKey();
            } else if (!varyingBitPos.equals(entry.getKey())) {
                return null;
            }
            varyingPredicates.add(entry.getValue());
        }

        if (varyingBitPos == null) {
            return common;
        }
        if (varyingPredicates.isEmpty()) {
            return null;
        }

        final RolapStar.Column column =
            varyingPredicates.get(0).getConstrainedColumn();
        if (column == null) {
            return null;
        }
        final ListColumnPredicate listPredicate =
            new ListColumnPredicate(column, varyingPredicates);
        common.addPredicate(varyingBitPos, listPredicate, varyingPredicates);
        return common;
    }

    static final class NormalizedSubcubePredicate {
        private final SortedMap<Integer, StarColumnPredicate> predicatesByBitPos =
            new TreeMap<Integer, StarColumnPredicate>();
        private final SortedMap<Integer, SortedSet<String>> constrainedLevelNames =
            new TreeMap<Integer, SortedSet<String>>();

        static NormalizedSubcubePredicate of(StarColumnPredicate predicate) {
            final NormalizedSubcubePredicate normalized =
                new NormalizedSubcubePredicate();
            normalized.addPredicate(
                predicate.getConstrainedColumn().getBitPosition(),
                predicate,
                Collections.singletonList(predicate));
            return normalized;
        }

        boolean mergeAnd(NormalizedSubcubePredicate other) {
            for (Map.Entry<Integer, StarColumnPredicate> entry
                : other.predicatesByBitPos.entrySet())
            {
                final StarColumnPredicate existing =
                    predicatesByBitPos.get(entry.getKey());
                if (existing != null
                    && !existing.equalConstraint(entry.getValue()))
                {
                    return false;
                }
            }
            copyConstrainedLevelNames(
                other.constrainedLevelNames,
                constrainedLevelNames);
            predicatesByBitPos.putAll(other.predicatesByBitPos);
            return true;
        }

        NormalizedSubcubePredicate copyCommonPrefix(
            List<NormalizedSubcubePredicate> predicates)
        {
            final NormalizedSubcubePredicate common =
                new NormalizedSubcubePredicate();
            for (Map.Entry<Integer, StarColumnPredicate> entry
                : predicatesByBitPos.entrySet())
            {
                boolean presentInAll = true;
                for (int i = 1; i < predicates.size(); i++) {
                    final StarColumnPredicate other =
                        predicates.get(i).predicatesByBitPos.get(entry.getKey());
                    if (other == null
                        || !entry.getValue().equalConstraint(other))
                    {
                        presentInAll = false;
                        break;
                    }
                }
                if (presentInAll) {
                    common.addPredicate(
                        entry.getKey(),
                        entry.getValue(),
                        Collections.singletonList(entry.getValue()));
                    final SortedSet<String> levelNames =
                        constrainedLevelNames.get(entry.getKey());
                    if (levelNames != null) {
                        common.constrainedLevelNames.put(
                            entry.getKey(),
                            new TreeSet<String>(levelNames));
                    }
                }
            }
            return common;
        }

        SortedMap<Integer, StarColumnPredicate> without(
            NormalizedSubcubePredicate other)
        {
            final SortedMap<Integer, StarColumnPredicate> remainder =
                new TreeMap<Integer, StarColumnPredicate>();
            for (Map.Entry<Integer, StarColumnPredicate> entry
                : predicatesByBitPos.entrySet())
            {
                if (!other.predicatesByBitPos.containsKey(entry.getKey())) {
                    remainder.put(entry.getKey(), entry.getValue());
                }
            }
            return remainder;
        }

        void addPredicate(
            int bitPos,
            StarColumnPredicate predicate,
            List<StarColumnPredicate> sources)
        {
            predicatesByBitPos.put(bitPos, predicate);
            SortedSet<String> levelNames = constrainedLevelNames.get(bitPos);
            if (levelNames == null) {
                levelNames = new TreeSet<String>();
                constrainedLevelNames.put(bitPos, levelNames);
            }
            for (StarColumnPredicate source : sources) {
                if (source instanceof MemberColumnPredicate) {
                    levelNames.add(
                        ((MemberColumnPredicate) source)
                            .getMember()
                            .getLevel()
                            .getUniqueName());
                }
            }
            if (levelNames.isEmpty()) {
                constrainedLevelNames.remove(bitPos);
            }
        }

        BitKey getBitKey() {
            final BitKey bitKey = BitKey.Factory.makeBitKey(0);
            for (Integer bitPos : predicatesByBitPos.keySet()) {
                bitKey.set(bitPos);
            }
            return bitKey;
        }

        SortedMap<Integer, StarColumnPredicate> getPredicatesByBitPos() {
            return Collections.unmodifiableSortedMap(predicatesByBitPos);
        }

        SortedMap<Integer, SortedSet<String>> getConstrainedLevelNames() {
            return Collections.unmodifiableSortedMap(constrainedLevelNames);
        }

        String describe() {
            return "bitKey=" + getBitKey()
                + ", predicates=" + predicatesByBitPos
                + ", constrainedLevelNames=" + constrainedLevelNames;
        }
    }

    /**
     * Finds an aggregate table in the given star which has the desired levels
     * and measures. Returns null if no aggregate table is suitable.
     *
     * <p>If there no aggregate is an exact match, returns a more
     * granular aggregate which can be rolled up, and sets rollup to true.
     * If one or more of the measures are distinct-count measures
     * rollup is possible only in limited circumstances.
     *
     * @param star Star
     * @param levelBitKey Set of levels
     * @param measureBitKey Set of measures
     * @param rollup Out parameter, is set to true if the aggregate is not
     *   an exact match
     * @return An aggregate, or null if none is suitable.
     */
    public static AggStar findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup)
    {
        return findAgg(
            star,
            levelBitKey,
            measureBitKey,
            rollup,
            Collections.<Integer, SortedSet<String>>emptySortedMap(),
            null);
    }

    public static AggStar findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup,
        SortedMap<Integer, SortedSet<String>> constrainedLevelNames)
    {
        return findAgg(
            star,
            levelBitKey,
            measureBitKey,
            rollup,
            constrainedLevelNames,
            null);
    }

    public static AggStar findAggConsideringSubcubePredicate(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup,
        SortedMap<Integer, SortedSet<String>> constrainedLevelNames,
        StarPredicate subcubePredicate)
    {
        if (subcubePredicate == null) {
            return findAgg(
                star,
                levelBitKey,
                measureBitKey,
                rollup,
                constrainedLevelNames);
        }
        final NormalizedSubcubePredicate normalizedSubcubePredicate =
            normalizeSubcubePredicate(subcubePredicate);
        if (normalizedSubcubePredicate == null) {
            return null;
        }
        final BitKey effectiveLevelBitKey =
            levelBitKey.or(normalizedSubcubePredicate.getBitKey());
        final SortedMap<Integer, SortedSet<String>>
            effectiveConstrainedLevelNames =
                mergeConstrainedLevelNames(
                    constrainedLevelNames,
                    normalizedSubcubePredicate.getConstrainedLevelNames());
        return findAgg(
            star,
            effectiveLevelBitKey,
            measureBitKey,
            rollup,
            effectiveConstrainedLevelNames);
    }

    public static AggStar findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup,
        SortedMap<Integer, SortedSet<String>> constrainedLevelNames,
        AggStarFilter aggStarFilter)
    {
        // If there is no distinct count measure, isDistinct == false,
        // then all we want is an AggStar whose BitKey is a superset
        // of the combined measure BitKey and foreign-key/level BitKey.
        //
        // On the other hand, if there is at least one distinct count
        // measure, isDistinct == true, then what is wanted is an AggStar
        // whose measure BitKey is a superset of the measure BitKey,
        // whose level BitKey is an exact match and the aggregate table
        // can NOT have any foreign keys.
        assert rollup != null;
        BitKey fullBitKey = levelBitKey.or(measureBitKey);
        final boolean distinctMergeConstrainedRollupEnabled =
            isDistinctMergeConstrainedRollupEnabled();

        // a levelBitKey with all parent bits set.
        final BitKey expandedLevelBitKey = expandLevelBitKey(
            star, levelBitKey.copy());

        // The AggStars are already ordered from smallest to largest so
        // we need only find the first one and return it.
        for (AggStar aggStar : star.getAggStars()) {
            // superset match
            if (!aggStar.superSetMatch(fullBitKey)) {
                continue;
            }
            boolean isDistinct = measureBitKey.intersects(
                aggStar.getDistinctMeasureBitKey());

            // The AggStar has no "distinct count" measures so
            // we can use it without looking any further.
            if (!isDistinct) {
                if (!matchesRequestedLevels(
                    aggStar,
                    expandedLevelBitKey,
                    constrainedLevelNames))
                {
                    continue;
                }
                if (aggStarFilter != null && !aggStarFilter.allows(aggStar)) {
                    continue;
                }
                // Need to use SUM if the query levels don't match
                // the agg stars levels, or if the agg star is not
                // fully collapsed.
                rollup[0] = !aggStar.isFullyCollapsed()
                    || aggStar.hasIgnoredColumns()
                    || (levelBitKey.isEmpty()
                    || !aggStar.getLevelBitKey().equals(levelBitKey));
                return aggStar;
            } else if (aggStar.hasIgnoredColumns()) {
                final boolean distinctCountMergeEnabled =
                    areSelectedDistinctMeasuresMergeEnabled(
                        aggStar,
                        measureBitKey);
                // If the distinct-count merge function is configured,
                // allow agg tables with ignored columns for distinct-count
                // measures. The merge function (e.g. uniqCombinedMerge in
                // ClickHouse) can correctly roll up pre-aggregated states.
                if (!distinctCountMergeEnabled) {
                    // Original behavior: skip agg tables with ignored columns
                    LOGGER.info(
                        aggStar.getFactTable().getName()
                        + " cannot be used for distinct-count measures since"
                        + " it has unused or ignored columns.");
                    continue;
                }
                LOGGER.info(
                    aggStar.getFactTable().getName()
                    + " using configured merge function mapping"
                    + "' for distinct-count measures with ignored columns.");
            }

            // If there are distinct measures, we can only rollup in limited
            // circumstances.

            // No foreign keys (except when its used as a distinct count
            //   measure).
            // Level key exact match.
            // Measure superset match.

            // Compute the core levels -- those which can be safely
            // rolled up to. For example,
            // if the measure is 'distinct customer count',
            // and the agg table has levels customer_id,
            // then gender is a core level.
            final BitKey distinctMeasuresBitKey =
                measureBitKey.and(aggStar.getDistinctMeasureBitKey());
            final BitSet distinctMeasures = distinctMeasuresBitKey.toBitSet();
            BitKey combinedLevelBitKey = null;
            boolean distinctCountMergeEnabled = true;
            for (int k = distinctMeasures.nextSetBit(0); k >= 0;
                k = distinctMeasures.nextSetBit(k + 1))
            {
                final AggStar.FactTable.Measure distinctMeasure =
                    aggStar.lookupMeasure(k);
                if (!isDistinctMergeEnabledForMeasure(
                    aggStar, distinctMeasure))
                {
                    distinctCountMergeEnabled = false;
                }
                BitKey rollableLevelBitKey =
                    distinctMeasure.getRollableLevelBitKey();
                if (combinedLevelBitKey == null) {
                    combinedLevelBitKey = rollableLevelBitKey;
                } else {
                    // TODO use '&=' to remove unnecessary copy
                    combinedLevelBitKey =
                        combinedLevelBitKey.and(rollableLevelBitKey);
                }
            }
            if (distinctCountMergeEnabled
                && distinctMergeConstrainedRollupEnabled
                && combinedLevelBitKey != null)
            {
                // Distinct merge aggregators (for example, ClickHouse
                // uniqCombinedMerge over AggregateFunction states) can be
                // safely rolled up from finer to coarser grains. Allow
                // rollup across all aggregate levels for these measures,
                // including constrained queries with extra slicer levels.
                combinedLevelBitKey = aggStar.getLevelBitKey().copy();
            }

            if (aggStar.hasForeignKeys()) {
/*
                    StringBuilder buf = new StringBuilder(256);
                    buf.append("");
                    buf.append(star.getFactTable().getAlias());
                    buf.append(Util.nl);
                    buf.append("foreign =");
                    buf.append(levelBitKey);
                    buf.append(Util.nl);
                    buf.append("measure =");
                    buf.append(measureBitKey);
                    buf.append(Util.nl);
                    buf.append("aggstar =");
                    buf.append(aggStar.getBitKey());
                    buf.append(Util.nl);
                    buf.append("distinct=");
                    buf.append(aggStar.getDistinctMeasureBitKey());
                    buf.append(Util.nl);
                    buf.append("AggStar=");
                    buf.append(aggStar.getFactTable().getName());
                    buf.append(Util.nl);
                    for (Iterator columnIter =
                            aggStar.getFactTable().getColumns().iterator();
                         columnIter.hasNext();) {
                        AggStar.Table.Column column =
                                (AggStar.Table.Column) columnIter.next();
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.nl);
                    }
System.out.println(buf.toString());
*/
                // This is a little pessimistic. If the measure is
                // 'count(distinct customer_id)' and one of the foreign keys is
                // 'customer_id' then it is OK to roll up.

                // Some of the measures in this query are distinct count.
                // Get all of the foreign key columns.
                // For each such measure, is it based upon a foreign key.
                // Are there any foreign keys left over. No, can use AggStar.
                BitKey fkBitKey = aggStar.getForeignKeyBitKey().copy();
                for (AggStar.FactTable.Measure measure
                    : aggStar.getFactTable().getMeasures())
                {
                    if (measure.isDistinct()) {
                        if (measureBitKey.get(measure.getBitPosition())) {
                            fkBitKey.clear(measure.getBitPosition());
                        }
                    }
                }
                if (!fkBitKey.isEmpty()) {
                    // there are foreign keys left so we can not use this
                    // AggStar.
                    continue;
                }
            }

            // We can use the expandedLevelBitKey here because
            // presence of parent level columns won't effect granularity,
            // so will still be an allowable agg match
            if (!aggStar.select(
                    expandedLevelBitKey, combinedLevelBitKey, measureBitKey))
            {
                continue;
            }

            if (!matchesRequestedLevels(
                aggStar,
                expandedLevelBitKey,
                constrainedLevelNames))
            {
                continue;
            }

            if (expandedLevelBitKey.isEmpty()
                && !(distinctCountMergeEnabled
                && distinctMergeConstrainedRollupEnabled))
            {
                // Legacy distinct-count aggregates (count distinct on raw
                // values) cannot be safely resolved at the all-level without
                // group-by levels. Merge-state aggregates can do this safely
                // when constrained/full-grain rollup mode is explicitly
                // enabled via DistinctCountMergeAllowConstrainedRollup.
                continue;
            }
            if (aggStarFilter != null && !aggStarFilter.allows(aggStar)) {
                continue;
            }
            rollup[0] = !aggStar.getLevelBitKey().equals(expandedLevelBitKey);
            return aggStar;
        }
        return null;
    }

    private static boolean matchesRequestedLevels(
        AggStar aggStar,
        BitKey expandedLevelBitKey,
        SortedMap<Integer, SortedSet<String>> constrainedLevelNames)
    {
        if (constrainedLevelNames == null || constrainedLevelNames.isEmpty()) {
            return true;
        }
        for (int bitPos = expandedLevelBitKey.nextSetBit(0);
             bitPos >= 0;
             bitPos = expandedLevelBitKey.nextSetBit(bitPos + 1))
        {
            final SortedSet<String> requestedLevelNames =
                constrainedLevelNames.get(bitPos);
            if (requestedLevelNames == null || requestedLevelNames.isEmpty()) {
                continue;
            }
            if (!aggStar.matchesRequestedLevels(bitPos, requestedLevelNames)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDistinctMergeConstrainedRollupEnabled() {
        final String value = MondrianProperties.instance()
            .getProperty(
                "mondrian.rolap.aggregates"
                + ".DistinctCountMergeAllowConstrainedRollup");
        if (value == null) {
            return false;
        }
        final String trimmed = value.trim();
        return "true".equalsIgnoreCase(trimmed);
    }

    private static boolean areSelectedDistinctMeasuresMergeEnabled(
        AggStar aggStar,
        BitKey measureBitKey)
    {
        final BitKey selectedDistinctMeasures =
            measureBitKey.and(aggStar.getDistinctMeasureBitKey());
        final BitSet bits = selectedDistinctMeasures.toBitSet();
        if (bits.isEmpty()) {
            return false;
        }
        for (int bit = bits.nextSetBit(0); bit >= 0;
             bit = bits.nextSetBit(bit + 1))
        {
            if (!isDistinctMergeEnabledForMeasure(
                aggStar, aggStar.lookupMeasure(bit)))
            {
                return false;
            }
        }
        return true;
    }

    private static boolean isDistinctMergeEnabledForMeasure(
        AggStar aggStar,
        AggStar.FactTable.Measure measure)
    {
        if (aggStar == null || measure == null) {
            return false;
        }
        return DistinctCountMergeSupport.isEnabledForDialect(
            aggStar.getStar().getSqlQueryDialect(),
            measure.getName());
    }

    /**
     * Sets the bits for parent columns.
     */
    private static BitKey expandLevelBitKey(
        RolapStar star, BitKey levelBitKey)
    {
        int bitPos = levelBitKey.nextSetBit(0);
        while (bitPos >= 0) {
            levelBitKey = setParentsBitKey(star, levelBitKey, bitPos);
            bitPos = levelBitKey.nextSetBit(bitPos + 1);
        }
        return levelBitKey;
    }

    private static BitKey setParentsBitKey(
        RolapStar star, BitKey levelBitKey, int bitPos)
    {
        RolapStar.Column parent = star.getColumn(bitPos).getParentColumn();
        if (parent == null) {
            return levelBitKey;
        }
        levelBitKey.set(parent.getBitPosition());
        return setParentsBitKey(star, levelBitKey, parent.getBitPosition());
    }

    public PinSet createPinSet() {
        return new PinSetImpl();
    }

    public void shutdown() {
        // Send a shutdown command and wait for it to return.
        cacheMgr.shutdown();
        // Now we can cleanup.
        for (SegmentCacheWorker worker : cacheMgr.segmentCacheWorkers) {
            worker.shutdown();
        }
    }

    /**
     * Implementation of {@link mondrian.rolap.RolapAggregationManager.PinSet}
     * using a {@link HashSet}.
     */
    public static class PinSetImpl
        extends HashSet<Segment>
        implements RolapAggregationManager.PinSet
    {
    }


    public SegmentCacheManager getCacheMgr(RolapConnection connection)  {
        if(connection == null || !MondrianProperties.instance().EnableSessionCaching.get()) {
            return cacheMgr;
        }
        else {
            final String sessionId = connection.getConnectInfo().get("sessionId");
            mondrian.server.Session session = mondrian.server.Session.getWithoutCheck(sessionId);

            if(session == null) {
                return cacheMgr;
            }

            return session.getOrCreateSegmentCacheManager(this.server);
        }
    }

    public void ResetCacheManager() {
        cacheMgr.shutdown();
        // Now we can cleanup.
        for (SegmentCacheWorker worker : cacheMgr.segmentCacheWorkers) {
            worker.shutdown();
        }
        this.cacheMgr = new SegmentCacheManager(server);
        Session.ResetAllCaches();
    }
}

// End AggregationManager.java
