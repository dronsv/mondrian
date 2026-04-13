/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.rolap.aggmatcher.AggStar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Resolves the best physical source table (fact or aggregate) for an NQE
 * {@link CoordinateClassPlan}.
 *
 * <p>Mirrors the {@link mondrian.rolap.agg.AggregationManager#findAgg} logic
 * for AggStar selection but returns NQE-native metadata via
 * {@link ResolvedTable} (either {@link FactResolvedTable} or
 * {@link AggResolvedTable}).
 *
 * <p>The result is a {@link ResolvedSourcePlan} that pairs the resolved table
 * with the original plan. An {@code Unresolved} sentinel is returned when no
 * star can be found for the cube.
 */
public class NqeTableStrategy {

    private static final Logger LOGGER =
        LogManager.getLogger(NqeTableStrategy.class);

    // -----------------------------------------------------------------------
    // Result types
    // -----------------------------------------------------------------------

    /**
     * Result of resolving a {@link CoordinateClassPlan} to a physical source.
     */
    public static abstract class ResolvedSourcePlan {
        private final CoordinateClassPlan plan;

        protected ResolvedSourcePlan(CoordinateClassPlan plan) {
            this.plan = plan;
        }

        public CoordinateClassPlan getPlan() { return plan; }

        public abstract boolean isResolved();

        /** Returns the resolved table, or {@code null} for Unresolved. */
        public abstract ResolvedTable getTable();
    }

    /**
     * Successfully resolved to a physical source table.
     */
    public static class SingleSourcePlan extends ResolvedSourcePlan {
        private final ResolvedTable table;

        public SingleSourcePlan(CoordinateClassPlan plan, ResolvedTable table) {
            super(plan);
            this.table = table;
        }

        @Override public boolean isResolved() { return true; }
        @Override public ResolvedTable getTable() { return table; }
    }

    /**
     * Could not resolve to a physical source (no star, no cube, etc.).
     */
    public static class Unresolved extends ResolvedSourcePlan {
        private final String reason;

        public Unresolved(CoordinateClassPlan plan, String reason) {
            super(plan);
            this.reason = reason;
        }

        @Override public boolean isResolved() { return false; }
        @Override public ResolvedTable getTable() { return null; }

        public String getReason() { return reason; }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Resolves the best physical source for the given plan.
     *
     * @param baseCube  the base cube that owns the measures/hierarchies
     * @param plan      the coordinate class plan to resolve
     * @param evaluator the current evaluator (provides slicer context)
     * @return a {@link ResolvedSourcePlan} — either a
     *         {@link SingleSourcePlan} or an {@link Unresolved}
     */
    public ResolvedSourcePlan resolve(
        RolapCube baseCube,
        CoordinateClassPlan plan,
        RolapEvaluator evaluator)
    {
        // 1. Get RolapStar from baseCube
        RolapStar star = baseCube.getStar();
        if (star == null) {
            LOGGER.debug(
                "NqeTableStrategy: no star for cube={}", baseCube.getName());
            return new Unresolved(plan, "no star for cube " + baseCube.getName());
        }

        // 2. Build levelBitKey from projected hierarchies + evaluator slicer
        BitKey levelBitKey = buildLevelBitKey(star, plan, evaluator);

        // 3. Build measureBitKey from STORED_COLUMN / STATE_AGGREGATE requests
        BitKey measureBitKey = buildMeasureBitKey(
            star, baseCube.getName(), plan);

        LOGGER.debug(
            "NqeTableStrategy: classId={}, levelBitKey={}, measureBitKey={}",
            plan.getClassId(), levelBitKey, measureBitKey);

        // 4. If no agg stars, return fact table directly
        List<AggStar> aggStars = star.getAggStars();
        if (aggStars == null || aggStars.isEmpty()) {
            LOGGER.debug("NqeTableStrategy: no agg stars, using fact table");
            return new SingleSourcePlan(
                plan, new FactResolvedTable(star, baseCube));
        }

        // 5. Iterate AggStars (smallest first) — mirror findAgg logic
        BitKey fullBitKey = levelBitKey.or(measureBitKey);

        for (AggStar aggStar : aggStars) {
            // superSetMatch: agg must cover all level + measure bits
            if (!aggStar.superSetMatch(fullBitKey)) {
                LOGGER.debug(
                    "NqeTableStrategy: aggStar {} failed superSetMatch",
                    aggStar.getFactTable().getName());
                continue;
            }

            // Distinct-count compatibility
            boolean isDistinct = measureBitKey.intersects(
                aggStar.getDistinctMeasureBitKey());

            if (isDistinct && aggStar.hasIgnoredColumns()) {
                if (!areSelectedDistinctMeasuresMergeEnabled(
                    aggStar, measureBitKey))
                {
                    LOGGER.debug(
                        "NqeTableStrategy: aggStar {} skipped — distinct-count"
                        + " with ignored columns, merge not enabled",
                        aggStar.getFactTable().getName());
                    continue;
                }
            }

            // Coverage validation: every level bit has an actual agg column
            if (!validateLevelCoverage(aggStar, levelBitKey)) {
                LOGGER.debug(
                    "NqeTableStrategy: aggStar {} failed level coverage",
                    aggStar.getFactTable().getName());
                continue;
            }

            // Coverage validation: every measure bit has an actual agg Measure
            if (!validateMeasureCoverage(aggStar, measureBitKey)) {
                LOGGER.debug(
                    "NqeTableStrategy: aggStar {} failed measure coverage",
                    aggStar.getFactTable().getName());
                continue;
            }

            // Compute rollup flag
            boolean rollup = !aggStar.isFullyCollapsed()
                || aggStar.hasIgnoredColumns()
                || levelBitKey.isEmpty()
                || !aggStar.getLevelBitKey().equals(levelBitKey);

            LOGGER.debug(
                "NqeTableStrategy: selected aggStar={} (size={},"
                + " rollup={})",
                aggStar.getFactTable().getName(),
                aggStar.getSize(),
                rollup);

            return new SingleSourcePlan(
                plan, new AggResolvedTable(aggStar, rollup));
        }

        // 6. No match — fall back to fact table
        LOGGER.debug(
            "NqeTableStrategy: no covering agg star, using fact table");
        return new SingleSourcePlan(
            plan, new FactResolvedTable(star, baseCube));
    }

    // -----------------------------------------------------------------------
    // BitKey builders
    // -----------------------------------------------------------------------

    /**
     * Builds the level bit key from projected hierarchies (minus reset) in
     * all requests of the plan, plus any slicer members from the evaluator
     * that constrain hierarchies not already in the projected/reset sets.
     */
    BitKey buildLevelBitKey(
        RolapStar star,
        CoordinateClassPlan plan,
        RolapEvaluator evaluator)
    {
        BitKey levelBitKey = BitKey.Factory.makeBitKey(
            star.getColumnCount());

        // Collect all projected and reset hierarchies across all requests
        Set<Hierarchy> projectedHierarchies = new LinkedHashSet<Hierarchy>();
        Set<Hierarchy> resetHierarchies = new LinkedHashSet<Hierarchy>();
        for (PhysicalValueRequest req : plan.getRequests()) {
            projectedHierarchies.addAll(req.getProjectedHierarchies());
            resetHierarchies.addAll(req.getResetHierarchies());
        }

        // Set bits for projected hierarchies (excluding reset)
        for (Hierarchy hier : projectedHierarchies) {
            if (resetHierarchies.contains(hier)) {
                continue;
            }
            setLevelBit(star, hier, levelBitKey);
        }

        // Add bits for evaluator slicer members that constrain hierarchies
        // not already in projected or reset sets
        if (evaluator != null) {
            for (Member member : evaluator.getNonAllMembers()) {
                Hierarchy memberHier = member.getHierarchy();
                if (!projectedHierarchies.contains(memberHier)
                    && !resetHierarchies.contains(memberHier))
                {
                    setLevelBit(star, memberHier, levelBitKey);
                }
            }
        }

        return levelBitKey;
    }

    /**
     * Sets the bit in {@code levelBitKey} for the leaf non-All level of the
     * given hierarchy by looking up the star column via its key expression.
     */
    private void setLevelBit(
        RolapStar star,
        Hierarchy hierarchy,
        BitKey levelBitKey)
    {
        if (!(hierarchy instanceof RolapHierarchy)) {
            return;
        }
        RolapHierarchy rolapHier = (RolapHierarchy) hierarchy;
        Level[] levels = rolapHier.getLevels();

        // Find leaf non-All level
        RolapLevel leafLevel = null;
        for (int i = levels.length - 1; i >= 0; i--) {
            if (!levels[i].isAll()) {
                leafLevel = (RolapLevel) levels[i];
                break;
            }
        }
        if (leafLevel == null) {
            return;
        }

        MondrianDef.Expression keyExp = leafLevel.getKeyExp();
        if (!(keyExp instanceof MondrianDef.Column)) {
            return;
        }
        MondrianDef.Column keyCol = (MondrianDef.Column) keyExp;
        RolapStar.Column starCol =
            star.lookupColumn(keyCol.table, keyCol.name);
        if (starCol != null) {
            levelBitKey.set(starCol.getBitPosition());
        }
    }

    /**
     * Builds the measure bit key from STORED_COLUMN and STATE_AGGREGATE
     * requests in the plan.
     */
    BitKey buildMeasureBitKey(
        RolapStar star,
        String cubeName,
        CoordinateClassPlan plan)
    {
        BitKey measureBitKey = BitKey.Factory.makeBitKey(
            star.getColumnCount());

        RolapStar.Table factTable = star.getFactTable();

        for (PhysicalValueRequest req : plan.getRequests()) {
            PhysicalValueRequest.ExpressionProviderKind kind =
                req.getProviderKind();
            if (kind != PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN
                && kind != PhysicalValueRequest.ExpressionProviderKind.STATE_AGGREGATE)
            {
                continue;
            }

            String simpleName =
                FactResolvedTable.extractSimpleName(req.getPhysicalMeasureId());
            if (simpleName == null) {
                continue;
            }

            // Try cube-qualified lookup first
            RolapStar.Measure starMeasure =
                factTable.lookupMeasureByName(cubeName, simpleName);
            if (starMeasure == null) {
                // Fallback: match by simple name across all cubes
                for (RolapStar.Column col : factTable.getColumns()) {
                    if (col instanceof RolapStar.Measure
                        && col.getName().equals(simpleName))
                    {
                        starMeasure = (RolapStar.Measure) col;
                        break;
                    }
                }
            }
            if (starMeasure != null) {
                measureBitKey.set(starMeasure.getBitPosition());
            }
        }

        return measureBitKey;
    }

    // -----------------------------------------------------------------------
    // Coverage validation
    // -----------------------------------------------------------------------

    /**
     * Validates that every set bit in levelBitKey has a corresponding column
     * in the agg star.
     */
    private boolean validateLevelCoverage(AggStar aggStar, BitKey levelBitKey) {
        for (int bit = levelBitKey.nextSetBit(0);
             bit >= 0;
             bit = levelBitKey.nextSetBit(bit + 1))
        {
            if (aggStar.lookupColumn(bit) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validates that every set bit in measureBitKey maps to an
     * {@link AggStar.FactTable.Measure} in the agg star.
     */
    private boolean validateMeasureCoverage(
        AggStar aggStar,
        BitKey measureBitKey)
    {
        for (int bit = measureBitKey.nextSetBit(0);
             bit >= 0;
             bit = measureBitKey.nextSetBit(bit + 1))
        {
            AggStar.Table.Column col = aggStar.lookupColumn(bit);
            if (!(col instanceof AggStar.FactTable.Measure)) {
                return false;
            }
        }
        return true;
    }

    // -----------------------------------------------------------------------
    // Distinct-count merge checks (replicated from AggregationManager)
    // -----------------------------------------------------------------------

    /**
     * Checks whether all selected distinct-count measures in the given
     * measureBitKey are merge-enabled for the agg star's dialect.
     *
     * <p>Replicates
     * {@code AggregationManager.areSelectedDistinctMeasuresMergeEnabled}
     * which is private.
     */
    static boolean areSelectedDistinctMeasuresMergeEnabled(
        AggStar aggStar,
        BitKey measureBitKey)
    {
        BitKey selectedDistinctMeasures =
            measureBitKey.and(aggStar.getDistinctMeasureBitKey());
        java.util.BitSet bits = selectedDistinctMeasures.toBitSet();
        if (bits.isEmpty()) {
            return false;
        }
        for (int bit = bits.nextSetBit(0); bit >= 0;
             bit = bits.nextSetBit(bit + 1))
        {
            AggStar.FactTable.Measure measure = aggStar.lookupMeasure(bit);
            if (!isDistinctMergeEnabledForMeasure(aggStar, measure)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether a specific distinct-count measure is merge-enabled.
     *
     * <p>Replicates
     * {@code AggregationManager.isDistinctMergeEnabledForMeasure}
     * which is private.
     */
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
}
