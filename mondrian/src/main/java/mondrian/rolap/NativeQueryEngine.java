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

import mondrian.olap.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * Query-wide SQL pushdown engine. Replaces Mondrian's cell-by-cell
 * batch-drain loop for eligible queries.
 *
 * <p>Pipeline: Classify (A) -> Resolve (B) -> Merge (C) -> Generate (D).
 * Falls back to legacy evaluator on any unsupported pattern.
 */
public class NativeQueryEngine {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeQueryEngine.class);

    private final Query query;
    private final RolapEvaluator evaluator;
    private final List<MeasureClassifier.Candidate> candidates;

    private NativeQueryEngine(
        Query query,
        RolapEvaluator evaluator,
        List<MeasureClassifier.Candidate> candidates)
    {
        this.query = query;
        this.evaluator = evaluator;
        this.candidates = candidates;
    }

    /**
     * Attempts to create a NativeQueryEngine for the given query.
     * Returns null if the query is not eligible (falls back to legacy).
     */
    public static NativeQueryEngine tryCreate(
        Query query,
        RolapEvaluator evaluator)
    {
        if (!MondrianProperties.instance().NativeQueryEngineEnable.get()) {
            return null;
        }

        final Set<Member> measures = query.getMeasuresMembers();
        if (measures == null || measures.isEmpty()) {
            return null;
        }

        // Phase A: classify
        final List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        if (candidates == null) {
            LOGGER.info(
                "NativeQueryEngine: fallback reason={}, measures={}",
                FallbackReason.UNSUPPORTED_MEASURE_PATTERN,
                measureNames(measures));
            return null;
        }

        // Phase 1: bail out immediately if any measure is NATIVE_TEMPLATE
        // (NQE can't resolve ${placeholder} templates — NativeSqlCalc handles those)
        for (MeasureClassifier.Candidate c : candidates) {
            if (c.candidateClass
                == MeasureClassifier.CandidateClass.DIRECT_PUSH_NATIVE)
            {
                return null;
            }
        }

        LOGGER.info(
            "NativeQueryEngine: eligible, measures={}",
            describeCandidates(candidates));

        return new NativeQueryEngine(query, evaluator, candidates);
    }

    /**
     * Executes the query-wide SQL plan and populates result cells.
     * Returns true if successful, false if fell back.
     */
    public boolean execute(RolapResult result) {
        try {
            // 1. Collect query hierarchies from axes
            Set<Hierarchy> queryHierarchies =
                collectQueryHierarchies(result);

            // 2. Phase B: Resolve dependencies
            DependencyResolver.ResolvedPlan resolvedPlan =
                DependencyResolver.resolve(candidates, queryHierarchies);
            if (resolvedPlan == null) {
                LOGGER.info(
                    "NativeQueryEngine: Phase B fallback"
                    + " — dependency resolution failed");
                return false;
            }

            // 3. Phase C: Merge into coordinate classes
            List<CoordinateClassPlan> classPlans =
                CoordinateClassMerger.merge(resolvedPlan.allRequests);
            if (classPlans.isEmpty()) {
                LOGGER.info(
                    "NativeQueryEngine: Phase C"
                    + " — no class plans generated");
                return false;
            }

            // 3b. Pre-validate: ensure all stored/state requests can be
            //     found in the star. Fails fast without JDBC overhead.
            RolapCube baseCube = findBaseCube(candidates, query);
            RolapStar star = baseCube.getStar();
            if (star == null) {
                LOGGER.info(
                    "NativeQueryEngine: no star on base cube {}"
                    + ", skipping",
                    baseCube.getName());
                return false;
            }
            if (!validateStarMeasures(
                    resolvedPlan.allRequests, star, baseCube))
            {
                return false;
            }

            // 4. Phase D.1-D.2: Generate and execute SQL
            NativeQueryResultContext context =
                new NativeQueryResultContext();
            NativeQuerySqlGenerator sqlGen =
                new NativeQuerySqlGenerator(evaluator, baseCube);

            if (!sqlGen.executeAll(classPlans, context)) {
                LOGGER.info(
                    "NativeQueryEngine: Phase D.1-D.2 fallback"
                    + " — SQL execution failed");
                return false;
            }

            // 5. Phase D.3: Evaluate PostProcess + populate cells
            Map<String, CoordinateClassPlan> classPlanMap =
                new LinkedHashMap<String, CoordinateClassPlan>();
            for (CoordinateClassPlan cp : classPlans) {
                classPlanMap.put(cp.getClassId(), cp);
            }

            populateCells(
                result, context, resolvedPlan,
                classPlanMap, queryHierarchies);

            LOGGER.info(
                "NativeQueryEngine: successfully populated {} cells",
                context.size());
            return true;

        } catch (Exception e) {
            LOGGER.warn(
                "NativeQueryEngine: execute() failed,"
                + " falling back to legacy", e);
            return false;
        }
    }

    // -----------------------------------------------------------------------
    // Query hierarchy collection
    // -----------------------------------------------------------------------

    /**
     * Collects the set of non-measure hierarchies appearing on query axes.
     */
    private Set<Hierarchy> collectQueryHierarchies(RolapResult result) {
        Set<Hierarchy> hierarchies = new LinkedHashSet<Hierarchy>();
        for (Axis axis : result.getAxes()) {
            List<Position> positions = axis.getPositions();
            if (positions.isEmpty()) {
                continue;
            }
            // Get hierarchies from the first position (all positions on
            // the same axis have the same hierarchy tuple shape)
            for (Member m : positions.get(0)) {
                if (!m.isMeasure()) {
                    hierarchies.add(m.getHierarchy());
                }
            }
        }
        return hierarchies;
    }

    // -----------------------------------------------------------------------
    // Cell population
    // -----------------------------------------------------------------------

    /**
     * Iterates all cell positions and populates values from context
     * or PostProcessEvaluator.
     */
    private void populateCells(
        RolapResult result,
        NativeQueryResultContext context,
        DependencyResolver.ResolvedPlan resolvedPlan,
        Map<String, CoordinateClassPlan> classPlanMap,
        Set<Hierarchy> queryHierarchies)
    {
        Axis[] axes = result.getAxes();
        if (axes.length == 0) {
            return;
        }

        // Compute sizes
        int[] axisSizes = new int[axes.length];
        for (int i = 0; i < axes.length; i++) {
            axisSizes[i] = axes[i].getPositions().size();
        }

        // Iterate all cell positions
        int[] pos = new int[axes.length];
        iterateCells(
            result, context, resolvedPlan, classPlanMap,
            queryHierarchies, axes, axisSizes, pos, 0);
    }

    /**
     * Recursive axis iterator. When all axis positions are fixed,
     * evaluates and sets the cell value.
     */
    private void iterateCells(
        RolapResult result,
        NativeQueryResultContext context,
        DependencyResolver.ResolvedPlan resolvedPlan,
        Map<String, CoordinateClassPlan> classPlanMap,
        Set<Hierarchy> queryHierarchies,
        Axis[] axes, int[] axisSizes, int[] pos, int axisOrdinal)
    {
        if (axisOrdinal == axes.length) {
            // We have a complete position — evaluate this cell
            evaluateAndSetCell(
                result, context, resolvedPlan,
                classPlanMap, queryHierarchies, axes, pos);
            return;
        }
        for (int i = 0; i < axisSizes[axisOrdinal]; i++) {
            pos[axisOrdinal] = i;
            iterateCells(
                result, context, resolvedPlan, classPlanMap,
                queryHierarchies, axes, axisSizes, pos,
                axisOrdinal + 1);
        }
    }

    /**
     * Evaluates the value for a single cell position and stores it
     * in the result.
     */
    private void evaluateAndSetCell(
        RolapResult result,
        NativeQueryResultContext context,
        DependencyResolver.ResolvedPlan resolvedPlan,
        Map<String, CoordinateClassPlan> classPlanMap,
        Set<Hierarchy> queryHierarchies,
        Axis[] axes, int[] pos)
    {
        // 1. Find the measure at this position
        Member measure = findMeasureAtPosition(axes, pos);
        if (measure == null) {
            return;
        }

        // 2. Build projected key from non-measure axis members
        String projectedKey =
            buildProjectedKeyFromAxes(axes, pos, queryHierarchies);

        // 3. Determine value
        Object value;
        if (resolvedPlan.postProcessPlans.containsKey(measure)) {
            // PostProcess: evaluate formula
            DependencyResolver.PostProcessPlan pp =
                resolvedPlan.postProcessPlans.get(measure);
            value = PostProcessEvaluator.evaluate(
                pp, context, projectedKey, classPlanMap);
        } else {
            // DirectPush: look up from context
            String measureId = measure.getUniqueName();
            // Find which class this measure belongs to
            String classId = findClassForMeasure(measureId, classPlanMap);
            if (classId != null) {
                value = context.get(classId, projectedKey, measureId);
            } else {
                value = null;
            }
        }

        // 4. Set cell value in RolapResult
        result.setCellValue(pos, value);
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    /**
     * Resolves the base cube that owns the star schema. For a regular
     * cube the query cube is returned directly. For a VirtualCube (whose
     * {@code getStar()} returns {@code null}) we unwrap the first stored
     * measure candidate and return its base cube — the one that actually
     * has a {@link RolapStar}.
     *
     * @param candidates classified measure candidates (Phase A output)
     * @param query      the MDX query
     * @return the {@link RolapCube} whose star should be used for SQL
     *         generation; never {@code null}
     */
    private static RolapCube findBaseCube(
        List<MeasureClassifier.Candidate> candidates,
        Query query)
    {
        for (MeasureClassifier.Candidate c : candidates) {
            if (c.candidateClass
                == MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED)
            {
                Member m = c.measure;
                // Unwrap cube-level wrappers (RolapCubeMember and
                // other DelegatingRolapMember subclasses).
                while (m instanceof DelegatingRolapMember) {
                    m = ((DelegatingRolapMember) m).member;
                }
                if (m instanceof RolapStoredMeasure) {
                    return ((RolapStoredMeasure) m).getCube();
                }
            }
        }
        // Fallback: query cube itself (may be a VirtualCube — callers
        // will detect the null star and fall back to legacy).
        return (RolapCube) query.getCube();
    }

    /**
     * Pre-validates that every STORED_COLUMN / STATE_AGGREGATE request
     * in the resolved plan can be mapped to a {@link RolapStar.Measure}
     * on the given star.  If any request is unmappable (e.g. the measure
     * belongs to a different base cube in a VirtualCube), we return
     * {@code false} so the caller can fall back to legacy evaluation
     * <em>before</em> opening any JDBC connections or building SQL.
     *
     * <p>The lookup mirrors
     * {@link NativeQuerySqlGenerator#findStarMeasure} exactly.
     */
    private boolean validateStarMeasures(
        List<PhysicalValueRequest> allRequests,
        RolapStar star,
        RolapCube baseCube)
    {
        RolapStar.Table factTable = star.getFactTable();
        String cubeName = baseCube.getName();

        for (PhysicalValueRequest req : allRequests) {
            PhysicalValueRequest.ExpressionProviderKind kind =
                req.getProviderKind();
            if (kind
                    != PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN
                && kind
                    != PhysicalValueRequest.ExpressionProviderKind.STATE_AGGREGATE)
            {
                continue;
            }

            String measureId = req.getPhysicalMeasureId();
            String simpleName =
                NativeQuerySqlGenerator.extractSimpleName(measureId);

            // 1. Try cube-qualified lookup
            RolapStar.Measure m =
                factTable.lookupMeasureByName(cubeName, simpleName);
            if (m != null) {
                continue;
            }

            // 2. Fallback: match by simple name across all cubes
            boolean found = false;
            for (RolapStar.Column col : factTable.getColumns()) {
                if (col instanceof RolapStar.Measure
                    && col.getName().equals(simpleName))
                {
                    found = true;
                    break;
                }
            }

            if (!found) {
                LOGGER.info(
                    "NativeQueryEngine: measure {} (simpleName={}) not in"
                    + " star of cube {}, skipping NQE",
                    measureId, simpleName, cubeName);
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the measure member at the given cell position by scanning
     * all axes.
     */
    private Member findMeasureAtPosition(Axis[] axes, int[] pos) {
        for (int a = 0; a < axes.length; a++) {
            Position position = axes[a].getPositions().get(pos[a]);
            for (Member m : position) {
                if (m.isMeasure()) {
                    return m;
                }
            }
        }
        return null;
    }

    /**
     * Builds a projected key string from the non-measure axis members
     * at the given cell position. The key format matches what
     * {@link NativeQuerySqlGenerator#buildProjectedKey} produces from
     * SQL result rows.
     */
    private String buildProjectedKeyFromAxes(
        Axis[] axes, int[] pos, Set<Hierarchy> queryHierarchies)
    {
        List<Object> keyParts = new ArrayList<Object>();
        for (int a = 0; a < axes.length; a++) {
            Position position = axes[a].getPositions().get(pos[a]);
            for (Member m : position) {
                if (!m.isMeasure()
                    && queryHierarchies.contains(m.getHierarchy()))
                {
                    // Use the member's key for the projected key
                    Object key = (m instanceof RolapMember)
                        ? ((RolapMember) m).getKey()
                        : m.getName();
                    keyParts.add(key);
                }
            }
        }
        return NativeQuerySqlGenerator.encodeProjectedKey(keyParts);
    }

    /**
     * Scans coordinate class plans to find which class contains
     * the given measure (by unique name).
     */
    private String findClassForMeasure(
        String measureId,
        Map<String, CoordinateClassPlan> classPlanMap)
    {
        for (Map.Entry<String, CoordinateClassPlan> entry
                : classPlanMap.entrySet())
        {
            if (entry.getValue().getMeasureIds().contains(measureId)) {
                return entry.getKey();
            }
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private static String measureNames(Set<Member> measures) {
        StringBuilder sb = new StringBuilder("[");
        int i = 0;
        for (Member m : measures) {
            if (i++ > 0) sb.append(", ");
            sb.append(m.getName());
        }
        return sb.append("]").toString();
    }

    private static String describeCandidates(
        List<MeasureClassifier.Candidate> candidates)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < candidates.size(); i++) {
            if (i > 0) sb.append(", ");
            MeasureClassifier.Candidate c = candidates.get(i);
            sb.append(c.measure.getName())
              .append("=").append(c.candidateClass);
        }
        return sb.append("]").toString();
    }
}

// End NativeQueryEngine.java
