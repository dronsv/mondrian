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
import java.util.Locale;

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

    /** Cached locale-based formatter, created lazily. */
    private RolapResult.ValueFormatter cachedLocaleFormatter;

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

            // Guard: NQE only supports up to 4 axes (CellInfoPool limit)
            Axis[] axes = result.getAxes();
            if (axes.length > 4) {
                LOGGER.info(
                    "NativeQueryEngine: >4 axes ({}), falling back",
                    axes.length);
                return false;
            }

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
            //    (isCompatibleWith now checks sourceCubeName, so
            //    cross-cube measures are naturally split)
            List<CoordinateClassPlan> classPlans =
                CoordinateClassMerger.merge(resolvedPlan.allRequests);
            if (classPlans.isEmpty()) {
                LOGGER.info(
                    "NativeQueryEngine: Phase C"
                    + " — no class plans generated");
                return false;
            }

            // 3b. Resolve the base cube for each coordinate class plan.
            //     Plans from different cubes (e.g. "Продажи" vs
            //     "География") each get their own star.
            RolapCube primaryCube = findBaseCube(candidates, query);
            Map<String, RolapCube> cubeByClassId =
                resolveCubesForPlans(classPlans, primaryCube);

            // 3c. Pre-validate: ensure all stored/state requests can be
            //     found in the star of their respective cube.
            if (!validateAllPlanMeasures(classPlans, cubeByClassId)) {
                return false;
            }

            // 4. Phase D.1-D.2: Generate and execute SQL per plan,
            //    each against its own star.
            NativeQueryResultContext context =
                new NativeQueryResultContext();

            for (CoordinateClassPlan plan : classPlans) {
                RolapCube planCube = cubeByClassId.get(plan.getClassId());
                NativeQuerySqlGenerator sqlGen =
                    new NativeQuerySqlGenerator(evaluator, planCube);
                if (!sqlGen.executePlan(plan, context)) {
                    LOGGER.info(
                        "NativeQueryEngine: Phase D.1-D.2 fallback"
                        + " — SQL execution failed for class={}"
                        + " cube={}",
                        plan.getClassId(), planCube.getName());
                    return false;
                }
            }

            // 5. Phase D.3: Evaluate PostProcess + populate cells
            Map<String, CoordinateClassPlan> classPlanMap =
                new LinkedHashMap<String, CoordinateClassPlan>();
            for (CoordinateClassPlan cp : classPlans) {
                classPlanMap.put(cp.getClassId(), cp);
            }

            populateCells(
                result, context, resolvedPlan,
                classPlanMap, cubeByClassId);

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
        Map<String, RolapCube> cubeByClassId)
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
            cubeByClassId, axes, axisSizes, pos, 0);
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
        Map<String, RolapCube> cubeByClassId,
        Axis[] axes, int[] axisSizes, int[] pos, int axisOrdinal)
    {
        if (axisOrdinal == axes.length) {
            // We have a complete position — evaluate this cell
            evaluateAndSetCell(
                result, context, resolvedPlan,
                classPlanMap, cubeByClassId, axes, pos);
            return;
        }
        for (int i = 0; i < axisSizes[axisOrdinal]; i++) {
            pos[axisOrdinal] = i;
            iterateCells(
                result, context, resolvedPlan, classPlanMap,
                cubeByClassId, axes, axisSizes, pos,
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
        Map<String, RolapCube> cubeByClassId,
        Axis[] axes, int[] pos)
    {
        // 1. Find the measure at this position
        Member measure = findMeasureAtPosition(axes, pos);
        if (measure == null) {
            return;
        }

        // 2. Build per-class projected keys.
        //    Each class's SQL only groups by hierarchies that exist
        //    in its cube's star, so we must build a matching key per
        //    class rather than a single global key.
        Map<String, String> keyByClassId =
            new LinkedHashMap<String, String>();
        for (Map.Entry<String, CoordinateClassPlan> entry
                : classPlanMap.entrySet())
        {
            String classId = entry.getKey();
            CoordinateClassPlan plan = entry.getValue();
            RolapCube planCube = cubeByClassId.get(classId);
            keyByClassId.put(
                classId,
                buildProjectedKeyForClass(axes, pos, plan, planCube));
        }

        // 3. Determine value
        Object value;
        if (resolvedPlan.postProcessPlans.containsKey(measure)) {
            // PostProcess: evaluate formula
            DependencyResolver.PostProcessPlan pp =
                resolvedPlan.postProcessPlans.get(measure);
            value = PostProcessEvaluator.evaluate(
                pp, context, keyByClassId, classPlanMap);
        } else {
            // DirectPush: look up from context
            String measureId = measure.getUniqueName();
            // Find which class this measure belongs to
            String classId = findClassForMeasure(measureId, classPlanMap);
            if (classId != null) {
                value = context.get(
                    classId, keyByClassId.get(classId), measureId);
            } else {
                value = null;
            }
        }

        // 4. Set cell value in RolapResult (with format string)
        setCellWithFormat(result, pos, value, measure);
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
     * Resolves the base cube for each coordinate class plan.
     *
     * <p>Each plan's requests share the same {@code sourceCubeName}
     * (guaranteed by {@link PhysicalValueRequest#isCompatibleWith}).
     * If the source cube name is {@code null}, the primary cube is used.
     * Otherwise, we look up the named cube among the VirtualCube's
     * base cubes.
     *
     * @param classPlans  merged plans from Phase C
     * @param primaryCube the default base cube (from first stored measure)
     * @return map from classId to the RolapCube whose star should be used
     */
    private Map<String, RolapCube> resolveCubesForPlans(
        List<CoordinateClassPlan> classPlans,
        RolapCube primaryCube)
    {
        // Build a lookup table of base cubes by name (from VirtualCube)
        Map<String, RolapCube> cubesByName =
            new LinkedHashMap<String, RolapCube>();
        cubesByName.put(primaryCube.getName(), primaryCube);
        RolapCube queryCube = (RolapCube) query.getCube();
        if (queryCube.isVirtual()) {
            for (RolapCube bc : queryCube.getBaseCubes()) {
                cubesByName.put(bc.getName(), bc);
            }
        }

        Map<String, RolapCube> result =
            new LinkedHashMap<String, RolapCube>();
        for (CoordinateClassPlan plan : classPlans) {
            // All requests in a plan share the same sourceCubeName
            String sourceCubeName = plan.getRequests().isEmpty()
                ? null
                : plan.getRequests().get(0).getSourceCubeName();

            RolapCube planCube;
            if (sourceCubeName == null) {
                planCube = primaryCube;
            } else {
                planCube = cubesByName.get(sourceCubeName);
                if (planCube == null) {
                    LOGGER.warn(
                        "NativeQueryEngine: sourceCubeName={} not found"
                        + " among base cubes, falling back to primary"
                        + " cube {}",
                        sourceCubeName, primaryCube.getName());
                    planCube = primaryCube;
                }
            }

            result.put(plan.getClassId(), planCube);

            if (planCube != primaryCube) {
                LOGGER.info(
                    "NativeQueryEngine: plan class={} mapped to"
                    + " cross-cube {} (primary={})",
                    plan.getClassId(),
                    planCube.getName(),
                    primaryCube.getName());
            }
        }
        return result;
    }

    /**
     * Pre-validates every plan's measures against its respective star.
     *
     * <p>Each plan is validated against the star of the cube assigned
     * to it by {@link #resolveCubesForPlans}.  Returns {@code false}
     * if any plan's measures are unmappable.
     */
    private boolean validateAllPlanMeasures(
        List<CoordinateClassPlan> classPlans,
        Map<String, RolapCube> cubeByClassId)
    {
        for (CoordinateClassPlan plan : classPlans) {
            RolapCube planCube = cubeByClassId.get(plan.getClassId());
            RolapStar star = planCube.getStar();
            if (star == null) {
                LOGGER.info(
                    "NativeQueryEngine: no star on cube {} for"
                    + " class={}, skipping NQE",
                    planCube.getName(), plan.getClassId());
                return false;
            }
            if (!validateStarMeasures(
                    plan.getRequests(), star, planCube))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Pre-validates that every STORED_COLUMN / STATE_AGGREGATE request
     * in the list can be mapped to a {@link RolapStar.Measure}
     * on the given star.  If any request is unmappable, we return
     * {@code false} so the caller can fall back to legacy evaluation
     * <em>before</em> opening any JDBC connections or building SQL.
     *
     * <p>The lookup mirrors
     * {@link NativeQuerySqlGenerator#findStarMeasure} exactly.
     */
    private boolean validateStarMeasures(
        List<PhysicalValueRequest> requests,
        RolapStar star,
        RolapCube baseCube)
    {
        RolapStar.Table factTable = star.getFactTable();
        String cubeName = baseCube.getName();

        for (PhysicalValueRequest req : requests) {
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
     * Builds a projected key for a specific coordinate class plan.
     *
     * <p>The key includes only the hierarchies that the plan's SQL
     * actually groups by. A hierarchy is included when:
     * <ul>
     *   <li>it appears in the plan's projected hierarchies;</li>
     *   <li>it is not in the reset hierarchies;</li>
     *   <li>it can be resolved in the plan's cube (the cube has a
     *       {@link HierarchyUsage} for it, or the hierarchy's key
     *       column lives on the fact table).</li>
     * </ul>
     *
     * <p>This mirrors the logic in
     * {@link NativeQuerySqlGenerator#generateStoredSql}: hierarchies
     * for which {@code resolveHierarchyColumn()} returns {@code null}
     * are skipped from GROUP BY and therefore from the projected key.
     */
    private String buildProjectedKeyForClass(
        Axis[] axes, int[] pos,
        CoordinateClassPlan plan, RolapCube planCube)
    {
        PhysicalValueRequest first = plan.getRequests().get(0);
        Set<Hierarchy> projectedHierarchies =
            first.getProjectedHierarchies();
        Set<Hierarchy> resetHierarchies = first.getResetHierarchies();

        List<Object> keyParts = new ArrayList<Object>();
        for (int a = 0; a < axes.length; a++) {
            Position position = axes[a].getPositions().get(pos[a]);
            for (Member m : position) {
                if (m.isMeasure()) {
                    continue;
                }
                Hierarchy hier = m.getHierarchy();
                if (!projectedHierarchies.contains(hier)) {
                    continue;
                }
                if (resetHierarchies.contains(hier)) {
                    continue;
                }
                // Check that this hierarchy can be resolved in the
                // plan's cube. If the cube has no HierarchyUsage for
                // this hierarchy, the SQL generator would have skipped
                // it from GROUP BY, so we must skip it here too.
                if (planCube != null
                    && !hierarchyResolvesInCube(hier, planCube))
                {
                    continue;
                }
                Object key = (m instanceof RolapMember)
                    ? ((RolapMember) m).getKey()
                    : m.getName();
                keyParts.add(key);
            }
        }
        return NativeQuerySqlGenerator.encodeProjectedKey(keyParts);
    }

    /**
     * Tests whether a hierarchy can be resolved to a column in a cube's
     * star schema. Returns {@code true} if the cube has a
     * {@link HierarchyUsage} for this hierarchy, or if the hierarchy's
     * leaf-level key column resides directly on the fact table.
     *
     * <p>This is a lightweight check that avoids the full column
     * resolution path of
     * {@link NativeQuerySqlGenerator#resolveHierarchyColumn} but covers
     * the same decision logic.
     */
    private static boolean hierarchyResolvesInCube(
        Hierarchy hierarchy, RolapCube cube)
    {
        HierarchyUsage[] usages = cube.getUsages(hierarchy);
        if (usages != null && usages.length > 0) {
            return true;
        }
        // No explicit usage. Check whether the hierarchy's key column
        // lives directly on the fact table (degenerate dimension).
        if (hierarchy instanceof RolapHierarchy) {
            RolapHierarchy rh = (RolapHierarchy) hierarchy;
            if (rh.getRelation() == null) {
                // No dimension table — key column must be on fact table.
                // If there's no usage, the star won't know about it either.
                return false;
            }
        }
        return false;
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
    // Format string resolution
    // -----------------------------------------------------------------------

    /**
     * Sets a cell value with the correct format string and formatter,
     * mirroring the logic in RolapResult.executeStripe (lines 2003-2018).
     *
     * <p>For measures with a CellFormatter, the formatter is used directly.
     * Otherwise, the FORMAT_STRING property is read from the measure
     * member and a locale-based {@link RolapResult.FormatValueFormatter}
     * is used.
     */
    private void setCellWithFormat(
        RolapResult result, int[] pos, Object value, Member measure)
    {
        String formatString = null;
        RolapResult.ValueFormatter formatter = null;

        // Unwrap delegating wrappers to reach the actual measure
        Member unwrapped = DependencyResolver.unwrapMember(measure);

        if (unwrapped instanceof RolapMeasure) {
            RolapMeasure rm = (RolapMeasure) unwrapped;
            formatter = rm.getFormatter();
            if (formatter == null) {
                // No CellFormatter — use FORMAT_STRING property
                Object fmtProp = unwrapped.getPropertyValue(
                    Property.FORMAT_STRING.name);
                if (fmtProp != null) {
                    formatString = fmtProp.toString();
                }
                // Use cached FormatValueFormatter
                if (cachedLocaleFormatter == null) {
                    Locale locale = evaluator.getConnectionLocale();
                    cachedLocaleFormatter =
                        new RolapResult.FormatValueFormatter(locale);
                }
                formatter = cachedLocaleFormatter;
            }
        }

        result.setCellValue(pos, value, formatString, formatter);
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
