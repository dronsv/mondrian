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
import mondrian.olap.Member;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.PredicateCanonicalizer;
import mondrian.server.Execution;
import mondrian.server.StatementImpl;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Cost and shape of the NQE prefetch read guard
 * (dronsv/mondrian#49 review).
 *
 * <p>The guard runs on the cell-read hot path, where the legacy
 * {@code CellRequestKey} never did any work, so what it costs per read
 * is part of its contract and is pinned here.
 *
 * <p>Carried over from PR #49's {@code NqePrefetchContextTest} under a
 * distinct name: PR #42 landed a same-named class of real-SQL #103
 * regressions, and both sets are kept.
 */
class NqePrefetchGuardCostTest {

    private static final String MEASURE = "[Measures].[Quantity]";

    /** The restriction a read with no subselect canonicalizes to. */
    private static final String NO_RESTRICTION = "";

    // -----------------------------------------------------------------
    // Hot-path cost
    // -----------------------------------------------------------------

    @Test void readCanonicalizesItsRestrictionOncePerRequest() {
        final Map<String, CoordinateClassPlan> plans = new LinkedHashMap<>();
        final Map<String, String> restrictions = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            plans.put("c" + i, plan("c" + i));
            restrictions.put("c" + i, "some-other-restriction");
        }
        final FastBatchingCellReader reader =
            reader(new NativeQueryResultContext(), plans, restrictions);
        final CellRequest request = cellRequest();

        final long before = PredicateCanonicalizer.canonicalizationCount();
        assertNull(reader.lookupFromPrefetch(evaluator(), request));
        assertEquals(
            1,
            PredicateCanonicalizer.canonicalizationCount() - before,
            "8 candidate plans must not cost 8 predicate-tree walks");

        final long afterFirstRead =
            PredicateCanonicalizer.canonicalizationCount();
        assertNull(reader.lookupFromPrefetch(evaluator(), request));
        assertEquals(
            afterFirstRead,
            PredicateCanonicalizer.canonicalizationCount(),
            "a request canonicalizes its own restriction at most once");
    }

    // -----------------------------------------------------------------
    // The guard itself
    // -----------------------------------------------------------------

    @Test void readWithTheSameRestrictionIsAnswered() {
        final NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("c0", projectedKey(), MEASURE, 42d);
        final FastBatchingCellReader reader = reader(
            context,
            Map.of("c0", plan("c0")),
            Map.of("c0", NO_RESTRICTION));

        assertEquals(
            42d, reader.lookupFromPrefetch(evaluator(), cellRequest()));
    }

    @Test void readWithADifferentRestrictionIsDeclined() {
        final NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("c0", projectedKey(), MEASURE, 42d);
        final FastBatchingCellReader reader = reader(
            context,
            Map.of("c0", plan("c0")),
            Map.of("c0", "restricted-to-one-product"));

        assertNull(reader.lookupFromPrefetch(evaluator(), cellRequest()));
    }

    @Test void planWithNoRecordedRestrictionIsNeverRead() {
        // A plan the engine declined to describe — unknown cube, or a
        // mixed-reset plan — must not answer reads by default.
        final NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("c0", projectedKey(), MEASURE, 42d);
        final FastBatchingCellReader reader = reader(
            context,
            Map.of("c0", plan("c0")),
            Collections.<String, String>emptyMap());

        assertNull(reader.lookupFromPrefetch(evaluator(), cellRequest()));
    }

    // -----------------------------------------------------------------
    // Mixed-reset plans
    // -----------------------------------------------------------------

    @Test void mergedPlansAreUniformAndMixedResetPlansAreNot() {
        assertTrue(
            plan("c0").hasUniformResetHierarchies(),
            "CoordinateClassMerger only groups equal reset sets");

        final Hierarchy reset = mock(Hierarchy.class);
        when(reset.getUniqueName()).thenReturn("[Product]");
        final CoordinateClassPlan mixed = new CoordinateClassPlan(
            "mixed",
            List.of(
                request(Collections.<Hierarchy>emptySet()),
                request(Set.of(reset))));

        assertFalse(
            mixed.hasUniformResetHierarchies(),
            "each request is rendered in its own scope, so no single"
            + " subselect restriction describes the plan's SQL");
    }

    // -----------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------

    private static FastBatchingCellReader reader(
        NativeQueryResultContext context,
        Map<String, CoordinateClassPlan> plans,
        Map<String, String> restrictions)
    {
        final StatementImpl statement = mock(StatementImpl.class);
        final RolapConnection connection = mock(RolapConnection.class);
        when(statement.getMondrianConnection()).thenReturn(connection);
        final AggregationManager aggMgr = mock(AggregationManager.class);
        final FastBatchingCellReader reader = new FastBatchingCellReader(
            new Execution(statement, 0), mock(RolapCube.class), aggMgr);
        // The prefetch members / projected levels come from #35's
        // context guard; a single-measure array is the "nothing drifted"
        // case, which leaves this test measuring only the #49 guard.
        reader.setPrefetchContext(
            context,
            plans,
            new Member[] {measureMember()},
            Collections.<Hierarchy, mondrian.olap.Level>emptyMap(),
            restrictions);
        return reader;
    }

    /** A read of {@link #MEASURE} with no subselect restriction. */
    private static CellRequest cellRequest() {
        final RolapStar star = mock(RolapStar.class);
        when(star.getColumnCount()).thenReturn(1);
        final RolapStar.Measure measure = mock(RolapStar.Measure.class);
        when(measure.getStar()).thenReturn(star);
        return new CellRequest(measure, false, false);
    }

    private static Member measureMember() {
        final Member measure = mock(Member.class);
        when(measure.getUniqueName()).thenReturn(MEASURE);
        return measure;
    }

    private static RolapEvaluator evaluator() {
        // The member is built before when(...) opens, so its own stubbing
        // does not land inside the evaluator's (Mockito UnfinishedStubbing).
        final Member[] members = new Member[] {measureMember()};
        final RolapEvaluator evaluator = mock(RolapEvaluator.class);
        when(evaluator.getMembers()).thenReturn(members);
        return evaluator;
    }

    private static CoordinateClassPlan plan(String classId) {
        return new CoordinateClassPlan(
            classId,
            List.of(request(Collections.<Hierarchy>emptySet())));
    }

    private static PhysicalValueRequest request(Set<Hierarchy> resets) {
        return new PhysicalValueRequest(
            MEASURE,
            Collections.<Hierarchy>emptySet(),
            resets,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);
    }

    private static String projectedKey() {
        return NativeQuerySqlGenerator.encodeProjectedKey(
            Collections.emptyList());
    }
}
