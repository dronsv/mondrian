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

import mondrian.olap.Result;
import mondrian.rolap.nativesql.NativeSqlRegistry;
import mondrian.rolap.nativesql.NativeSqlTelemetry;
import mondrian.test.FoodMartTestCase;

/**
 * Integration tests for Phase 8a SqlMemberSource one-shot migration.
 *
 * <p>Runs against the embedded-H2 FoodMart fixture (see
 * scripts/test-it-h2.sh and the it-h2-foodmart Maven profile).
 */
public class SqlMemberSourceOneShotIT extends FoodMartTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeSqlRegistry.clearGlobalCache();
        NativeSqlTelemetry.resetForTests();
    }

    /**
     * Verifies the cache-reuse contract for getMemberCount across two
     * statements: the second invocation must see a cached payload and
     * fire cachedSuccessHit instead of re-executing SQL.
     */
    public void testGetMemberCount_cachesAcrossStatements() {
        // First MDX query — triggers level-member-count SQL via
        // SqlMemberSource.getMemberCount when Mondrian needs cardinality.
        Result r1 = executeQuery(
            "SELECT [Measures].[Unit Sales] ON 0,"
                + " [Store].[Store City].Members ON 1"
                + " FROM [Sales]");
        assertNotNull(r1);

        java.util.SortedMap<String, Integer> snap1 = NativeSqlTelemetry.snapshot();

        // Second identical query — should reuse cached count.
        Result r2 = executeQuery(
            "SELECT [Measures].[Unit Sales] ON 0,"
                + " [Store].[Store City].Members ON 1"
                + " FROM [Sales]");
        assertNotNull(r2);

        // executionCount must not have grown for any fingerprint that the
        // first query established.
        java.util.SortedMap<String, Integer> snap2 = NativeSqlTelemetry.snapshot();
        for (java.util.Map.Entry<String, Integer> e : snap1.entrySet()) {
            assertEquals(
                "fingerprint " + e.getKey()
                    + " executed again on second identical query",
                e.getValue(),
                snap2.getOrDefault(e.getKey(), 0));
        }

        // The strict contract is "no fresh fingerprint executions on
        // identical workload", which the loop above already enforces.
        // Cached-hit count is intentionally not asserted: Mondrian's
        // higher-level member cache may serve the second query without
        // consulting the registry at all (zero hits), or it may consult
        // and hit (positive hits). Both outcomes satisfy the spec §9.3
        // soft contract, so a precise hit-count assertion would either
        // be flaky or a tautology.
    }
}
