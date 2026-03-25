/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import junit.framework.TestCase;

public class AggregationManagerAggBypassReasonTest extends TestCase {

    public void testBypassWhenUseAggregatesDisabled() {
        assertEquals(
            "use_aggregates_disabled",
            AggregationManager.getAggBypassReason(false, 0, false, false));
    }

    public void testBypassWhenSubcubePredicatePresent() {
        assertEquals(
            "subcube_predicate_present",
            AggregationManager.getAggBypassReason(true, 1, true, false));
    }

    public void testNoBypassWhenSubcubePredicateNormalized() {
        assertNull(
            AggregationManager.getAggBypassReason(true, 0, true, true));
    }

    public void testBypassWhenCompoundPredicatesPresent() {
        assertEquals(
            "compound_predicates_present",
            AggregationManager.getAggBypassReason(true, 1, false, false));
    }

    public void testNoBypassWhenAggregatesEnabledAndNoCompoundPredicates() {
        assertNull(
            AggregationManager.getAggBypassReason(true, 0, false, false));
    }
}
