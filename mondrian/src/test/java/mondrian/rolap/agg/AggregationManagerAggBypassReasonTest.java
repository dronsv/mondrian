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

import mondrian.rolap.StarPredicate;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationManagerAggBypassReasonTest {

    @Test public void testBypassWhenUseAggregatesDisabled() {
        assertEquals(
            "use_aggregates_disabled",
            AggregationManager.getAggBypassReason(false, 0, false, false));
    }

    @Test public void testBypassWhenSubcubePredicatePresent() {
        assertEquals(
            "subcube_predicate_present",
            AggregationManager.getAggBypassReason(true, 1, true, false));
    }

    @Test public void testNoBypassWhenSubcubePredicateNormalized() {
        assertNull(
            AggregationManager.getAggBypassReason(true, 0, true, true));
    }

    @Test public void testBypassWhenCompoundPredicatesPresent() {
        assertEquals(
            "compound_predicates_present",
            AggregationManager.getAggBypassReason(true, 1, false, false));
    }

    @Test public void testNoBypassWhenAggregatesEnabledAndNoCompoundPredicates() {
        assertNull(
            AggregationManager.getAggBypassReason(true, 0, false, false));
    }

    @Test public void testStripSharedSubcubePredicateFromCompoundPredicates() {
        final StarPredicate sharedSubcube = mock(StarPredicate.class);
        final StarPredicate otherPredicate = mock(StarPredicate.class);
        when(sharedSubcube.equalConstraint(sharedSubcube)).thenReturn(true);
        when(otherPredicate.equalConstraint(sharedSubcube)).thenReturn(false);

        assertEquals(
            Arrays.asList(otherPredicate),
            AggregationManager.stripSharedSubcubePredicate(
                Arrays.asList(sharedSubcube, otherPredicate),
                sharedSubcube));
    }
}
