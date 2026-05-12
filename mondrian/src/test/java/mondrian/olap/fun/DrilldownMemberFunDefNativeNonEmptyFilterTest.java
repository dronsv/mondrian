/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.rolap.NativeNonEmptyFilter;
import mondrian.rolap.NativeSqlConfig;
import mondrian.rolap.RolapCalculatedMember;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapMember;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DrilldownMemberFunDefNativeNonEmptyFilterTest {

    @Test public void testExpandedDrilldownUsesNativeFilterForStoredMeasures() {
        final boolean previous =
            MondrianProperties.instance().NativeNonEmptyFilterEnable.get();
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            final RolapEvaluator evaluator = mock(RolapEvaluator.class);
            final Query query = mock(Query.class);
            final Member storedMeasure = mock(Member.class);
            final Member axisMember = mock(Member.class);
            final Set<Member> measures =
                Collections.singleton(storedMeasure);
            final TupleList candidates = TupleCollections.createList(1, 1);
            final TupleList filtered = TupleCollections.createList(1, 1);
            candidates.addTuple(axisMember);
            filtered.addTuple(axisMember);

            when(evaluator.isNonEmpty()).thenReturn(true);
            when(evaluator.getQuery()).thenReturn(query);
            when(query.getMeasuresMembers()).thenReturn(measures);
            when(storedMeasure.isMeasure()).thenReturn(true);
            when(storedMeasure.isCalculated()).thenReturn(false);

            try (MockedStatic<NativeNonEmptyFilter> nativeFilter =
                     mockStatic(NativeNonEmptyFilter.class))
            {
                nativeFilter.when(() -> NativeNonEmptyFilter.tryPrune(
                    evaluator,
                    candidates,
                    measures))
                    .thenReturn(filtered);

                assertSame(
                    filtered,
                    DrilldownMemberFunDef.tryPruneExpandedDrilldownMember(
                        evaluator,
                        candidates));
                nativeFilter.verify(() -> NativeNonEmptyFilter.tryPrune(
                    evaluator,
                    candidates,
                    measures));
            }
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(
                previous);
        }
    }

    @Test public void testExpandedDrilldownUsesNativeFilterForNativeSqlMeasures() {
        final boolean previous =
            MondrianProperties.instance().NativeNonEmptyFilterEnable.get();
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            final RolapEvaluator evaluator = mock(RolapEvaluator.class);
            final Query query = mock(Query.class);
            final Member storedMeasure = mock(Member.class);
            final RolapMember nativeSqlMeasure = mock(RolapMember.class);
            final RolapCalculatedMember nativeSqlMember =
                mock(RolapCalculatedMember.class);
            final Member axisMember = mock(Member.class);
            final Set<Member> measures = new LinkedHashSet<Member>();
            measures.add(storedMeasure);
            measures.add(nativeSqlMeasure);
            final TupleList candidates = TupleCollections.createList(1, 1);
            final TupleList filtered = TupleCollections.createList(1, 1);
            candidates.addTuple(axisMember);
            filtered.addTuple(axisMember);

            when(evaluator.isNonEmpty()).thenReturn(true);
            when(evaluator.getQuery()).thenReturn(query);
            when(query.getMeasuresMembers()).thenReturn(measures);
            when(storedMeasure.isMeasure()).thenReturn(true);
            when(storedMeasure.isCalculated()).thenReturn(false);
            when(nativeSqlMeasure.isMeasure()).thenReturn(true);
            when(nativeSqlMeasure.isCalculated()).thenReturn(true);

            try (MockedStatic<NativeSqlConfig> nativeSqlConfig =
                     mockStatic(NativeSqlConfig.class);
                 MockedStatic<NativeNonEmptyFilter> nativeFilter =
                     mockStatic(NativeNonEmptyFilter.class))
            {
                nativeSqlConfig.when(() -> NativeSqlConfig.findNativeSqlMember(
                    nativeSqlMeasure))
                    .thenReturn(nativeSqlMember);
                nativeFilter.when(() -> NativeNonEmptyFilter.tryPrune(
                    evaluator,
                    candidates,
                    measures))
                    .thenReturn(filtered);

                assertSame(
                    filtered,
                    DrilldownMemberFunDef.tryPruneExpandedDrilldownMember(
                        evaluator,
                        candidates));
                nativeFilter.verify(() -> NativeNonEmptyFilter.tryPrune(
                    evaluator,
                    candidates,
                    measures));
            }
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(
                previous);
        }
    }

    @Test public void testExpandedDrilldownSkipsNativeFilterForCalcMeasures() {
        final boolean previous =
            MondrianProperties.instance().NativeNonEmptyFilterEnable.get();
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            final RolapEvaluator evaluator = mock(RolapEvaluator.class);
            final Query query = mock(Query.class);
            final Member calcMeasure = mock(Member.class);
            final Member axisMember = mock(Member.class);
            final Set<Member> measures = Collections.singleton(calcMeasure);
            final TupleList candidates = TupleCollections.createList(1, 1);
            candidates.addTuple(axisMember);

            when(evaluator.isNonEmpty()).thenReturn(true);
            when(evaluator.getQuery()).thenReturn(query);
            when(query.getMeasuresMembers()).thenReturn(measures);
            when(calcMeasure.isMeasure()).thenReturn(true);
            when(calcMeasure.isCalculated()).thenReturn(true);

            try (MockedStatic<NativeNonEmptyFilter> nativeFilter =
                     mockStatic(NativeNonEmptyFilter.class))
            {
                assertSame(
                    candidates,
                    DrilldownMemberFunDef.tryPruneExpandedDrilldownMember(
                        evaluator,
                        candidates));
                nativeFilter.verifyNoInteractions();
            }
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(
                previous);
        }
    }
}
