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
import mondrian.olap.MondrianProperties;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapStar;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationManagerDistinctMergeFindAggTest extends TestCase {
    private static final String DISTINCT_MERGE_PROP =
        "mondrian.rolap.aggregates.DistinctCountMergeFunction";
    private static final String DISTINCT_MERGE_MODE_PROP =
        "mondrian.rolap.aggregates.DistinctCountMergeMode";
    private static final String DISTINCT_MERGE_MAP_PROP =
        "mondrian.rolap.aggregates.DistinctCountMergeFunctionMap";
    private static final String DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP =
        "mondrian.rolap.aggregates.DistinctCountMergeAllowConstrainedRollup";

    public void testFindAggSkipsFilteredCandidateAndUsesNextFeasibleAgg() {
        final RolapStar star = mock(RolapStar.class);
        final AggStar first = mock(AggStar.class);
        final AggStar second = mock(AggStar.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final BitKey levelBitKey = bitKey(8, 1);
        final BitKey measureBitKey = bitKey(8, 6);

        when(column.getParentColumn()).thenReturn(null);
        when(star.getColumn(1)).thenReturn(column);
        when(star.getAggStars()).thenReturn(Arrays.asList(first, second));
        stubSimpleAdditiveAgg(first, levelBitKey, measureBitKey);
        stubSimpleAdditiveAgg(second, levelBitKey, measureBitKey);

        final AggStar found = AggregationManager.findAgg(
            star,
            levelBitKey,
            measureBitKey,
            new boolean[] {false},
            Collections.<Integer, SortedSet<String>>emptySortedMap(),
            new AggregationManager.AggStarFilter() {
                public boolean allows(AggStar aggStar) {
                    return aggStar == second;
                }
            });

        assertSame(second, found);
    }

    public void testFindAggDistinctMergeAllowsRollupWithExtraSlicerLevels() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.setProperty(DISTINCT_MERGE_PROP, "uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.remove(DISTINCT_MERGE_MAP_PROP);
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "true");
        try {
            final Fixture f = fixture(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertSame(f.aggStar, found);
            assertTrue(rollup[0]);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggDistinctWithoutMergeKeepsLegacyCoreRestriction() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        properties.remove(DISTINCT_MERGE_PROP);
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.remove(DISTINCT_MERGE_MAP_PROP);
        try {
            final Fixture f = fixture(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertNull(found);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
        }
    }

    public void testFindAggDistinctMergeDisabledFlagKeepsLegacyCoreRestriction() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.setProperty(DISTINCT_MERGE_PROP, "uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.remove(DISTINCT_MERGE_MAP_PROP);
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "false");
        try {
            final Fixture f = fixture(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertNull(found);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggDistinctMergeAllowsAllLevelWithoutGroupBy() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.setProperty(DISTINCT_MERGE_PROP, "uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.remove(DISTINCT_MERGE_MAP_PROP);
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "true");
        try {
            final Fixture f = fixtureAllLevelDistinctTwoMeasures(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertSame(f.aggStar, found);
            assertFalse(rollup[0]);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggDistinctMergeAllLevelDisabledFlagKeepsLegacyFallback() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.setProperty(DISTINCT_MERGE_PROP, "uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.remove(DISTINCT_MERGE_MAP_PROP);
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "false");
        try {
            final Fixture f = fixtureAllLevelDistinctTwoMeasures(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertNull(found);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggDistinctMergeAutoUnsupportedDialectKeepsLegacyFallback() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.setProperty(DISTINCT_MERGE_PROP, "uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.remove(DISTINCT_MERGE_MAP_PROP);
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "true");
        try {
            final Fixture f = fixture(true, false);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertNull(found);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggDistinctMergeMapRequiresAllSelectedDistinctMeasures() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.remove(DISTINCT_MERGE_PROP);
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.setProperty(
            DISTINCT_MERGE_MAP_PROP,
            "m6=uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "true");
        try {
            final Fixture f = fixtureAllLevelDistinctTwoMeasures(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertNull(found);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggDistinctMergeMapWorksForMixedDistinctMeasures() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String previous = properties.getProperty(DISTINCT_MERGE_PROP);
        final String previousMode =
            properties.getProperty(DISTINCT_MERGE_MODE_PROP);
        final String previousMap =
            properties.getProperty(DISTINCT_MERGE_MAP_PROP);
        final String previousConstrainedRollup =
            properties.getProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP);
        properties.remove(DISTINCT_MERGE_PROP);
        properties.setProperty(DISTINCT_MERGE_MODE_PROP, "auto");
        properties.setProperty(
            DISTINCT_MERGE_MAP_PROP,
            "m6=uniqCombinedMerge,m7=uniqCombinedMerge");
        properties.setProperty(DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP, "true");
        try {
            final Fixture f = fixtureAllLevelDistinctTwoMeasures(true);
            final boolean[] rollup = {false};

            final AggStar found = AggregationManager.findAgg(
                f.star,
                f.queryLevelBitKey,
                f.measureBitKey,
                rollup);

            assertSame(f.aggStar, found);
            assertFalse(rollup[0]);
        } finally {
            restoreProperty(properties, previous);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MODE_PROP,
                previousMode);
            restoreProperty(
                properties,
                DISTINCT_MERGE_MAP_PROP,
                previousMap);
            restoreProperty(
                properties,
                DISTINCT_MERGE_CONSTRAINED_ROLLUP_PROP,
                previousConstrainedRollup);
        }
    }

    public void testFindAggRejectsAggregateWhenRequestedLevelIdentityDiffers() {
        final Fixture f = fixtureForLevelIdentity();
        final boolean[] rollup = {false};
        final SortedMap<Integer, SortedSet<String>> requestedLevels =
            requestedLevels(1, "[Flat].[Manufacturer]");

        when(f.aggStar.matchesRequestedLevels(anyInt(), anyCollection()))
            .thenReturn(false);

        final AggStar found = AggregationManager.findAgg(
            f.star,
            f.queryLevelBitKey,
            f.measureBitKey,
            rollup,
            requestedLevels);

        assertNull(found);
    }

    public void testFindAggKeepsAggregateWhenRequestedLevelIdentityMatches() {
        final Fixture f = fixtureForLevelIdentity();
        final boolean[] rollup = {false};
        final SortedMap<Integer, SortedSet<String>> requestedLevels =
            requestedLevels(1, "[Flat].[Manufacturer]");

        when(f.aggStar.matchesRequestedLevels(anyInt(), anyCollection()))
            .thenReturn(true);

        final AggStar found = AggregationManager.findAgg(
            f.star,
            f.queryLevelBitKey,
            f.measureBitKey,
            rollup,
            requestedLevels);

        assertSame(f.aggStar, found);
    }

    public void testFindAggConsideringSubcubePredicatePrefersAggWithExtraLevel() {
        final RolapStar star = mock(RolapStar.class);
        final AggStar coarseAgg = mock(AggStar.class);
        final AggStar yearAgg = mock(AggStar.class);
        final RolapStar.Column yearColumn = mock(RolapStar.Column.class);
        final BitKey queryLevelBitKey = bitKey(8);
        final BitKey measureBitKey = bitKey(8, 6);
        final BitKey effectiveMeasureAndYear = bitKey(8, 1, 6);
        final SortedMap<Integer, SortedSet<String>> constrainedLevelNames =
            Collections.emptySortedMap();
        final MemberColumnPredicate year2024 =
            memberPredicate(yearColumn, 2024, "[Период.Год].[2024]");

        when(yearColumn.getBitPosition()).thenReturn(1);
        when(yearColumn.getStar()).thenReturn(star);
        when(yearColumn.getParentColumn()).thenReturn(null);
        when(star.getColumn(1)).thenReturn(yearColumn);
        when(star.getColumnCount()).thenReturn(8);
        when(star.getAggStars()).thenReturn(Arrays.asList(coarseAgg, yearAgg));

        stubSimpleAdditiveAgg(coarseAgg, bitKey(8), measureBitKey);
        stubSimpleAdditiveAgg(yearAgg, bitKey(8, 1), measureBitKey);
        when(coarseAgg.superSetMatch(measureBitKey)).thenReturn(true);
        when(coarseAgg.superSetMatch(effectiveMeasureAndYear)).thenReturn(false);
        when(yearAgg.superSetMatch(measureBitKey)).thenReturn(true);
        when(yearAgg.superSetMatch(effectiveMeasureAndYear)).thenReturn(true);
        when(yearAgg.matchesRequestedLevels(anyInt(), anyCollection()))
            .thenReturn(true);

        final boolean[] plainRollup = {false};
        final AggStar plain = AggregationManager.findAgg(
            star,
            queryLevelBitKey,
            measureBitKey,
            plainRollup,
            constrainedLevelNames);
        assertSame(coarseAgg, plain);

        final boolean[] subcubeAwareRollup = {false};
        final AggStar subcubeAware =
            AggregationManager.findAggConsideringSubcubePredicate(
                star,
                queryLevelBitKey,
                measureBitKey,
                subcubeAwareRollup,
                constrainedLevelNames,
                year2024);
        assertSame(yearAgg, subcubeAware);
    }

    public void testFindAggConsideringSubcubePredicatePrefersAggForCartesianSubcubeTuples() {
        final RolapStar star = mock(RolapStar.class);
        final AggStar coarseAgg = mock(AggStar.class);
        final AggStar regionChainAgg = mock(AggStar.class);
        final RolapStar.Column regionColumn = mock(RolapStar.Column.class);
        final RolapStar.Column chainColumn = mock(RolapStar.Column.class);
        final RolapStar.Table regionTable = mock(RolapStar.Table.class);
        final RolapStar.Table chainTable = mock(RolapStar.Table.class);
        final BitKey queryLevelBitKey = bitKey(8);
        final BitKey measureBitKey = bitKey(8, 6);
        final BitKey effectiveMeasureAndFilters = bitKey(8, 1, 2, 6);
        final SortedMap<Integer, SortedSet<String>> constrainedLevelNames =
            Collections.emptySortedMap();
        final MemberColumnPredicate regionCentral =
            memberPredicate(
                regionColumn,
                "central",
                "[Регион].[Центральный]",
                "[Регион].[Регион]");
        final MemberColumnPredicate regionSiberia =
            memberPredicate(
                regionColumn,
                "siberia",
                "[Регион].[Сибирь]",
                "[Регион].[Регион]");
        final MemberColumnPredicate chainA =
            memberPredicate(
                chainColumn,
                "A",
                "[Сеть].[A]",
                "[Сеть].[Сеть]");
        final MemberColumnPredicate chainB =
            memberPredicate(
                chainColumn,
                "B",
                "[Сеть].[B]",
                "[Сеть].[Сеть]");
        final OrPredicate subcubePredicate =
            new OrPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    regionCentral,
                    chainA)),
                new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    regionCentral,
                    chainB)),
                new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    regionSiberia,
                    chainA)),
                new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    regionSiberia,
                    chainB))));

        when(regionColumn.getBitPosition()).thenReturn(1);
        when(regionColumn.getStar()).thenReturn(star);
        when(regionColumn.getTable()).thenReturn(regionTable);
        when(regionColumn.getParentColumn()).thenReturn(null);
        when(chainColumn.getBitPosition()).thenReturn(2);
        when(chainColumn.getStar()).thenReturn(star);
        when(chainColumn.getTable()).thenReturn(chainTable);
        when(chainColumn.getParentColumn()).thenReturn(null);
        when(star.getColumn(1)).thenReturn(regionColumn);
        when(star.getColumn(2)).thenReturn(chainColumn);
        when(star.getColumnCount()).thenReturn(8);
        when(star.getAggStars()).thenReturn(
            Arrays.asList(coarseAgg, regionChainAgg));

        stubSimpleAdditiveAgg(coarseAgg, bitKey(8), measureBitKey);
        stubSimpleAdditiveAgg(regionChainAgg, bitKey(8, 1, 2), measureBitKey);
        when(coarseAgg.superSetMatch(measureBitKey)).thenReturn(true);
        when(coarseAgg.superSetMatch(effectiveMeasureAndFilters))
            .thenReturn(false);
        when(regionChainAgg.superSetMatch(measureBitKey)).thenReturn(true);
        when(regionChainAgg.superSetMatch(effectiveMeasureAndFilters))
            .thenReturn(true);
        when(regionChainAgg.matchesRequestedLevels(anyInt(), anyCollection()))
            .thenReturn(true);

        final boolean[] plainRollup = {false};
        final AggStar plain = AggregationManager.findAgg(
            star,
            queryLevelBitKey,
            measureBitKey,
            plainRollup,
            constrainedLevelNames);
        assertSame(coarseAgg, plain);

        final boolean[] subcubeAwareRollup = {false};
        final AggStar subcubeAware =
            AggregationManager.findAggConsideringSubcubePredicate(
                star,
                queryLevelBitKey,
                measureBitKey,
                subcubeAwareRollup,
                constrainedLevelNames,
                subcubePredicate);
        assertSame(regionChainAgg, subcubeAware);
    }

    private Fixture fixture(boolean dialectSupportsMerge) {
        return fixture(false, dialectSupportsMerge);
    }

    private Fixture fixture(
        boolean allLevelDistinctTwoMeasures,
        boolean dialectSupportsMerge)
    {
        final Fixture f = new Fixture();
        f.star = mock(RolapStar.class);
        f.aggStar = mock(AggStar.class);
        final Dialect dialect = mock(Dialect.class);
        when(dialect.supportsDistinctCountMergeFunction(anyString()))
            .thenReturn(dialectSupportsMerge);
        when(f.star.getSqlQueryDialect()).thenReturn(dialect);

        when(f.star.getAggStars()).thenReturn(Collections.singletonList(f.aggStar));
        when(f.aggStar.getStar()).thenReturn(f.star);

        final BitKey distinctMeasureBitKey;
        final BitKey aggLevelBitKey;
        final BitKey aggMeasureBitKey;
        final BitKey rollableCoreLevelBitKey = bitKey(8, 0);
        if (allLevelDistinctTwoMeasures) {
            f.queryLevelBitKey = bitKey(8);
            f.measureBitKey = bitKey(8, 6, 7);
            distinctMeasureBitKey = bitKey(8, 6, 7);
            aggLevelBitKey = bitKey(8);
            aggMeasureBitKey = bitKey(8, 6, 7);
        } else {
            final RolapStar.Column column = mock(RolapStar.Column.class);
            when(column.getParentColumn()).thenReturn(null);
            when(f.star.getColumn(1)).thenReturn(column);
            f.queryLevelBitKey = bitKey(8, 1);
            f.measureBitKey = bitKey(8, 6);
            distinctMeasureBitKey = bitKey(8, 6);
            aggLevelBitKey = bitKey(8, 1, 2);
            aggMeasureBitKey = bitKey(8, 6);
        }

        when(f.aggStar.superSetMatch(any(BitKey.class))).thenReturn(true);
        when(f.aggStar.getDistinctMeasureBitKey()).thenReturn(distinctMeasureBitKey);
        when(f.aggStar.hasIgnoredColumns()).thenReturn(false);
        when(f.aggStar.hasForeignKeys()).thenReturn(false);
        when(f.aggStar.getLevelBitKey()).thenReturn(aggLevelBitKey);
        when(f.aggStar.getMeasureBitKey()).thenReturn(aggMeasureBitKey);

        final AggStar.FactTable.Measure distinctMeasure =
            mock(AggStar.FactTable.Measure.class);
        when(distinctMeasure.getRollableLevelBitKey())
            .thenReturn(rollableCoreLevelBitKey);
        when(distinctMeasure.getName()).thenReturn("m6");
        when(f.aggStar.lookupMeasure(6)).thenReturn(distinctMeasure);
        if (allLevelDistinctTwoMeasures) {
            final AggStar.FactTable.Measure distinctMeasure7 =
                mock(AggStar.FactTable.Measure.class);
            when(distinctMeasure7.getRollableLevelBitKey())
                .thenReturn(rollableCoreLevelBitKey);
            when(distinctMeasure7.getName()).thenReturn("m7");
            when(f.aggStar.lookupMeasure(7)).thenReturn(distinctMeasure7);
        }

        when(f.aggStar.select(any(BitKey.class), any(BitKey.class), any(BitKey.class)))
            .thenAnswer(invocation -> {
                final BitKey requestedLevelBitKey = invocation.getArgument(0);
                final BitKey coreLevelBitKey = invocation.getArgument(1);
                final BitKey requestedMeasureBitKey = invocation.getArgument(2);

                if (!aggMeasureBitKey.isSuperSetOf(requestedMeasureBitKey)) {
                    return false;
                }
                if (aggLevelBitKey.equals(requestedLevelBitKey)) {
                    return true;
                }
                return aggLevelBitKey.isSuperSetOf(requestedLevelBitKey)
                    && aggLevelBitKey.andNot(coreLevelBitKey).equals(
                        requestedLevelBitKey.andNot(coreLevelBitKey));
            });

        return f;
    }

    private Fixture fixtureAllLevelDistinctTwoMeasures(
        boolean dialectSupportsMerge)
    {
        return fixture(true, dialectSupportsMerge);
    }

    private Fixture fixtureForLevelIdentity() {
        final Fixture f = new Fixture();
        f.star = mock(RolapStar.class);
        f.aggStar = mock(AggStar.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        f.queryLevelBitKey = bitKey(8, 1);
        f.measureBitKey = bitKey(8, 6);

        when(column.getParentColumn()).thenReturn(null);
        when(f.star.getColumn(1)).thenReturn(column);
        when(f.star.getAggStars()).thenReturn(Collections.singletonList(f.aggStar));
        when(f.aggStar.getStar()).thenReturn(f.star);
        when(f.aggStar.superSetMatch(any(BitKey.class))).thenReturn(true);
        when(f.aggStar.getDistinctMeasureBitKey()).thenReturn(bitKey(8));
        when(f.aggStar.hasIgnoredColumns()).thenReturn(false);
        when(f.aggStar.hasForeignKeys()).thenReturn(false);
        when(f.aggStar.getLevelBitKey()).thenReturn(bitKey(8, 1));
        when(f.aggStar.getMeasureBitKey()).thenReturn(bitKey(8, 6));
        when(f.aggStar.select(any(BitKey.class), any(BitKey.class), any(BitKey.class)))
            .thenReturn(true);
        return f;
    }

    private BitKey bitKey(int size, int... bits) {
        final BitKey key = BitKey.Factory.makeBitKey(size);
        for (int bit : bits) {
            key.set(bit);
        }
        return key;
    }

    private SortedMap<Integer, SortedSet<String>> requestedLevels(
        int bitPosition,
        String levelUniqueName)
    {
        final SortedSet<String> levelNames = new TreeSet<String>();
        levelNames.add(levelUniqueName);
        final SortedMap<Integer, SortedSet<String>> requestedLevels =
            new TreeMap<Integer, SortedSet<String>>();
        requestedLevels.put(bitPosition, levelNames);
        return requestedLevels;
    }

    private MemberColumnPredicate memberPredicate(
        RolapStar.Column column,
        Object key,
        String uniqueName)
    {
        return memberPredicate(
            column,
            key,
            uniqueName,
            "[Период.Год].[Год]");
    }

    private MemberColumnPredicate memberPredicate(
        RolapStar.Column column,
        Object key,
        String uniqueName,
        String levelUniqueName)
    {
        final RolapMember member = mock(RolapMember.class);
        final RolapLevel level = mock(RolapLevel.class);
        when(member.getKey()).thenReturn(key);
        when(member.getUniqueName()).thenReturn(uniqueName);
        when(member.getLevel()).thenReturn(level);
        when(level.getUniqueName()).thenReturn(levelUniqueName);
        return new MemberColumnPredicate(column, member);
    }

    private void stubSimpleAdditiveAgg(
        AggStar aggStar,
        BitKey levelBitKey,
        BitKey measureBitKey)
    {
        when(aggStar.superSetMatch(levelBitKey.or(measureBitKey)))
            .thenReturn(true);
        when(aggStar.getDistinctMeasureBitKey()).thenReturn(bitKey(8));
        when(aggStar.matchesRequestedLevels(anyInt(), anyCollection()))
            .thenReturn(true);
        when(aggStar.isFullyCollapsed()).thenReturn(true);
        when(aggStar.hasIgnoredColumns()).thenReturn(false);
        when(aggStar.getLevelBitKey()).thenReturn(levelBitKey.copy());
    }

    private void restoreProperty(MondrianProperties properties, String value) {
        restoreProperty(properties, DISTINCT_MERGE_PROP, value);
    }

    private void restoreProperty(
        MondrianProperties properties,
        String path,
        String value)
    {
        if (value == null) {
            properties.remove(path);
        } else {
            properties.setProperty(path, value);
        }
    }

    private static class Fixture {
        private RolapStar star;
        private AggStar aggStar;
        private BitKey queryLevelBitKey;
        private BitKey measureBitKey;
    }
}
