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
import mondrian.rolap.RolapStar;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
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

    private BitKey bitKey(int size, int... bits) {
        final BitKey key = BitKey.Factory.makeBitKey(size);
        for (int bit : bits) {
            key.set(bit);
        }
        return key;
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
