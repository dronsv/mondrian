/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapStar;
import mondrian.rolap.SyntheticFlatHierarchy;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExplicitRecognizerAliasMatchTest {

    @Test public void testResolvePrefersExactMatchOverAliasCandidate() {
        final RolapCubeLevel requested = cubeLevel("[Flat].[Mfr]", 11, "mfr_id");
        final ExplicitRules.TableDef.Level exact = aggLevel("[Flat].[Mfr]", "manufacturer_flat", 91, "mfr_id");
        final ExplicitRules.TableDef.Level alias = aggLevel("[Hier].[Mfr]", "manufacturer_hier", 11, "mfr_id");

        final Map<String, ExplicitRules.TableDef.Level> exactMap =
            Collections.singletonMap("[Flat].[Mfr]", exact);

        assertSame(
            exact,
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                exactMap,
                Arrays.asList(exact, alias),
                aggColumns("manufacturer_flat", "manufacturer_hier"),
                true));
    }

    @Test public void testResolveUsesAliasMatchWhenEnabled() {
        final RolapCubeLevel requested = cubeLevel("[Flat].[Mfr]", 11, "mfr_id");
        final ExplicitRules.TableDef.Level alias = aggLevel("[Hier].[Mfr]", "manufacturer_hier", 11, "mfr_id");

        assertSame(
            alias,
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.<String, ExplicitRules.TableDef.Level>emptyMap(),
                Collections.singletonList(alias),
                aggColumns("manufacturer_hier"),
                true));
    }

    @Test public void testResolveRejectsAliasMatchWhenDisabled() {
        final RolapCubeLevel requested = cubeLevel("[Flat].[Mfr]", 11, "mfr_id");
        final ExplicitRules.TableDef.Level alias = aggLevel("[Hier].[Mfr]", "manufacturer_hier", 11, "mfr_id");

        assertNull(
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.<String, ExplicitRules.TableDef.Level>emptyMap(),
                Collections.singletonList(alias),
                aggColumns("manufacturer_hier"),
                false));
    }

    @Test public void testResolveRejectsAmbiguousAliasMatches() {
        final RolapCubeLevel requested = cubeLevel("[Flat].[Mfr]", 11, "mfr_id");
        final ExplicitRules.TableDef.Level aliasOne = aggLevel("[Hier].[Mfr]", "manufacturer_hier", 11, "mfr_id");
        final ExplicitRules.TableDef.Level aliasTwo = aggLevel("[Alt].[Mfr]", "manufacturer_alt", 11, "mfr_id");

        assertNull(
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.<String, ExplicitRules.TableDef.Level>emptyMap(),
                Arrays.asList(aliasOne, aliasTwo),
                aggColumns("manufacturer_hier", "manufacturer_alt"),
                true));
    }

    @Test public void testResolveSkipsHiddenHierarchyDirectQuery() {
        // Direct query against a schema-hidden hierarchy must NOT bind
        // to <AggLevel> — aggregate storage misalignment causes the
        // cells to be looked up as null. The fact-table fallback path
        // handles these correctly.
        final mondrian.rolap.RolapHierarchy hiddenHier =
            mock(mondrian.rolap.RolapHierarchy.class);
        when(hiddenHier.isVisible()).thenReturn(false);

        final RolapLevel hiddenLevel = mock(RolapLevel.class);
        when(hiddenLevel.getHierarchy()).thenReturn(hiddenHier);
        when(hiddenLevel.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getRolapLevel()).thenReturn(hiddenLevel);
        when(requested.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");

        final ExplicitRules.TableDef.Level agg =
            mock(ExplicitRules.TableDef.Level.class);
        when(agg.getColumnName()).thenReturn("category_l1_id");
        when(agg.getName()).thenReturn("[Product.Category].[Category1]");

        assertNull(
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.singletonMap(
                    "[Product.Category].[Category1]", agg),
                Collections.singletonList(agg),
                aggColumns("category_l1_id"),
                true));
    }

    @Test public void testResolveRejectsHiddenSourceEvenWhenSyntheticAliasExists() {
        final String hiddenName = "[Product.Category].[Category1]";
        final String syntheticName = "[Product.Category1].[Category1]";

        final RolapHierarchy hiddenHier = mock(RolapHierarchy.class);
        when(hiddenHier.isVisible()).thenReturn(false);

        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Column starColumn = mock(RolapStar.Column.class);
        when(starColumn.getBitPosition()).thenReturn(3);
        when(starColumn.getStar()).thenReturn(star);
        when(star.getLevelAliases(3, hiddenName))
            .thenReturn(sortedSet(syntheticName));

        final RolapLevel hiddenLevel = mock(RolapLevel.class);
        when(hiddenLevel.getHierarchy()).thenReturn(hiddenHier);
        when(hiddenLevel.getUniqueName()).thenReturn(hiddenName);

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getRolapLevel()).thenReturn(hiddenLevel);
        when(requested.getUniqueName()).thenReturn(hiddenName);
        when(requested.getStarKeyColumn()).thenReturn(starColumn);

        final ExplicitRules.TableDef.Level agg =
            mock(ExplicitRules.TableDef.Level.class);
        when(agg.getColumnName()).thenReturn("category_l1_id");
        when(agg.getName()).thenReturn(hiddenName);

        assertNull(
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.singletonMap(hiddenName, agg),
                Collections.singletonList(agg),
                aggColumns("category_l1_id"),
                true));
    }

    @Test public void testAggStarBlocksHiddenSourceAliases()
        throws Exception
    {
        final String hiddenName = "[Product.Category].[Category1]";
        final String syntheticName = "[Product.Category1].[Category1]";

        final RolapHierarchy hiddenHier = mock(RolapHierarchy.class);
        when(hiddenHier.isVisible()).thenReturn(false);

        final RolapStar star = mock(RolapStar.class);
        when(star.getColumnCount()).thenReturn(8);
        when(star.getLevelAliases(3, hiddenName))
            .thenReturn(sortedSet(syntheticName));

        final RolapStar.Column starColumn = mock(RolapStar.Column.class);
        when(starColumn.getStar()).thenReturn(star);
        when(starColumn.getBitPosition()).thenReturn(3);

        final RolapLevel hiddenLevel = mock(RolapLevel.class);
        when(hiddenLevel.getHierarchy()).thenReturn(hiddenHier);
        when(hiddenLevel.getUniqueName()).thenReturn(hiddenName);

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getRolapLevel()).thenReturn(hiddenLevel);
        when(requested.getUniqueName()).thenReturn(hiddenName);
        when(requested.getStarKeyColumn()).thenReturn(starColumn);

        final JdbcSchema.Table dbTable = mock(JdbcSchema.Table.class);
        when(dbTable.getName()).thenReturn("agg_category");
        final AggStar aggStar = new AggStar(star, dbTable, 10);

        Method register =
            AggStar.class.getDeclaredMethod(
                "registerSourceLevel",
                int.class,
                RolapLevel.class);
        register.setAccessible(true);
        register.invoke(aggStar, 3, requested);

        assertFalse(
            aggStar.matchesRequestedLevels(
                3,
                Collections.singleton(syntheticName)));
        assertFalse(
            aggStar.matchesRequestedLevels(
                3,
                Collections.singleton(hiddenName)));
    }

    @Test public void testResolveFallsBackToSourceLevelForCubeWrappedSyntheticFlat() {
        // Source (real, visible) level whose unique name appears
        // in the user-declared <AggName>.
        final RolapLevel sourceLevel = mock(RolapLevel.class);
        when(sourceLevel.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");

        // SyntheticFlatHierarchy projects sourceLevel as a one-level
        // standalone hierarchy. Its source link points back at the
        // real source level.
        final SyntheticFlatHierarchy synth =
            mock(SyntheticFlatHierarchy.class);
        when(synth.getSourceLinks()).thenReturn(sourceLinks(sourceLevel));

        // Underlying schema-level synthetic flat level. Its getHierarchy()
        // returns the SyntheticFlatHierarchy.
        final RolapLevel underlyingSynth = mock(RolapLevel.class);
        when(underlyingSynth.getHierarchy()).thenReturn(synth);

        // Cube-wrapped synthetic flat level — what the recognizer
        // actually sees. RolapCubeLevel.getHierarchy() returns the
        // RolapCubeHierarchy (not the synthetic), so we must unwrap
        // via getRolapLevel() to reach the synthetic check.
        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getUniqueName())
            .thenReturn("[Product.Category1].[Category1]");
        when(requested.getRolapLevel()).thenReturn(underlyingSynth);

        // <AggLevel> declared once against the source (real) level.
        final ExplicitRules.TableDef.Level sourceAgg =
            mock(ExplicitRules.TableDef.Level.class);
        when(sourceAgg.getColumnName()).thenReturn("category_l1_id");
        when(sourceAgg.getName())
            .thenReturn("[Product.Category].[Category1]");

        final Map<String, ExplicitRules.TableDef.Level> exactMap =
            Collections.singletonMap(
                "[Product.Category].[Category1]", sourceAgg);

        // aliasMatchEnabled=false to prove the synthetic-flat fallback
        // path operates independently of the MatchAliasLevelsByStarColumn
        // property.
        assertSame(
            sourceAgg,
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                exactMap,
                Collections.singletonList(sourceAgg),
                aggColumns("category_l1_id"),
                false));
    }

    @Test public void testResolveFallsBackToSecondSyntheticSourceLink() {
        final RolapLevel firstSourceLevel = mock(RolapLevel.class);
        when(firstSourceLevel.getUniqueName())
            .thenReturn("[Product.ByBrand].[Sku]");

        final RolapLevel secondSourceLevel = mock(RolapLevel.class);
        when(secondSourceLevel.getUniqueName())
            .thenReturn("[Product.ByCategory].[Sku]");

        final SyntheticFlatHierarchy synth =
            mock(SyntheticFlatHierarchy.class);
        when(synth.getSourceLinks())
            .thenReturn(sourceLinks(firstSourceLevel, secondSourceLevel));

        final RolapLevel underlyingSynth = mock(RolapLevel.class);
        when(underlyingSynth.getHierarchy()).thenReturn(synth);

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getUniqueName())
            .thenReturn("[Product.Sku].[Sku]");
        when(requested.getRolapLevel()).thenReturn(underlyingSynth);

        final ExplicitRules.TableDef.Level secondSourceAgg =
            mock(ExplicitRules.TableDef.Level.class);
        when(secondSourceAgg.getColumnName()).thenReturn("sku_id");
        when(secondSourceAgg.getName())
            .thenReturn("[Product.ByCategory].[Sku]");

        assertSame(
            secondSourceAgg,
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.singletonMap(
                    "[Product.ByCategory].[Sku]",
                    secondSourceAgg),
                Collections.singletonList(secondSourceAgg),
                aggColumns("sku_id"),
                false));
    }

    @Test public void
    testResolveAcceptsHiddenSourceForSyntheticFlatFallback() {
        // Regression for dronsv/mondrian#19 Bug A. The synthetic-flat
        // fallback must succeed even when the source level lives on a
        // schema-hidden hierarchy — schema authors declare <AggLevel>
        // against the source (hidden) level once, and the synthetic
        // flat projection inherits aggregate coverage via that
        // declaration. Storage divergence #20 is still avoided because
        // the *requesting* level is the synthetic flat one (visible);
        // direct queries against the hidden source level continue to
        // decline via the existing isSchemaHiddenLevel(rLevel) guard at
        // the top of resolveAggLevelForRolapLevel.
        final RolapHierarchy hiddenHier = mock(RolapHierarchy.class);
        when(hiddenHier.isVisible()).thenReturn(false);

        final RolapLevel sourceLevel = mock(RolapLevel.class);
        when(sourceLevel.getHierarchy()).thenReturn(hiddenHier);
        when(sourceLevel.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");

        final SyntheticFlatHierarchy synth =
            mock(SyntheticFlatHierarchy.class);
        when(synth.getSourceLinks()).thenReturn(sourceLinks(sourceLevel));

        final RolapLevel underlyingSynth = mock(RolapLevel.class);
        when(underlyingSynth.getHierarchy()).thenReturn(synth);

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getUniqueName())
            .thenReturn("[Product.Category1].[Category1]");
        when(requested.getRolapLevel()).thenReturn(underlyingSynth);

        final ExplicitRules.TableDef.Level sourceAgg =
            mock(ExplicitRules.TableDef.Level.class);
        when(sourceAgg.getColumnName()).thenReturn("category_l1_id");
        when(sourceAgg.getName())
            .thenReturn("[Product.Category].[Category1]");

        assertSame(
            sourceAgg,
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.singletonMap(
                    "[Product.Category].[Category1]",
                    sourceAgg),
                Collections.singletonList(sourceAgg),
                aggColumns("category_l1_id"),
                false));
    }

    @Test public void
    testResolveStillRejectsDirectQueryAgainstHiddenSourceLevel() {
        // Direct queries against a schema-hidden hierarchy level still
        // decline aggregate coverage (dronsv/mondrian#20 cell-storage
        // divergence protection). The rejection now happens at the
        // top-level isSchemaHiddenLevel(rLevel) guard, not in the
        // synthetic-flat fallback.
        final RolapHierarchy hiddenHier = mock(RolapHierarchy.class);
        when(hiddenHier.isVisible()).thenReturn(false);

        final RolapLevel hiddenSourceLevel = mock(RolapLevel.class);
        when(hiddenSourceLevel.getHierarchy()).thenReturn(hiddenHier);
        when(hiddenSourceLevel.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");
        when(requested.getRolapLevel()).thenReturn(hiddenSourceLevel);

        final ExplicitRules.TableDef.Level sourceAgg =
            mock(ExplicitRules.TableDef.Level.class);
        when(sourceAgg.getColumnName()).thenReturn("category_l1_id");
        when(sourceAgg.getName())
            .thenReturn("[Product.Category].[Category1]");

        assertNull(
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.singletonMap(
                    "[Product.Category].[Category1]",
                    sourceAgg),
                Collections.singletonList(sourceAgg),
                aggColumns("category_l1_id"),
                false));
    }

    @Test public void testResolveReturnsNullWhenSyntheticHasNoSourceMatchAndAliasDisabled() {
        // Synthetic flat requesting level whose source is also unmapped.
        final RolapLevel sourceLevel = mock(RolapLevel.class);
        when(sourceLevel.getUniqueName())
            .thenReturn("[Product.Category].[Category1]");

        final SyntheticFlatHierarchy synth =
            mock(SyntheticFlatHierarchy.class);
        when(synth.getSourceLinks()).thenReturn(sourceLinks(sourceLevel));

        final RolapLevel underlyingSynth = mock(RolapLevel.class);
        when(underlyingSynth.getHierarchy()).thenReturn(synth);

        final RolapCubeLevel requested = mock(RolapCubeLevel.class);
        when(requested.getUniqueName())
            .thenReturn("[Product.Category1].[Category1]");
        when(requested.getRolapLevel()).thenReturn(underlyingSynth);

        assertNull(
            ExplicitRecognizer.resolveAggLevelForRolapLevel(
                requested,
                Collections.<String, ExplicitRules.TableDef.Level>emptyMap(),
                Collections.<ExplicitRules.TableDef.Level>emptyList(),
                aggColumns("category_l1_id"),
                false));
    }

    @Test public void testAliasMatchFallsBackToGenericExpressionForPlainRolapLevel() {
        final RolapLevel requested = level("[Flat].[City]", "city_id");
        final RolapLevel candidate = level("[Hier].[City]", "city_id");

        assertTrue(ExplicitRecognizer.isAliasLevelMatch(requested, candidate));
    }

    private static Map<String, JdbcSchema.Table.Column> aggColumns(String... names) {
        final Map<String, JdbcSchema.Table.Column> columns =
            new TreeMap<String, JdbcSchema.Table.Column>(String.CASE_INSENSITIVE_ORDER);
        for (String name : names) {
            columns.put(name, mock(JdbcSchema.Table.Column.class));
        }
        return columns;
    }

    private static SortedSet<String> sortedSet(String... names) {
        return new TreeSet<String>(Arrays.asList(names));
    }

    private static List<SyntheticFlatHierarchy.SourceLink> sourceLinks(
        RolapLevel... levels)
    {
        SyntheticFlatHierarchy.SourceLink[] links =
            new SyntheticFlatHierarchy.SourceLink[levels.length];
        for (int i = 0; i < levels.length; i++) {
            links[i] =
                new SyntheticFlatHierarchy.SourceLink(null, levels[i], i);
        }
        return Arrays.asList(links);
    }

    private static ExplicitRules.TableDef.Level aggLevel(
        String uniqueName,
        String columnName,
        int bitPosition,
        String expression)
    {
        final ExplicitRules.TableDef.Level level = mock(ExplicitRules.TableDef.Level.class);
        final RolapLevel rolapLevel = cubeLevel(uniqueName, bitPosition, expression);
        when(level.getColumnName()).thenReturn(columnName);
        when(level.getRolapLevel()).thenReturn(rolapLevel);
        when(level.getName()).thenReturn(uniqueName);
        return level;
    }

    private static RolapCubeLevel cubeLevel(
        String uniqueName,
        int bitPosition,
        String expression)
    {
        final RolapCubeLevel level = mock(RolapCubeLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final MondrianDef.Expression keyExp = mock(MondrianDef.Expression.class);
        when(level.getUniqueName()).thenReturn(uniqueName);
        when(level.getStarKeyColumn()).thenReturn(column);
        when(level.getKeyExp()).thenReturn(keyExp);
        when(column.getBitPosition()).thenReturn(bitPosition);
        when(keyExp.getGenericExpression()).thenReturn(expression);
        return level;
    }

    private static RolapLevel level(String uniqueName, String expression) {
        final RolapLevel level = mock(RolapLevel.class);
        final MondrianDef.Expression keyExp = mock(MondrianDef.Expression.class);
        when(level.getUniqueName()).thenReturn(uniqueName);
        when(level.getKeyExp()).thenReturn(keyExp);
        when(keyExp.getGenericExpression()).thenReturn(expression);
        return level;
    }
}
