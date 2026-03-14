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
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapStar;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExplicitRecognizerAliasMatchTest extends TestCase {

    public void testResolvePrefersExactMatchOverAliasCandidate() {
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

    public void testResolveUsesAliasMatchWhenEnabled() {
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

    public void testResolveRejectsAliasMatchWhenDisabled() {
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

    public void testResolveRejectsAmbiguousAliasMatches() {
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

    public void testAliasMatchFallsBackToGenericExpressionForPlainRolapLevel() {
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
