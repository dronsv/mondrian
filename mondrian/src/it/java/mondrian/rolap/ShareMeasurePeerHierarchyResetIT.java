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

import mondrian.olap.Axis;
import mondrian.olap.Cell;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Result;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Integration coverage for share-measure peer resets under native crossjoin
 * execution with explicit dependency metadata.
 */
public class ShareMeasurePeerHierarchyResetIT extends FoodMartTestCase {
    public ShareMeasurePeerHierarchyResetIT() {
    }

    public ShareMeasurePeerHierarchyResetIT(String name) {
        super(name);
    }

    public void testAutoResetMatchesManualDenominatorUnderNativeCrossJoin() {
        enableNativeCrossJoinDependencyFeatures();

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            false);
        final Result legacyResult =
            createStoreFlatContext()
                .withFreshConnection()
                .executeQuery(buildStoreFlatQuery());

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            true);
        final Result autoResetResult =
            createStoreFlatContext()
                .withFreshConnection()
                .executeQuery(buildStoreFlatQuery());

        assertTrue(
            "Expected at least one tuple on rows",
            autoResetResult.getAxes()[1].getPositions().size() > 0);
        assertManualAndAutoDifferOnAtLeastOneRow(legacyResult);
        assertManualAndAutoMatchOnEveryRow(autoResetResult);
        assertCountryStateBlocksRemainContiguous(autoResetResult);
    }

    public void testAutoResetAutoMeasureAloneExecutesUnderNativeCrossJoin() {
        enableNativeCrossJoinDependencyFeatures();

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            true);
        final Result autoResetOnlyResult =
            createStoreFlatContext()
                .withFreshConnection()
                .executeQuery(buildStoreFlatAutoOnlyQuery());

        assertTrue(
            "Expected at least one tuple on rows",
            autoResetOnlyResult.getAxes()[1].getPositions().size() > 0);
        assertCountryStateBlocksRemainContiguous(autoResetOnlyResult);
    }

    public void testManualMeasureAloneExecutesUnderNativeCrossJoin() {
        enableNativeCrossJoinDependencyFeatures();

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            true);
        final Result manualOnlyResult =
            createStoreFlatContext()
                .withFreshConnection()
                .executeQuery(buildStoreFlatManualOnlyQuery());

        assertTrue(
            "Expected at least one tuple on rows",
            manualOnlyResult.getAxes()[1].getPositions().size() > 0);
        assertCountryStateBlocksRemainContiguous(manualOnlyResult);
    }

    public void testAutoResetShareMeasureMatchesManualShareUnderNativeCrossJoin() {
        enableNativeCrossJoinDependencyFeatures();

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            true);
        final Result shareResult =
            createStoreFlatContext()
                .withFreshConnection()
                .executeQuery(buildStoreFlatShareQuery());

        assertTrue(
            "Expected at least one tuple on rows",
            shareResult.getAxes()[1].getPositions().size() > 0);
        assertManualAndAutoMatchOnEveryRow(shareResult, 0, 1);
        assertCountryStateBlocksRemainContiguous(shareResult);
    }

    public void testAutoResetStateScopedDenominatorMatchesManualUnderChildRows() {
        enableNativeCrossJoinDependencyFeatures();

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            true);
        final Result shareResult =
            createStoreFlatContext()
                .withFreshConnection()
                .executeQuery(buildStoreFlatStateScopedShareQuery());

        assertTrue(
            "Expected at least one tuple on rows",
            shareResult.getAxes()[1].getPositions().size() > 0);
        assertManualAndAutoMatchOnEveryRow(shareResult, 0, 1);
        assertCountryStateBlocksRemainContiguous(shareResult);
    }

    private void enableNativeCrossJoinDependencyFeatures() {
        propSaver.set(MondrianProperties.instance().EnableNativeCrossJoin, true);
        propSaver.set(MondrianProperties.instance().EnableNativeNonEmpty, true);
        propSaver.set(MondrianProperties.instance().EnableNativeFilter, true);
        propSaver.set(
            MondrianProperties.instance().AlertNativeEvaluationUnsupported,
            "ERROR");
        propSaver.set(
            MondrianProperties.instance().CrossJoinDependencyPruningPolicy,
            "STRICT");
        propSaver.set(
            MondrianProperties.instance().CrossJoinOrderByDependsOnChain,
            true);
    }

    private TestContext createStoreFlatContext() {
        return getTestContext().createSubstitutingCube(
            "Sales",
            storeFlatDimensionXml(),
            storeFlatMeasureXml());
    }

    private String buildStoreFlatQuery() {
        final String countryHierarchy = hierarchyRef("Country");
        return "SELECT "
            + "{[Measures].[Unit Sales], "
            + "[Measures].[Country Sales Manual], "
            + "[Measures].[Country Sales Auto]} ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  {" + countryHierarchy + ".[USA], " + countryHierarchy + ".[Canada]},\n"
            + "  CrossJoin(\n"
            + "    " + levelRef("State", "State") + ".Members,\n"
            + "    " + levelRef("City", "City") + ".Members)) ON ROWS\n"
            + "FROM [Sales]";
    }

    private String buildStoreFlatShareQuery() {
        final String countryHierarchy = hierarchyRef("Country");
        return "SELECT "
            + "{[Measures].[State Share Manual], "
            + "[Measures].[State Share Auto]} ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  {" + countryHierarchy + ".[USA], " + countryHierarchy + ".[Canada]},\n"
            + "  CrossJoin(\n"
            + "    " + levelRef("State", "State") + ".Members,\n"
            + "    " + levelRef("City", "City") + ".Members)) ON ROWS\n"
            + "FROM [Sales]";
    }

    private String buildStoreFlatAutoOnlyQuery() {
        final String countryHierarchy = hierarchyRef("Country");
        return "SELECT "
            + "{[Measures].[Country Sales Auto]} ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  {" + countryHierarchy + ".[USA], " + countryHierarchy + ".[Canada]},\n"
            + "  CrossJoin(\n"
            + "    " + levelRef("State", "State") + ".Members,\n"
            + "    " + levelRef("City", "City") + ".Members)) ON ROWS\n"
            + "FROM [Sales]";
    }

    private String buildStoreFlatStateScopedShareQuery() {
        final String countryHierarchy = hierarchyRef("Country");
        return "SELECT "
            + "{[Measures].[City Share Within State Manual], "
            + "[Measures].[City Share Within State Auto]} ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  {" + countryHierarchy + ".[USA], " + countryHierarchy + ".[Canada]},\n"
            + "  CrossJoin(\n"
            + "    " + levelRef("State", "State") + ".Members,\n"
            + "    " + levelRef("City", "City") + ".Members)) ON ROWS\n"
            + "FROM [Sales]";
    }

    private String buildStoreFlatManualOnlyQuery() {
        final String countryHierarchy = hierarchyRef("Country");
        return "SELECT "
            + "{[Measures].[Country Sales Manual]} ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  {" + countryHierarchy + ".[USA], " + countryHierarchy + ".[Canada]},\n"
            + "  CrossJoin(\n"
            + "    " + levelRef("State", "State") + ".Members,\n"
            + "    " + levelRef("City", "City") + ".Members)) ON ROWS\n"
            + "FROM [Sales]";
    }

    private String storeFlatDimensionXml() {
        return "<Dimension name=\"Store Flat\" foreignKey=\"store_id\">\n"
            + "  <Hierarchy name=\"Country\" allMemberName=\"All Countries\""
            + " hasAll=\"true\" primaryKey=\"store_id\" primaryKeyTable=\"store\">\n"
            + "    <Table name=\"store\"/>\n"
            + "    <Level name=\"Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "  <Hierarchy name=\"State\" allMemberName=\"All States\""
            + " hasAll=\"true\" primaryKey=\"store_id\" primaryKeyTable=\"store\">\n"
            + "    <Table name=\"store\"/>\n"
            + "    <Level name=\"State\" column=\"store_state\" uniqueMembers=\"false\">\n"
            + "      <Annotations>\n"
            + "        <Annotation name=\"drilldown.dependsOn\">"
            + hierarchyRef("Country") + "|property:CountryKey"
            + "</Annotation>\n"
            + "      </Annotations>\n"
            + "      <Property name=\"CountryKey\" column=\"store_country\""
            + " dependsOnLevelValue=\"true\"/>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "  <Hierarchy name=\"City\" allMemberName=\"All Cities\""
            + " hasAll=\"true\" primaryKey=\"store_id\" primaryKeyTable=\"store\">\n"
            + "    <Table name=\"store\"/>\n"
            + "    <Level name=\"City\" column=\"store_city\" uniqueMembers=\"false\">\n"
            + "      <Annotations>\n"
            + "        <Annotation name=\"drilldown.dependsOnChain\">"
            + hierarchyRef("Country") + "=CountryKey > "
            + hierarchyRef("State") + "=StateKey"
            + "</Annotation>\n"
            + "      </Annotations>\n"
            + "      <Property name=\"CountryKey\" column=\"store_country\""
            + " dependsOnLevelValue=\"true\"/>\n"
            + "      <Property name=\"StateKey\" column=\"store_state\""
            + " dependsOnLevelValue=\"true\"/>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>";
    }

    private String storeFlatMeasureXml() {
        final String countryHierarchy = hierarchyRef("Country");
        final String stateHierarchy = hierarchyRef("State");
        final String cityHierarchy = hierarchyRef("City");
        return "<CalculatedMember name=\"Country Sales Manual\" dimension=\"Measures\">\n"
            + "  <Formula>(" + countryHierarchy + ".CurrentMember,"
            + " " + stateHierarchy + ".DefaultMember,"
            + " " + cityHierarchy + ".DefaultMember,"
            + " [Measures].[Unit Sales])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"Standard\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"Country Sales Auto\" dimension=\"Measures\">\n"
            + "  <Annotations>\n"
            + "    <Annotation name=\"semantics.kind\">companion_denominator</Annotation>\n"
            + "    <Annotation name=\"semantics.childHierarchy\">" + stateHierarchy + "</Annotation>\n"
            + "    <Annotation name=\"semantics.topHierarchy\">" + countryHierarchy + "</Annotation>\n"
            + "  </Annotations>\n"
            + "  <Formula>(" + countryHierarchy + ".CurrentMember,"
            + " " + stateHierarchy + ".DefaultMember,"
            + " [Measures].[Unit Sales])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"Standard\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"State Share Manual\" dimension=\"Measures\">\n"
            + "  <Formula>Iif([Measures].[Country Sales Manual] = 0,"
            + " NULL,"
            + " [Measures].[Unit Sales] / [Measures].[Country Sales Manual])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"0.0000\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"State Share Auto\" dimension=\"Measures\">\n"
            + "  <Formula>Iif([Measures].[Country Sales Auto] = 0,"
            + " NULL,"
            + " [Measures].[Unit Sales] / [Measures].[Country Sales Auto])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"0.0000\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"State Sales Manual\" dimension=\"Measures\">\n"
            + "  <Formula>(" + countryHierarchy + ".CurrentMember,"
            + " " + stateHierarchy + ".CurrentMember,"
            + " " + cityHierarchy + ".DefaultMember,"
            + " [Measures].[Unit Sales])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"Standard\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"State Sales Auto\" dimension=\"Measures\">\n"
            + "  <Annotations>\n"
            + "    <Annotation name=\"semantics.kind\">companion_denominator</Annotation>\n"
            + "    <Annotation name=\"semantics.childHierarchy\">" + cityHierarchy + "</Annotation>\n"
            + "    <Annotation name=\"semantics.topHierarchy\">" + stateHierarchy + "</Annotation>\n"
            + "  </Annotations>\n"
            + "  <Formula>(" + countryHierarchy + ".CurrentMember,"
            + " " + stateHierarchy + ".CurrentMember,"
            + " [Measures].[Unit Sales])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"Standard\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"City Share Within State Manual\" dimension=\"Measures\">\n"
            + "  <Formula>Iif([Measures].[State Sales Manual] = 0,"
            + " NULL,"
            + " [Measures].[Unit Sales] / [Measures].[State Sales Manual])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"0.0000\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"City Share Within State Auto\" dimension=\"Measures\">\n"
            + "  <Formula>Iif([Measures].[State Sales Auto] = 0,"
            + " NULL,"
            + " [Measures].[Unit Sales] / [Measures].[State Sales Auto])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"0.0000\"/>\n"
            + "</CalculatedMember>";
    }

    private String hierarchyRef(String hierarchy) {
        return TestContext.hierarchyName("Store Flat", hierarchy);
    }

    private String levelRef(String hierarchy, String level) {
        return TestContext.levelName("Store Flat", hierarchy, level);
    }

    private void assertManualAndAutoDifferOnAtLeastOneRow(Result result) {
        boolean foundDifference = false;
        final Axis rowAxis = result.getAxes()[1];
        for (int row = 0; row < rowAxis.getPositions().size(); row++) {
            final String manualValue = cellValue(result, 1, row);
            final String autoValue = cellValue(result, 2, row);
            if (!manualValue.equals(autoValue)) {
                foundDifference = true;
                break;
            }
        }
        assertTrue(
            "Expected peer-hierarchy leakage while auto-reset is disabled",
            foundDifference);
    }

    private void assertManualAndAutoMatchOnEveryRow(Result result) {
        assertManualAndAutoMatchOnEveryRow(result, 1, 2);
    }

    private void assertManualAndAutoMatchOnEveryRow(
        Result result,
        int manualColumn,
        int autoColumn)
    {
        final Axis rowAxis = result.getAxes()[1];
        for (int row = 0; row < rowAxis.getPositions().size(); row++) {
            final String manualValue = cellValue(result, manualColumn, row);
            final String autoValue = cellValue(result, autoColumn, row);
            assertEquals(
                "Auto-reset denominator mismatch on " + tupleLabel(
                    rowAxis.getPositions().get(row)),
                manualValue,
                autoValue);
        }
    }

    private void assertCountryStateBlocksRemainContiguous(Result result) {
        final Axis rowAxis = result.getAxes()[1];
        final Set<String> closedBlocks = new LinkedHashSet<String>();
        String currentBlock = null;
        for (Position position : rowAxis.getPositions()) {
            final String nextBlock =
                position.get(0).getUniqueName() + "|" + position.get(1).getUniqueName();
            if (nextBlock.equals(currentBlock)) {
                continue;
            }
            assertFalse(
                "Country/state block reopened after native dependsOnChain ordering: "
                    + nextBlock,
                closedBlocks.contains(nextBlock));
            if (currentBlock != null) {
                closedBlocks.add(currentBlock);
            }
            currentBlock = nextBlock;
        }
    }

    private String cellValue(Result result, int column, int row) {
        final Cell cell = result.getCell(new int[] {column, row});
        final Object value = cell.getValue();
        return value == null
            ? String.valueOf(cell.getFormattedValue())
            : String.valueOf(value);
    }

    private String tupleLabel(Position position) {
        final StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int i = 0; i < position.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            final Member member = position.get(i);
            builder.append(member == null ? "<null>" : member.getUniqueName());
        }
        builder.append(']');
        return builder.toString();
    }
}
