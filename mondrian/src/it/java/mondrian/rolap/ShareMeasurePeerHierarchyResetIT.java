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
            createProductFlatContext()
                .withFreshConnection()
                .executeQuery(buildProductFlatQuery());

        propSaver.set(
            MondrianProperties.instance().CalcShareMeasureAutoResetPeerHierarchies,
            true);
        final Result autoResetResult =
            createProductFlatContext()
                .withFreshConnection()
                .executeQuery(buildProductFlatQuery());

        assertTrue(
            "Expected at least one tuple on rows",
            autoResetResult.getAxes()[1].getPositions().size() > 0);
        assertManualAndAutoDifferOnAtLeastOneRow(legacyResult);
        assertManualAndAutoMatchOnEveryRow(autoResetResult);
        assertFamilyBrandBlocksRemainContiguous(autoResetResult);
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

    private TestContext createProductFlatContext() {
        return getTestContext().createSubstitutingCube(
            "Sales",
            productFlatDimensionXml(),
            productFlatMeasureXml());
    }

    private String buildProductFlatQuery() {
        return "SELECT "
            + "{[Measures].[Unit Sales], "
            + "[Measures].[Family Sales Manual], "
            + "[Measures].[Family Sales Auto]} ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  {[Product Flat].[Family].[Drink], [Product Flat].[Family].[Food]},\n"
            + "  CrossJoin(\n"
            + "    [Product Flat].[Brand].[Brand].Members,\n"
            + "    [Product Flat].[Sku].[Sku].Members)) ON ROWS\n"
            + "FROM [Sales]";
    }

    private String productFlatDimensionXml() {
        return "<Dimension name=\"Product Flat\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy name=\"Family\" allMemberName=\"All Families\""
            + " hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "    <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "      <Table name=\"product\"/>\n"
            + "      <Table name=\"product_class\"/>\n"
            + "    </Join>\n"
            + "    <Level name=\"Family\" table=\"product_class\" column=\"product_family\""
            + " uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "  <Hierarchy name=\"Brand\" allMemberName=\"All Brands\""
            + " hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "    <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "      <Table name=\"product\"/>\n"
            + "      <Table name=\"product_class\"/>\n"
            + "    </Join>\n"
            + "    <Level name=\"Brand\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\">\n"
            + "      <Annotations>\n"
            + "        <Annotation name=\"drilldown.dependsOn\">"
            + "[Product Flat].[Family]|property:FamilyKey"
            + "</Annotation>\n"
            + "      </Annotations>\n"
            + "      <Property name=\"FamilyKey\" table=\"product_class\""
            + " column=\"product_family\" dependsOnLevelValue=\"true\"/>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "  <Hierarchy name=\"Sku\" allMemberName=\"All Skus\""
            + " hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "    <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "      <Table name=\"product\"/>\n"
            + "      <Table name=\"product_class\"/>\n"
            + "    </Join>\n"
            + "    <Level name=\"Sku\" table=\"product\" column=\"product_id\""
            + " nameColumn=\"product_name\" type=\"Numeric\" uniqueMembers=\"true\">\n"
            + "      <Annotations>\n"
            + "        <Annotation name=\"drilldown.dependsOnChain\">"
            + "[Product Flat].[Family]=FamilyKey > [Product Flat].[Brand]=BrandKey"
            + "</Annotation>\n"
            + "      </Annotations>\n"
            + "      <Property name=\"FamilyKey\" table=\"product_class\""
            + " column=\"product_family\" dependsOnLevelValue=\"true\"/>\n"
            + "      <Property name=\"BrandKey\" table=\"product\""
            + " column=\"brand_name\" dependsOnLevelValue=\"true\"/>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>";
    }

    private String productFlatMeasureXml() {
        return "<CalculatedMember name=\"Family Sales Manual\" dimension=\"Measures\">\n"
            + "  <Formula>([Product Flat].[Family].CurrentMember,"
            + " [Product Flat].[Brand].DefaultMember,"
            + " [Product Flat].[Sku].DefaultMember,"
            + " [Measures].[Unit Sales])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"Standard\"/>\n"
            + "</CalculatedMember>\n"
            + "<CalculatedMember name=\"Family Sales Auto\" dimension=\"Measures\">\n"
            + "  <Annotations>\n"
            + "    <Annotation name=\"semantics.kind\">companion_denominator</Annotation>\n"
            + "    <Annotation name=\"semantics.childHierarchy\">[Product Flat].[Brand]</Annotation>\n"
            + "    <Annotation name=\"semantics.topHierarchy\">[Product Flat].[Family]</Annotation>\n"
            + "  </Annotations>\n"
            + "  <Formula>([Product Flat].[Family].CurrentMember,"
            + " [Product Flat].[Brand].DefaultMember,"
            + " [Measures].[Unit Sales])</Formula>\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"Standard\"/>\n"
            + "</CalculatedMember>";
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
        final Axis rowAxis = result.getAxes()[1];
        for (int row = 0; row < rowAxis.getPositions().size(); row++) {
            final String manualValue = cellValue(result, 1, row);
            final String autoValue = cellValue(result, 2, row);
            assertEquals(
                "Auto-reset denominator mismatch on " + tupleLabel(
                    rowAxis.getPositions().get(row)),
                manualValue,
                autoValue);
        }
    }

    private void assertFamilyBrandBlocksRemainContiguous(Result result) {
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
                "Family/brand block reopened after native dependsOnChain ordering: "
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
