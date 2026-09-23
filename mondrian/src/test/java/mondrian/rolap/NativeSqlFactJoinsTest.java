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

import mondrian.olap.MondrianDef;
import mondrian.spi.Dialect;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@code ${factJoins}} star-join binding of NSC
 * templates (dronsv/emondrian-clickhouse#81 option 1, design spec
 * 2026-08-18-nsc-join-dimension-axes-design.md).
 */
public class NativeSqlFactJoinsTest {

    // ------------------------------------------------------------------
    // Structural AtomicPredicateInfo: column + tail stored separately,
    // rendering late-bound so a rebase can requalify the column.
    // ------------------------------------------------------------------

    @Test public void testStructuralPredicateRendersFactQualifierByDefault() {
        final NativeSqlCalc.AtomicPredicateInfo p =
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Регион", "region", "= 'Москва'", null, null);
        assertEquals("f.region = 'Москва'", p.render(null));
    }

    @Test public void testStructuralPredicateRendersIsNullTail() {
        final NativeSqlCalc.AtomicPredicateInfo p =
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Регион", "region", "IS NULL", null, null);
        assertEquals("f.region IS NULL", p.render(null));
    }

    @Test public void testWithQualifiedExprRequalifiesRendering() {
        final NativeSqlCalc.AtomicPredicateInfo p =
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Регион", "region", "= 'Москва'", null, null);
        assertEquals(
            "nscd0.`region` = 'Москва'",
            p.withQualifiedExpr("nscd0.`region`").render(null));
    }

    @Test public void testStructuralPredicateHonorsExclusionNames() {
        final NativeSqlCalc.AtomicPredicateInfo p =
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Регион", "region", "= 'Москва'", null, null);
        assertNull(
            p.render(
                new LinkedHashSet<String>(
                    Collections.singletonList("ТТ"))));
    }

    @Test public void testLegacyPreRenderedPredicateUnchanged() {
        final NativeSqlCalc.AtomicPredicateInfo p =
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "f.brand = 'A'");
        assertEquals("f.brand = 'A'", p.render(null));
        assertNull(p.columnName);
    }

    // ------------------------------------------------------------------
    // TemplateColumnSkip reason
    // ------------------------------------------------------------------

    @Test public void testTemplateColumnSkipDefaultsToColumnMissing() {
        final NativeSqlCalc.TemplateColumnSkip skip =
            new NativeSqlCalc.TemplateColumnSkip(
                0, "agg_brand_store",
                new LinkedHashSet<String>(
                    Collections.singletonList("region")));
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.COLUMN_MISSING,
            skip.reason());
    }

    @Test public void testTemplateColumnSkipWithReason() {
        final NativeSqlCalc.TemplateColumnSkip skip =
            new NativeSqlCalc.TemplateColumnSkip(
                1, "agg_brand_store",
                new LinkedHashSet<String>(
                    Collections.singletonList("region")))
                .withReason(
                    NativeSqlCalc.TemplateSkipReason
                        .NO_FACT_JOINS_PLACEHOLDER);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.NO_FACT_JOINS_PLACEHOLDER,
            skip.reason());
    }

    @Test public void testExhaustionWarnNamesSkipReason() {
        final java.util.List<NativeSqlCalc.TemplateColumnSkip> skips =
            Collections.singletonList(
                new NativeSqlCalc.TemplateColumnSkip(
                    0, "agg_brand_store",
                    new LinkedHashSet<String>(
                        Collections.singletonList("region")))
                    .withReason(
                        NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH));
        final String warn = NativeSqlCalc.describeExhaustedTemplateChain(
            "WD %", 1, skips,
            Collections.<NativeSqlCalc.AxisBinding>emptyList());
        org.junit.jupiter.api.Assertions.assertTrue(
            warn.contains("NO_STAR_PATH"),
            "WARN must name the skip reason: " + warn);
    }

    // ------------------------------------------------------------------
    // Raw-template validation: ${factJoins} count and reserved alias
    // ------------------------------------------------------------------

    @Test public void testValidateTemplateIgnoresTemplatesWithoutPlaceholder() {
        // 'nscd' text and unmatched FROM-f sites are all legal when the
        // template does not opt in via ${factJoins}.
        NativeSqlFactJoins.validateTemplate(
            "SELECT nscd0.x FROM agg f", "WD %", 0);
    }

    @Test public void testValidateTemplateAcceptsMatchingCounts() {
        NativeSqlFactJoins.validateTemplate(
            "SELECT f.brand FROM agg_brand_store f\n${factJoins}\n"
            + "WHERE ${whereClause}",
            "WD %", 0);
    }

    @Test public void testValidateTemplateAcceptsPlaceholderFactTableSource() {
        NativeSqlFactJoins.validateTemplate(
            "SELECT f.brand FROM ${factTable} f ${factJoins}",
            "WD %", 0);
    }

    @Test public void testValidateTemplateCountsMultipleFromSites() {
        NativeSqlFactJoins.validateTemplate(
            "WITH p AS (SELECT f.k FROM agg_a f ${factJoins} GROUP BY f.k)"
            + " SELECT f.k FROM agg_a f ${factJoins}",
            "WD %", 0);
    }

    @Test public void testValidateTemplateRejectsCountMismatch() {
        final mondrian.olap.MondrianException e =
            org.junit.jupiter.api.Assertions.assertThrows(
                mondrian.olap.MondrianException.class,
                () -> NativeSqlFactJoins.validateTemplate(
                    "WITH p AS (SELECT f.k FROM agg_a f GROUP BY f.k)"
                    + " SELECT f.k FROM agg_a f ${factJoins}",
                    "WD %", 2));
        org.junit.jupiter.api.Assertions.assertTrue(
            e.getMessage().contains("WD %")
                && e.getMessage().contains("2"),
            "error must name measure and template index: "
                + e.getMessage());
    }

    @Test public void testValidateTemplateRejectsReservedAlias() {
        org.junit.jupiter.api.Assertions.assertThrows(
            mondrian.olap.MondrianException.class,
            () -> NativeSqlFactJoins.validateTemplate(
                "SELECT nscd0.region FROM agg f ${factJoins}",
                "WD %", 0));
    }

    @Test public void testValidateTemplateAllowsNscdAsNamePrefix() {
        // 'nscdata' is not the reserved alias — only nscd / nscd<N>.
        NativeSqlFactJoins.validateTemplate(
            "SELECT f.nscdata FROM agg f ${factJoins}",
            "WD %", 0);
    }

    @Test public void testJoinAliasFromSitesIgnoresOtherAliases() {
        assertEquals(
            1,
            NativeSqlFactJoins.countFactAliasFromSites(
                "SELECT * FROM agg_a f JOIN dim d ON f.k = d.k"));
        assertEquals(
            0,
            NativeSqlFactJoins.countFactAliasFromSites(
                "SELECT * FROM agg_a g"));
    }

    // ------------------------------------------------------------------
    // Per-template rebase: resolver decision matrix
    // ------------------------------------------------------------------

    private static final String TEMPLATE =
        "SELECT ${axisExpr1} AS k0, sum(f.wd_num) AS val\n"
        + "FROM agg_brand_store f\n"
        + "${factJoins}\n"
        + "WHERE ${whereClause}\n"
        + "GROUP BY ${axisExpr1} AS k0";

    @Test public void testColumnPresentKeepsFactBindingAndEmptyJoins()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "wd_num"));
        final Map<String, String> base = basePlaceholders("f.brand");
        final NativeSqlCalc.AxisBinding brand = binding(
            "Продукт.Бренд", "brand", "k0", null);

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            base,
            Collections.singletonList(brand),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals("", r.placeholders.get("factJoins"));
        assertEquals("f.brand", r.placeholders.get("axisExpr1"));
        assertSame(brand, r.axisBindings.get(0));
        // base map must never be mutated
        assertNull(base.get("factJoins"));
    }

    @Test public void testMissingColumnResolvesStarJoin() throws Exception {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "store_key", "wd_num"),
            table("dim_konfet_store", "store_key", "region"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals(
            "LEFT ANY JOIN `dim_konfet_store` nscd0"
            + " ON f.`store_key` = nscd0.`store_key`",
            r.placeholders.get("factJoins"));
        assertEquals("nscd0.`region`", r.placeholders.get("axisExpr1"));
        assertEquals(
            "nscd0.`region`", r.axisBindings.get(0).qualifiedColumn);
        assertEquals("region", r.axisBindings.get(0).columnName);
        assertEquals("k0", r.axisBindings.get(0).keyAlias);
    }

    @Test public void testNonTableRelationDeclinesJoiningByShortName()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "store_key", "wd_num"),
            table("dim_view", "store_key", "region"));
        final RolapStar.Column column =
            starColumn("dim_view", "store_key", "store_key");
        when(column.getTable().getRelation()).thenReturn(new MondrianDef.View());
        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "Native", basePlaceholders("f.region"),
            List.of(binding("Geo.Region", "region", "k0", column)),
            List.of(), clickHouseDialect(), ds);
        assertNotNull(r.skip);
        assertEquals(NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH, r.skip.reason());
    }

    // ------------------------------------------------------------------
    // #50 review: the schema qualifier has to reach the driver argument
    // that driver actually filters on, and the rows it hands back have
    // to belong to the relation that was asked for.
    // ------------------------------------------------------------------

    @Test public void testEmptySchemaAttributeProbesUnqualifiedRelation()
        throws Exception
    {
        // XOM yields "" for schema="", which is "no schema" to JDBC.
        final DataSource ds = fakeDriverDataSource(
            SchemaFilter.SCHEMA_ARGUMENT,
            relation(null, "agg_brand_store", "brand", "store_key", "wd_num"),
            relation(null, "dim_konfet_store", "store_key", "region"));
        final RolapStar.Column column =
            starColumn("dim_konfet_store", "store_key", "store_key");
        declaredSchema(column, "");

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %", basePlaceholders("f.region"),
            List.of(binding("ТТ.Регион", "region", "k0", column)),
            List.of(), clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals(
            "LEFT ANY JOIN `dim_konfet_store` nscd0"
            + " ON f.`store_key` = nscd0.`store_key`",
            r.placeholders.get("factJoins"));
    }

    @Test public void testCatalogTermDriverKeepsTheDeclaredSchema()
        throws Exception
    {
        // ClickHouse under its default databaseTerm=catalog filters
        // system.columns by the CATALOG argument; a schema passed in the
        // schema argument leaves the filter at `database LIKE '%'`.
        assertNull(
            rebaseForeignColumn(SchemaFilter.CATALOG_ARGUMENT, "region").skip);
        final NativeSqlFactJoins.Rebase foreign =
            rebaseForeignColumn(SchemaFilter.CATALOG_ARGUMENT, "district");
        assertNotNull(
            foreign.skip,
            "district belongs to other.dim_konfet_store, not actual's");
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.DIM_COLUMN_MISSING,
            foreign.skip.reason());
    }

    @Test public void testDriverIgnoringTheFilterCannotLendForeignColumns()
        throws Exception
    {
        assertNull(
            rebaseForeignColumn(SchemaFilter.UNFILTERED, "region").skip);
        final NativeSqlFactJoins.Rebase foreign =
            rebaseForeignColumn(SchemaFilter.UNFILTERED, "district");
        assertNotNull(
            foreign.skip,
            "rows of another schema must not vouch for actual's relation");
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.DIM_COLUMN_MISSING,
            foreign.skip.reason());
    }

    @Test public void testSchemaQualifiedJoinRendersBothParts()
        throws Exception
    {
        final NativeSqlFactJoins.Rebase r =
            rebaseForeignColumn(SchemaFilter.CATALOG_ARGUMENT, "region");
        assertEquals(
            "LEFT ANY JOIN `actual`.`dim_konfet_store` nscd0"
            + " ON f.`store_key` = nscd0.`store_key`",
            r.placeholders.get("factJoins"));
    }

    @Test public void testQualifiedSourceTableIsProbedInItsOwnSchema()
        throws Exception
    {
        // FROM analytics.agg_brand_store f: the source lacks `region`,
        // but a same-named relation in `staging` has it. Probing the
        // bare name would leave the f-binding in place and emit SQL the
        // real source cannot run.
        final String template = TEMPLATE.replace(
            "FROM agg_brand_store f", "FROM analytics.agg_brand_store f");
        final DataSource ds = fakeDriverDataSource(
            SchemaFilter.SCHEMA_ARGUMENT,
            relation("analytics", "agg_brand_store", "brand", "store_key"),
            relation("staging", "agg_brand_store", "brand", "region"),
            relation(null, "dim_konfet_store", "store_key", "region"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            template, 0, "WD %", basePlaceholders("f.region"),
            List.of(binding(
                "ТТ.Регион", "region", "k0",
                starColumn("dim_konfet_store", "store_key", "store_key"))),
            List.of(), clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals(
            "nscd0.`region`", r.axisBindings.get(0).qualifiedColumn);
    }

    /**
     * Rebases {@code column} off {@code actual.dim_konfet_store} on a
     * server that also holds {@code other.dim_konfet_store}, whose
     * columns must never answer for actual's relation.
     */
    private NativeSqlFactJoins.Rebase rebaseForeignColumn(
        SchemaFilter filter, String column)
        throws Exception
    {
        final DataSource ds = fakeDriverDataSource(
            filter,
            relation(null, "agg_brand_store", "brand", "store_key", "wd_num"),
            relation("actual", "dim_konfet_store", "store_key", "region"),
            relation("other", "dim_konfet_store", "store_key", "district"));
        final RolapStar.Column starColumn =
            starColumn("dim_konfet_store", "store_key", "store_key");
        declaredSchema(starColumn, "actual");
        return NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %", basePlaceholders("f." + column),
            List.of(binding("ТТ.Регион", column, "k0", starColumn)),
            List.of(), clickHouseDialect(), ds);
    }

    @Test public void testMissingColumnWithoutStarColumnSkips()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "wd_num"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0", null);

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 3, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNotNull(r.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH,
            r.skip.reason());
        assertEquals(3, r.skip.templateIndex());
        assertTrue(r.skip.missingColumns().contains("region"));
    }

    @Test public void testFactTableColumnSkipsWithNoStarPath()
        throws Exception
    {
        // starColumn whose table has no join condition = fact column
        final RolapStar.Column factCol = mock(RolapStar.Column.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(factCol.getTable()).thenReturn(factTable);
        when(factTable.getJoinCondition()).thenReturn(null);
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "wd_num"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(
                binding("ТТ.Регион", "region", "k0", factCol)),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNotNull(r.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH,
            r.skip.reason());
    }

    @Test public void testFkMissingOnSourceSkips() throws Exception {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "wd_num"),
            table("dim_konfet_store", "store_key", "region"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNotNull(r.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.FK_MISSING_ON_SOURCE,
            r.skip.reason());
        assertTrue(r.skip.missingColumns().contains("store_key"));
    }

    @Test public void testDimColumnMissingSkips() throws Exception {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "store_key", "wd_num"),
            table("dim_konfet_store", "store_key", "city"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNotNull(r.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.DIM_COLUMN_MISSING,
            r.skip.reason());
    }

    @Test public void testDimMetadataUnreadableSkipsClosed()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "store_key", "wd_num"),
            table("dim_konfet_store"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNotNull(r.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.DIM_COLUMN_MISSING,
            r.skip.reason());
    }

    @Test public void testSourceMetadataUnreadableFailsOpen()
        throws Exception
    {
        // No readable metadata for the f-bound source: nothing provable,
        // keep f.col unchanged (same fail-open contract as the skip check).
        final DataSource ds = columnsDataSource(
            table("agg_brand_store"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals("", r.placeholders.get("factJoins"));
        assertEquals("f.region", r.placeholders.get("axisExpr1"));
    }

    @Test public void testPredicateRebaseSharesJoinWithAxis()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "store_key", "wd_num"),
            table("dim_konfet_store", "store_key", "region", "city"));
        final RolapStar.Column storeStar =
            starColumn("dim_konfet_store", "store_key", "store_key");
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0", storeStar);
        final NativeSqlCalc.AtomicPredicateInfo cityPred =
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Город", "city", "= 'Москва'", storeStar, null);

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>singletonList(
                cityPred),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        // one shared join for both the axis binding and the predicate
        assertEquals(
            "LEFT ANY JOIN `dim_konfet_store` nscd0"
            + " ON f.`store_key` = nscd0.`store_key`",
            r.placeholders.get("factJoins"));
        assertEquals(
            "nscd0.`city` = 'Москва'",
            r.predicates.get(0).render(null));
        assertEquals(
            "nscd0.`city` = 'Москва'",
            r.placeholders.get("whereClause"));
    }

    @Test public void testCompositePredicateRebasesChildren()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "store_key", "wd_num"),
            table("dim_konfet_store", "store_key", "region"));
        final NativeSqlCalc.AtomicPredicateInfo regionPred =
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Регион", "region", "IS NULL",
                starColumn("dim_konfet_store", "store_key", "store_key"),
                null);
        final NativeSqlCalc.AtomicPredicateInfo brandPred =
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "brand", "= 'A'", null, null);
        final NativeSqlCalc.CompositePredicateInfo or =
            new NativeSqlCalc.CompositePredicateInfo(
                NativeSqlCalc.BooleanOp.OR,
                Arrays.<NativeSqlCalc.PredicateInfo>asList(
                    regionPred, brandPred));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.brand"),
            Collections.<NativeSqlCalc.AxisBinding>emptyList(),
            Collections.<NativeSqlCalc.PredicateInfo>singletonList(or),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals(
            "(nscd0.`region` IS NULL OR f.brand = 'A')",
            r.placeholders.get("whereClause"));
    }

    @Test public void testLiteralPredicateUntouched() throws Exception {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "wd_num"));
        final NativeSqlCalc.AtomicPredicateInfo literal =
            new NativeSqlCalc.AtomicPredicateInfo(
                null, null, "true", Collections.<String>emptySet());

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.brand"),
            Collections.<NativeSqlCalc.AxisBinding>emptyList(),
            Collections.<NativeSqlCalc.PredicateInfo>singletonList(
                literal),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals("true", r.placeholders.get("whereClause"));
        assertEquals("", r.placeholders.get("factJoins"));
    }

    @Test public void testTwoDimTablesGetOrderedAliases() throws Exception {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "store_key", "period_month", "wd_num"),
            table("dim_konfet_store", "store_key", "region"),
            table("dim_konfet_period", "period_month", "quarter"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));
        final NativeSqlCalc.AxisBinding quarter = binding(
            "Период.Квартал", "quarter", "k1",
            starColumn(
                "dim_konfet_period", "period_month", "period_month"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Arrays.asList(region, quarter),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals(
            "LEFT ANY JOIN `dim_konfet_store` nscd0"
            + " ON f.`store_key` = nscd0.`store_key`\n"
            + "LEFT ANY JOIN `dim_konfet_period` nscd1"
            + " ON f.`period_month` = nscd1.`period_month`",
            r.placeholders.get("factJoins"));
        assertEquals(
            "nscd0.`region`", r.axisBindings.get(0).qualifiedColumn);
        assertEquals(
            "nscd1.`quarter`", r.axisBindings.get(1).qualifiedColumn);
    }

    @Test public void testNonClickHouseDialectUsesPlainLeftJoin()
        throws Exception
    {
        final Dialect mysql = mock(Dialect.class);
        when(mysql.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.MYSQL);
        when(mysql.quoteIdentifier(anyString()))
            .thenAnswer(inv -> "`" + inv.getArgument(0) + "`");
        stubQualifiedQuoting(mysql);
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "store_key", "wd_num"),
            table("dim_konfet_store", "store_key", "region"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            mysql, ds);

        assertNull(r.skip);
        assertTrue(
            r.placeholders.get("factJoins").startsWith("LEFT JOIN "),
            r.placeholders.get("factJoins"));
    }

    @Test public void testResolveTemplateWithoutPlaceholderIsIdentity()
        throws Exception
    {
        // No ${factJoins} → the exact base objects flow through: the
        // structural guarantee that legacy templates render bit-for-bit
        // identically (no copy, no re-render, no metadata reads).
        final Map<String, String> base = basePlaceholders("f.region");
        final List<NativeSqlCalc.AxisBinding> bindings =
            Collections.singletonList(
                binding("ТТ.Регион", "region", "k0", null));
        final List<NativeSqlCalc.PredicateInfo> predicates =
            Collections.<NativeSqlCalc.PredicateInfo>singletonList(
                new NativeSqlCalc.AtomicPredicateInfo(
                    "Продукт", "Бренд", "f.brand = 'A'"));

        final NativeSqlFactJoins.Rebase r =
            NativeSqlFactJoins.resolveTemplate(
                "SELECT f.region FROM agg_x f", 0, "WD %",
                base, bindings, predicates,
                clickHouseDialect(), mock(DataSource.class));

        assertNull(r.skip);
        assertSame(base, r.placeholders);
        assertSame(bindings, r.axisBindings);
        assertSame(predicates, r.predicates);
    }

    @Test public void testResolveTemplateWithPlaceholderRebases()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "brand", "wd_num"));
        final NativeSqlFactJoins.Rebase r =
            NativeSqlFactJoins.resolveTemplate(
                TEMPLATE, 0, "WD %",
                basePlaceholders("f.brand"),
                Collections.singletonList(
                    binding("Продукт.Бренд", "brand", "k0", null)),
                Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
                clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals("", r.placeholders.get("factJoins"));
    }

    @Test public void testEmptyFactJoinsSubstitutesToEmptyString() {
        final Map<String, String> ph = basePlaceholders("f.brand");
        ph.put("factJoins", "");
        ph.put("whereClause", "1 = 1");
        final String sql = NativeSqlCalc.substitutePlaceholders(
            "SELECT f.brand FROM agg f\n${factJoins}\nWHERE ${whereClause}",
            ph);
        assertEquals(
            "SELECT f.brand FROM agg f\n\nWHERE 1 = 1", sql);
    }

    // ------------------------------------------------------------------
    // M2: rollupAxes synthetic bindings rescued by ${factJoins}
    // (Task 44 pre-validation relaxed to "column on agg OR FK+star path")
    // ------------------------------------------------------------------

    @Test public void testSyntheticBindingRescuedByFactJoinsStarPath() {
        final RolapStar star = syntheticStar(
            "region", starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand_store", "brand", "store_key")),
                true);

        assertNotNull(
            binding,
            "column missing on agg but FK+star path present and chain"
            + " opts into ${factJoins} — binding must survive for the"
            + " per-template rebase");
        assertEquals("f.region", binding.qualifiedColumn);
        assertNotNull(binding.starColumn);
    }

    @Test public void testSyntheticBindingNotRescuedWithoutPlaceholder() {
        final RolapStar star = syntheticStar(
            "region", starColumn("dim_konfet_store", "store_key", "store_key"));

        assertNull(
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand_store", "brand", "store_key")),
                false),
            "no ${factJoins} in the chain — legacy Task 44 verdict holds");
    }

    @Test public void testSyntheticBindingNotRescuedWhenFkMissingOnAggs() {
        final RolapStar star = syntheticStar(
            "region", starColumn("dim_konfet_store", "store_key", "store_key"));

        assertNull(
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand", "brand")),
                true),
            "join FK absent on every candidate agg — not rescuable");
    }

    @Test public void testSyntheticBindingNotRescuedWithoutJoinCondition() {
        // star column resolves onto the fact table (no join condition)
        final RolapStar.Column factCol = mock(RolapStar.Column.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(factCol.getTable()).thenReturn(factTable);
        when(factTable.getJoinCondition()).thenReturn(null);
        final RolapStar star = syntheticStar("region", factCol);

        assertNull(
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand_store", "brand", "store_key")),
                true),
            "no star join path — not rescuable");
    }

    @Test public void testSyntheticRescueMatchesPhysicalAggColumnNames() {
        // Real AggStar level columns are SYMBOLICALLY named (the level
        // name, e.g. "Адрес"), with the physical column only in the
        // expression (agg_brand_store.store_key). The FK check must
        // match the physical name or every registered agg fails it.
        final RolapStar star = syntheticStar(
            "region", starColumn("dim_konfet_store", "store_key", "store_key"));

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg(
                    "agg_brand_store",
                    aggLevelColumn("Бренд", "brand"),
                    aggLevelColumn("Адрес", "store_key"))),
                true);

        assertNotNull(
            binding,
            "FK carried as a symbolically-named level column must count");
        assertEquals("f.region", binding.qualifiedColumn);
    }

    @Test public void testSyntheticRescueNeedsTheFactSideKeyOfASnowflake() {
        // region's table hangs off the store table: its join key
        // (store.region_id) is not a fact column, so an agg carrying a
        // same-named region_id cannot anchor the ${factJoins} path.
        final RolapStar star = syntheticStar(
            "region", snowflakeColumn("dim_region", "region_id", "region_id",
                starColumn("dim_konfet_store", "store_key", "store_key")));

        assertNull(
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand_store", "brand", "region_id")),
                true),
            "the path starts at the fact FK store_key, absent on every agg");
        assertNotNull(
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), star, "f",
                new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand_store", "brand", "store_key")),
                true),
            "the fact FK anchors the whole snowflake path");
    }

    @Test public void testSyntheticBindingNotRescuedOnNonUniqueJoinPath() {
        final RolapStar.Column column =
            starColumn("dim_konfet_store", "store_key", "store_key");
        when(column.getTable().hasNonUniqueJoinPath()).thenReturn(true);

        assertNull(
            NativeSqlCalc.resolveSyntheticBinding(
                syntheticHierarchy("region"), syntheticStar("region", column),
                "f", new java.util.ArrayList<String>(),
                new LinkedHashSet<String>(), 0,
                aggs(agg("agg_brand_store", "brand", "store_key")),
                true),
            "a join key that can address several rows is no star path");
    }

    @Test public void testSnowflakeRendersEveryJoinFromTheFactKey()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "store_key", "region_id", "wd_num"),
            table("dim_konfet_store", "store_key", "region_id"),
            table("dim_region", "region_id", "region"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            snowflakeColumn("dim_region", "region_id", "region_id",
                starColumn("dim_konfet_store", "store_key", "store_key")));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNull(r.skip);
        assertEquals(
            "LEFT ANY JOIN `dim_konfet_store` nscd0"
            + " ON f.`store_key` = nscd0.`store_key`\n"
            + "LEFT ANY JOIN `dim_region` nscd1"
            + " ON nscd0.`region_id` = nscd1.`region_id`",
            r.placeholders.get("factJoins"));
        assertEquals("nscd1.`region`", r.placeholders.get("axisExpr1"));
    }

    @Test public void testSnowflakeTableWithoutTheNextJoinKeySkips()
        throws Exception
    {
        final DataSource ds = columnsDataSource(
            table("agg_brand_store", "store_key", "wd_num"),
            table("dim_konfet_store", "store_key", "city"),
            table("dim_region", "region_id", "region"));
        final NativeSqlCalc.AxisBinding region = binding(
            "ТТ.Регион", "region", "k0",
            snowflakeColumn("dim_region", "region_id", "region_id",
                starColumn("dim_konfet_store", "store_key", "store_key")));

        final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
            TEMPLATE, 0, "WD %",
            basePlaceholders("f.region"),
            Collections.singletonList(region),
            Collections.<NativeSqlCalc.PredicateInfo>emptyList(),
            clickHouseDialect(), ds);

        assertNotNull(r.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.DIM_COLUMN_MISSING,
            r.skip.reason());
        assertEquals("dim_konfet_store", r.skip.tableName());
        assertTrue(r.skip.missingColumns().contains("region_id"));
    }

    @Test public void testChainContainsPlaceholder() {
        assertTrue(NativeSqlFactJoins.chainContainsPlaceholder(
            Arrays.asList(
                "SELECT 1 FROM a f",
                "SELECT 1 FROM b f ${factJoins}")));
        org.junit.jupiter.api.Assertions.assertFalse(
            NativeSqlFactJoins.chainContainsPlaceholder(
                Arrays.asList("SELECT 1 FROM a f")));
    }

    private static mondrian.olap.Hierarchy syntheticHierarchy(
        String columnName)
    {
        final mondrian.olap.Hierarchy hierarchy =
            mock(mondrian.olap.Hierarchy.class);
        final mondrian.olap.Level allLevel =
            mock(mondrian.olap.Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[ТТ.Регион]");
        when(hierarchy.getLevels()).thenReturn(
            new mondrian.olap.Level[] {allLevel, dataLevel});
        when(dataLevel.getUniqueName()).thenReturn("[ТТ.Регион].[Регион]");
        when(dataLevel.getKeyExp()).thenReturn(
            new MondrianDef.Column("dim_konfet_store", columnName));
        return hierarchy;
    }

    /** Star whose lookupColumn(dim_konfet_store, columnName) yields the
     *  given star column. */
    private static RolapStar syntheticStar(
        String columnName, RolapStar.Column starColumn)
    {
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(star.getFactTable()).thenReturn(factTable);
        when(star.lookupColumn("dim_konfet_store", columnName))
            .thenReturn(starColumn);
        return star;
    }

    private static mondrian.rolap.aggmatcher.AggStar agg(
        String name, String... columns)
    {
        final mondrian.rolap.aggmatcher.AggStar.Table.Column[] cols =
            new mondrian.rolap.aggmatcher.AggStar.Table.Column[
                columns.length];
        for (int i = 0; i < columns.length; i++) {
            cols[i] = aggLevelColumn(columns[i], columns[i]);
        }
        return agg(name, cols);
    }

    private static mondrian.rolap.aggmatcher.AggStar agg(
        String name,
        mondrian.rolap.aggmatcher.AggStar.Table.Column... columns)
    {
        final mondrian.rolap.aggmatcher.AggStar agg =
            mock(mondrian.rolap.aggmatcher.AggStar.class);
        final mondrian.rolap.aggmatcher.AggStar.FactTable fact =
            mock(mondrian.rolap.aggmatcher.AggStar.FactTable.class);
        when(agg.getFactTable()).thenReturn(fact);
        when(fact.getName()).thenReturn(name);
        when(fact.getColumns()).thenReturn(
            new java.util.ArrayList<
                mondrian.rolap.aggmatcher.AggStar.Table.Column>(
                    Arrays.asList(columns)));
        return agg;
    }

    /** Agg column shaped like a real AggLevel: symbolic name + physical
     *  expression. */
    private static mondrian.rolap.aggmatcher.AggStar.Table.Column
        aggLevelColumn(String symbolicName, String physicalName)
    {
        final mondrian.rolap.aggmatcher.AggStar.Table.Column col =
            mock(mondrian.rolap.aggmatcher.AggStar.Table.Column.class);
        when(col.getName()).thenReturn(symbolicName);
        final MondrianDef.Column expression = new MondrianDef.Column();
        expression.name = physicalName;
        when(col.getExpression()).thenReturn(expression);
        return col;
    }

    private static java.util.Set<mondrian.rolap.aggmatcher.AggStar> aggs(
        mondrian.rolap.aggmatcher.AggStar... entries)
    {
        return new LinkedHashSet<mondrian.rolap.aggmatcher.AggStar>(
            Arrays.asList(entries));
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static Map<String, String> basePlaceholders(String axisExpr1) {
        final Map<String, String> ph =
            new LinkedHashMap<String, String>();
        ph.put("factTable", "agg_brand_store");
        ph.put("factAlias", "f");
        ph.put("axisExpr1", axisExpr1);
        ph.put("axisPresenceSelectList", ",\n    " + axisExpr1 + " AS k0");
        ph.put("whereClause", "1 = 1");
        return ph;
    }

    private static NativeSqlCalc.AxisBinding binding(
        String hierarchyName,
        String columnName,
        String keyAlias,
        RolapStar.Column starColumn)
    {
        return new NativeSqlCalc.AxisBinding(
            null, hierarchyName, "f." + columnName, columnName, keyAlias,
            starColumn);
    }

    private static RolapStar.Column starColumn(
        String dimTableName, String fkName, String pkName)
    {
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapStar.Table dimTable = mock(RolapStar.Table.class);
        final RolapStar.Condition condition =
            mock(RolapStar.Condition.class);
        final MondrianDef.Column fk = new MondrianDef.Column();
        fk.name = fkName;
        final MondrianDef.Column pk = new MondrianDef.Column();
        pk.name = pkName;
        when(column.getTable()).thenReturn(dimTable);
        when(dimTable.getTableName()).thenReturn(dimTableName);
        final MondrianDef.Table relation = new MondrianDef.Table();
        relation.name = dimTableName;
        when(dimTable.getRelation()).thenReturn(relation);
        when(dimTable.getJoinCondition()).thenReturn(condition);
        when(condition.getLeft()).thenReturn(fk);
        when(condition.getRight()).thenReturn(pk);
        return column;
    }

    /** Star column on a snowflake table joined from {@code parent}'s
     *  table (not from the fact table). */
    private static RolapStar.Column snowflakeColumn(
        String dimTableName, String leftKey, String rightKey,
        RolapStar.Column parent)
    {
        final RolapStar.Column column =
            starColumn(dimTableName, leftKey, rightKey);
        final RolapStar.Table parentTable = parent.getTable();
        when(column.getTable().getParentTable()).thenReturn(parentTable);
        return column;
    }

    private static Map.Entry<String, List<String>> table(
        String name, String... columns)
    {
        return new java.util.AbstractMap.SimpleImmutableEntry<
            String, List<String>>(name, Arrays.asList(columns));
    }

    /** Records the {@code <Table schema="…">} the star hop declares. */
    private static void declaredSchema(
        RolapStar.Column column, String schema)
    {
        ((MondrianDef.Table) column.getTable().getRelation()).schema = schema;
    }

    /** Which {@code getColumns} argument a driver reads the owner from. */
    private enum SchemaFilter {
        /** JDBC's own reading, and ClickHouse databaseTerm=schema. */
        SCHEMA_ARGUMENT,
        /** ClickHouse's default databaseTerm=catalog. */
        CATALOG_ARGUMENT,
        /** A driver that filters on neither. */
        UNFILTERED
    }

    /** One relation in a fake server's catalog. */
    private record FakeRelation(
        String schema, String table, List<String> columns)
    {
    }

    private static FakeRelation relation(
        String schema, String table, String... columns)
    {
        return new FakeRelation(schema, table, Arrays.asList(columns));
    }

    /**
     * A DataSource whose metadata answers like a real driver: it
     * declares where it takes the owner from and then filters on
     * exactly that argument, ignoring the other one.
     */
    private static DataSource fakeDriverDataSource(
        SchemaFilter filter, FakeRelation... relations)
        throws Exception
    {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.supportsSchemasInTableDefinitions())
            .thenReturn(filter != SchemaFilter.CATALOG_ARGUMENT);
        when(metaData.supportsCatalogsInTableDefinitions())
            .thenReturn(filter == SchemaFilter.CATALOG_ARGUMENT);
        when(metaData.getColumns(
            nullable(String.class), nullable(String.class),
            nullable(String.class), nullable(String.class)))
            .thenAnswer(inv -> {
                final String owner = switch (filter) {
                    case SCHEMA_ARGUMENT -> (String) inv.getArgument(1);
                    case CATALOG_ARGUMENT -> (String) inv.getArgument(0);
                    case UNFILTERED -> null;
                };
                final String table = inv.getArgument(2);
                final List<FakeRelation> matched =
                    new java.util.ArrayList<FakeRelation>();
                for (FakeRelation r : relations) {
                    if (r.table().equals(table)
                        && (owner == null || owner.equals(r.schema())))
                    {
                        matched.add(r);
                    }
                }
                return fakeColumnsResultSet(matched);
            });
        return dataSource;
    }

    /**
     * A {@code getColumns} result set carrying TABLE_SCHEM per row.
     * A proxy rather than a mock: this is built while the metadata mock
     * is answering a call, where starting a new stubbing is not safe.
     */
    private static ResultSet fakeColumnsResultSet(
        List<FakeRelation> relations)
    {
        final List<String[]> rows = new java.util.ArrayList<String[]>();
        for (FakeRelation r : relations) {
            for (String column : r.columns()) {
                rows.add(new String[] {r.schema(), column});
            }
        }
        final int[] cursor = {-1};
        return (ResultSet) java.lang.reflect.Proxy.newProxyInstance(
            NativeSqlFactJoinsTest.class.getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "next" -> ++cursor[0] < rows.size();
                case "getString" -> "TABLE_SCHEM".equals(args[0])
                    ? rows.get(cursor[0])[0]
                    : "COLUMN_NAME".equals(args[0])
                        ? rows.get(cursor[0])[1]
                        : null;
                case "close" -> null;
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                case "toString" -> "fakeColumns(" + rows.size() + " rows)";
                default -> throw new UnsupportedOperationException(
                    method.getName());
            });
    }

    @SafeVarargs
    private static DataSource columnsDataSource(
        Map.Entry<String, List<String>>... tables)
        throws Exception
    {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        for (Map.Entry<String, List<String>> t : tables) {
            final ResultSet columns = columnsResultSet(t.getValue());
            when(metaData.getColumns(null, null, t.getKey(), null))
                .thenReturn(columns);
        }
        return dataSource;
    }

    private static ResultSet columnsResultSet(List<String> columns)
        throws Exception
    {
        final ResultSet resultSet = mock(ResultSet.class);
        final int n = columns == null ? 0 : columns.size();
        final Boolean[] next = new Boolean[n + 1];
        Arrays.fill(next, Boolean.TRUE);
        next[next.length - 1] = Boolean.FALSE;
        when(resultSet.next()).thenReturn(
            next[0], Arrays.copyOfRange(next, 1, next.length));
        if (n > 0) {
            when(resultSet.getString("COLUMN_NAME")).thenReturn(
                columns.get(0),
                columns.subList(1, n).toArray(new String[0]));
        }
        return resultSet;
    }

    private static Dialect clickHouseDialect() {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
        when(dialect.quoteIdentifier(anyString()))
            .thenAnswer(inv -> "`" + inv.getArgument(0) + "`");
        stubQualifiedQuoting(dialect);
        return dialect;
    }

    /**
     * Models {@link mondrian.spi.impl.JdbcDialectImpl#quoteIdentifier(
     * String, String)}: each non-null part quoted, joined by a dot.
     */
    private static void stubQualifiedQuoting(Dialect dialect) {
        when(dialect.quoteIdentifier(nullable(String.class), anyString()))
            .thenAnswer(inv -> {
                final String qualifier = inv.getArgument(0);
                final String name = inv.getArgument(1);
                return qualifier == null
                    ? "`" + name + "`"
                    : "`" + qualifier + "`.`" + name + "`";
            });
    }
}
