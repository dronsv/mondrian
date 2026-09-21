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
                "OR",
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

    @Test public void testChainContainsPlaceholder() {
        assertTrue(NativeSqlFactJoins.chainContainsPlaceholder(
            Arrays.asList(
                "SELECT 1 FROM a f",
                "SELECT 1 FROM b f ${factJoins}")));
        org.junit.jupiter.api.Assertions.assertFalse(
            NativeSqlFactJoins.chainContainsPlaceholder(
                Arrays.asList("SELECT 1 FROM a f")));
    }

    // ------------------------------------------------------------------
    // FK pushdown: a predicate bound through a star-joined dim table is
    // also rendered as the implied f.fk IN (SELECT pk FROM dim WHERE …),
    // the only form a database can prune the fact scan by.
    // ------------------------------------------------------------------

    private static final String STOCK_TEMPLATE =
        "SELECT sum(f.qty) AS val\n"
        + "FROM mart_stock f\n"
        + "${factJoins}\n"
        + "WHERE ${whereClause}";

    private static final String WEEK_PUSHDOWN =
        "f.`period_start` IN (SELECT nscs0.`period_start`"
        + " FROM `dim_period` nscs0"
        + " WHERE nscs0.`week_start` = '2026-09-07')";

    private static DataSource stockDataSource() throws Exception {
        return columnsDataSource(
            table("mart_stock", "period_start", "sku_key", "brand", "qty"),
            table("dim_period", "period_start", "week_start", "week_year"),
            table("dim_product", "sku_key", "manufacturer"));
    }

    private static NativeSqlCalc.AtomicPredicateInfo periodAtom(
        String hierarchy, String column, String tail)
    {
        return new NativeSqlCalc.AtomicPredicateInfo(
            "Период", hierarchy, column, tail,
            starColumn("dim_period", "period_start", "period_start"),
            null);
    }

    /** The same column reached through another dimension's hierarchy. */
    private static NativeSqlCalc.AtomicPredicateInfo calendarWeekAtom(
        String tail)
    {
        return new NativeSqlCalc.AtomicPredicateInfo(
            "Календарь", "Неделя", "week_start", tail,
            starColumn("dim_period", "period_start", "period_start"),
            null);
    }

    private static NativeSqlCalc.AtomicPredicateInfo productAtom(
        String tail)
    {
        return new NativeSqlCalc.AtomicPredicateInfo(
            "Продукт", "Производитель", "manufacturer", tail,
            starColumn("dim_product", "sku_key", "sku_key"), null);
    }

    private static NativeSqlFactJoins.Rebase rebaseStock(
        DataSource ds, NativeSqlCalc.PredicateInfo... predicates)
    {
        return rebaseStock(true, clickHouseDialect(), ds, predicates);
    }

    /** Rebases with the pushdown property set for this one call. */
    private static NativeSqlFactJoins.Rebase rebaseStock(
        boolean pushdown,
        Dialect dialect,
        DataSource ds,
        NativeSqlCalc.PredicateInfo... predicates)
    {
        final mondrian.olap.MondrianProperties props =
            mondrian.olap.MondrianProperties.instance();
        final boolean previous = props.NativeSqlFactJoinsFkPushdown.get();
        props.NativeSqlFactJoinsFkPushdown.set(pushdown);
        try {
            final NativeSqlFactJoins.Rebase r = NativeSqlFactJoins.rebase(
                STOCK_TEMPLATE, 0, "Stock",
                basePlaceholders("f.brand"),
                Collections.<NativeSqlCalc.AxisBinding>emptyList(),
                Arrays.asList(predicates),
                dialect, ds);
            assertNull(r.skip);
            return r;
        } finally {
            props.NativeSqlFactJoinsFkPushdown.set(previous);
        }
    }

    @Test public void testFkPushdownRendersImpliedFactSideCondition()
        throws Exception
    {
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07' AND " + WEEK_PUSHDOWN,
            r.placeholders.get("whereClause"));
        // the join itself is untouched
        assertEquals(
            "LEFT ANY JOIN `dim_period` nscd0"
            + " ON f.`period_start` = nscd0.`period_start`",
            r.placeholders.get("factJoins"));
    }

    @Test public void testFkPushdownCombinesPredicatesOfOneDimTable()
        throws Exception
    {
        // Two hierarchies of one dimension table: one subquery.
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"),
            periodAtom("Недели", "week_year", "= 2026"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07'"
            + " AND nscd0.`week_year` = 2026"
            + " AND f.`period_start` IN (SELECT nscs0.`period_start`"
            + " FROM `dim_period` nscs0"
            + " WHERE nscs0.`week_start` = '2026-09-07'"
            + " AND nscs0.`week_year` = 2026)",
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownDropsDuplicateConjuncts()
        throws Exception
    {
        // Sibling hierarchies on one column render the same condition
        // twice (slicer + ClosingPeriod member); the subquery keeps one.
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"),
            periodAtom("Недели", "week_start", "= '2026-09-07'"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07'"
            + " AND nscd0.`week_start` = '2026-09-07' AND "
            + WEEK_PUSHDOWN,
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownKeepsDisjunctionOfOneDimTable()
        throws Exception
    {
        final NativeSqlCalc.CompositePredicateInfo subselect =
            new NativeSqlCalc.CompositePredicateInfo(
                "OR",
                Arrays.<NativeSqlCalc.PredicateInfo>asList(
                    productAtom("= '44766'"), productAtom("= '44836'")));

        final NativeSqlFactJoins.Rebase r =
            rebaseStock(stockDataSource(), subselect);

        assertEquals(
            "(nscd0.`manufacturer` = '44766'"
            + " OR nscd0.`manufacturer` = '44836')"
            + " AND f.`sku_key` IN (SELECT nscs0.`sku_key`"
            + " FROM `dim_product` nscs0"
            + " WHERE (nscs0.`manufacturer` = '44766'"
            + " OR nscs0.`manufacturer` = '44836'))",
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownOnePerDimTableInJoinOrder()
        throws Exception
    {
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"),
            productAtom("= '44766'"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07'"
            + " AND nscd1.`manufacturer` = '44766' AND "
            + WEEK_PUSHDOWN
            + " AND f.`sku_key` IN (SELECT nscs1.`sku_key`"
            + " FROM `dim_product` nscs1"
            + " WHERE nscs1.`manufacturer` = '44766')",
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownSkipsDisjunctionAcrossTables()
        throws Exception
    {
        // (week OR f.brand): a fact row may pass through the other
        // branch, so nothing about the period key is implied.
        final NativeSqlCalc.CompositePredicateInfo or =
            new NativeSqlCalc.CompositePredicateInfo(
                "OR",
                Arrays.<NativeSqlCalc.PredicateInfo>asList(
                    periodAtom("Неделя", "week_start", "= '2026-09-07'"),
                    new NativeSqlCalc.AtomicPredicateInfo(
                        "Продукт", "Бренд", "brand", "= 'A'",
                        null, null)));

        final NativeSqlFactJoins.Rebase r =
            rebaseStock(stockDataSource(), or);

        assertEquals(
            "(nscd0.`week_start` = '2026-09-07' OR f.brand = 'A')",
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownProjectsConjunctionInsideDisjunction()
        throws Exception
    {
        // Compound slicer: (week AND mfr) OR (week AND mfr). Each dim
        // table gets the disjunction of its own atoms.
        final NativeSqlCalc.CompositePredicateInfo tuples =
            new NativeSqlCalc.CompositePredicateInfo(
                "OR",
                Arrays.<NativeSqlCalc.PredicateInfo>asList(
                    new NativeSqlCalc.CompositePredicateInfo(
                        "AND",
                        Arrays.<NativeSqlCalc.PredicateInfo>asList(
                            periodAtom(
                                "Неделя", "week_start", "= '2026-09-07'"),
                            productAtom("= '44766'"))),
                    new NativeSqlCalc.CompositePredicateInfo(
                        "AND",
                        Arrays.<NativeSqlCalc.PredicateInfo>asList(
                            periodAtom(
                                "Неделя", "week_start", "= '2026-08-31'"),
                            productAtom("= '44836'")))));

        final String where = rebaseStock(stockDataSource(), tuples)
            .placeholders.get("whereClause");

        assertTrue(
            where.endsWith(
                " AND f.`period_start` IN (SELECT nscs0.`period_start`"
                + " FROM `dim_period` nscs0"
                + " WHERE (nscs0.`week_start` = '2026-09-07'"
                + " OR nscs0.`week_start` = '2026-08-31'))"
                + " AND f.`sku_key` IN (SELECT nscs1.`sku_key`"
                + " FROM `dim_product` nscs1"
                + " WHERE (nscs1.`manufacturer` = '44766'"
                + " OR nscs1.`manufacturer` = '44836'))"),
            where);
    }

    @Test public void testFkPushdownSkipsConditionsTrueOnUnmatchedRow()
        throws Exception
    {
        // A fact row without a dim row sees NULLs or, on ClickHouse,
        // type defaults in the joined columns. A condition that such a
        // row can satisfy is not implied by the semi-join: no pushdown.
        for (String tail : Arrays.asList(
                "IS NULL", "= NULL", "= ''", "= 0", "= 0.00", "= '0'",
                "= '1970-01-01'", "= '1970-01-01 03:00:00'",
                "= '1969-12-31 19:00:00'", "= '00:00:00'",
                "= '00000000-0000-0000-0000-000000000000'",
                "= '0.0.0.0'", "= '::'", "= 'false'", "<> 'x'"))
        {
            final NativeSqlFactJoins.Rebase r = rebaseStock(
                stockDataSource(),
                periodAtom("Неделя", "week_start", tail));
            assertEquals(
                "nscd0.`week_start` " + tail,
                r.placeholders.get("whereClause"),
                "tail " + tail);
        }
    }

    @Test public void testFkPushdownWeakensConjunctionToSafeAtoms()
        throws Exception
    {
        // week_year = 0 could hold on an unmatched row; dropping it
        // from the conjunction only widens the implied key set.
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"),
            periodAtom("Недели", "week_year", "= 0"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07'"
            + " AND nscd0.`week_year` = 0 AND " + WEEK_PUSHDOWN,
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownSkipsEnumColumn() throws Exception {
        // The JOIN default of an Enum is its first value, which no
        // literal check can recognise.
        final DataSource ds = typedColumnsDataSource(
            table("mart_stock", "period_start", "qty"),
            typedTable(
                "dim_period",
                "period_start", "Date",
                "week_start", "Enum8('a' = 1, 'b' = 2)"));

        final NativeSqlFactJoins.Rebase r = rebaseStock(
            ds, periodAtom("Неделя", "week_start", "= 'a'"));

        assertEquals(
            "nscd0.`week_start` = 'a'",
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownFollowsWhereClauseExcept()
        throws Exception
    {
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"),
            productAtom("= '44766'"));

        // Excluding the period dimension removes its pushdown as well.
        assertEquals(
            "nscd1.`manufacturer` = '44766'"
            + " AND f.`sku_key` IN (SELECT nscs1.`sku_key`"
            + " FROM `dim_product` nscs1"
            + " WHERE nscs1.`manufacturer` = '44766')",
            NativeSqlCalc.buildWhereFromPredicates(
                r.predicates,
                new LinkedHashSet<String>(
                    Collections.singletonList("Период"))));
    }

    @Test public void testFkPushdownNotStrongerThanWhereClauseExcept()
        throws Exception
    {
        // (week AND mfr) OR week': excluding Период leaves (mfr), which
        // says nothing about rows of the second branch — the period
        // pushdown must vanish, not shrink to week'.
        final NativeSqlCalc.AtomicPredicateInfo otherWeek =
            calendarWeekAtom("= '2026-08-31'");
        final NativeSqlCalc.CompositePredicateInfo or =
            new NativeSqlCalc.CompositePredicateInfo(
                "OR",
                Arrays.<NativeSqlCalc.PredicateInfo>asList(
                    new NativeSqlCalc.CompositePredicateInfo(
                        "AND",
                        Arrays.<NativeSqlCalc.PredicateInfo>asList(
                            periodAtom(
                                "Неделя", "week_start", "= '2026-09-07'"),
                            productAtom("= '44766'"))),
                    otherWeek));

        final NativeSqlFactJoins.Rebase r =
            rebaseStock(stockDataSource(), or);
        final String except = NativeSqlCalc.buildWhereFromPredicates(
            r.predicates,
            new LinkedHashSet<String>(
                Collections.singletonList("Период")));

        assertEquals(
            "(nscd1.`manufacturer` = '44766'"
            + " OR nscd0.`week_start` = '2026-08-31')",
            except);
    }

    @Test public void testFkPushdownTreatsEmptiedCompositeAsAbsent()
        throws Exception
    {
        // (year AND week) OR week': excluding Период empties the first
        // branch, which then renders nothing — the OR is week', not TRUE.
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            new NativeSqlCalc.CompositePredicateInfo(
                "OR",
                Arrays.<NativeSqlCalc.PredicateInfo>asList(
                    new NativeSqlCalc.CompositePredicateInfo(
                        "AND",
                        Arrays.<NativeSqlCalc.PredicateInfo>asList(
                            periodAtom("Недели", "week_year", "= 2026"),
                            periodAtom(
                                "Неделя", "week_start", "= '2026-09-07'"))),
                    calendarWeekAtom("= '2026-08-31'"))));

        assertEquals(
            "nscd0.`week_start` = '2026-08-31'"
            + " AND f.`period_start` IN (SELECT nscs0.`period_start`"
            + " FROM `dim_period` nscs0"
            + " WHERE nscs0.`week_start` = '2026-08-31')",
            NativeSqlCalc.buildWhereFromPredicates(
                r.predicates,
                new LinkedHashSet<String>(
                    Collections.singletonList("Период"))));
    }

    @Test public void testFkPushdownSkipsEnumJoinKey() throws Exception {
        // f.fk IN (SELECT <Enum pk> …) casts the fact key to the Enum and
        // throws on a value outside it, where the JOIN just finds no row.
        final DataSource ds = typedColumnsDataSource(
            table("mart_stock", "period_start", "qty"),
            typedTable(
                "dim_period",
                "period_start", "Enum8('a' = 1, 'b' = 2)",
                "week_start", "Date"));

        final NativeSqlFactJoins.Rebase r = rebaseStock(
            ds, periodAtom("Неделя", "week_start", "= '2026-09-07'"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07'",
            r.placeholders.get("whereClause"));
    }

    /** An OR of {@code count} manufacturers, as a wide subselect gives. */
    private static NativeSqlCalc.PredicateInfo manufacturers(int count) {
        final List<NativeSqlCalc.PredicateInfo> atoms =
            new java.util.ArrayList<NativeSqlCalc.PredicateInfo>();
        for (int i = 0; i < count; i++) {
            atoms.add(productAtom("= 'manufacturer " + i + "'"));
        }
        return new NativeSqlCalc.CompositePredicateInfo("OR", atoms);
    }

    @Test public void testFkPushdownLeavesWideMemberListsAlone()
        throws Exception
    {
        // Repeating a wide list inside the subquery doubles the statement
        // towards the server's query size limit and prunes little.
        final NativeSqlCalc.PredicateInfo wide = manufacturers(500);
        final NativeSqlFactJoins.Rebase r =
            rebaseStock(stockDataSource(), wide);

        assertEquals(
            wide.render(null).replace("f.manufacturer", "nscd0.`manufacturer`"),
            r.placeholders.get("whereClause"));
    }

    @Test public void testFkPushdownBudgetIsPerRenderedCondition()
        throws Exception
    {
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"),
            manufacturers(500));

        // the week is still pushed next to the wide list …
        assertTrue(
            r.placeholders.get("whereClause")
                .endsWith(" AND " + WEEK_PUSHDOWN),
            r.placeholders.get("whereClause"));
        // … and a list that fits keeps its pushdown
        assertTrue(
            rebaseStock(stockDataSource(), manufacturers(50))
                .placeholders.get("whereClause")
                .contains(" AND f.`sku_key` IN (SELECT nscs0.`sku_key`"));
    }

    @Test public void testFkPushdownDisabledRendersLegacyWhere()
        throws Exception
    {
        final NativeSqlFactJoins.Rebase r = rebaseStock(
            false, clickHouseDialect(), stockDataSource(),
            periodAtom("Неделя", "week_start", "= '2026-09-07'"));

        assertEquals(
            "nscd0.`week_start` = '2026-09-07'",
            r.placeholders.get("whereClause"));
        assertEquals(1, r.predicates.size());
    }

    @Test public void testFkPushdownIsOffByDefault() {
        org.junit.jupiter.api.Assertions.assertFalse(
            mondrian.olap.MondrianProperties.instance()
                .NativeSqlFactJoinsFkPushdown.get());
    }

    // The subquery reads the dim table a second time and builds an IN
    // set: neither is the JOIN, so where the two can differ, no pushdown.

    private static final String WEEK_ONLY =
        "nscd0.`week_start` = '2026-09-07'";

    private static String weekWhere(Dialect dialect, DataSource ds) {
        return rebaseStock(
            true, dialect, ds,
            periodAtom("Неделя", "week_start", "= '2026-09-07'"))
            .placeholders.get("whereClause");
    }

    @Test public void testFkPushdownDeclinesUnderAnInSetLimit()
        throws Exception
    {
        // max_rows_in_set with set_overflow_mode = 'break' cuts the key
        // set short without an error
        final DataSource ds = stockDataSource();
        inSetLimits(ds, 1);

        assertEquals(WEEK_ONLY, weekWhere(clickHouseDialect(), ds));
    }

    @Test public void testFkPushdownDeclinesWhenInSetLimitsAreUnreadable()
        throws Exception
    {
        final DataSource ds = stockDataSource();
        when(ds.getConnection().createStatement())
            .thenThrow(new java.sql.SQLException("no access"));

        assertEquals(WEEK_ONLY, weekWhere(clickHouseDialect(), ds));
    }

    @Test public void testFkPushdownNeedsNoSetLimitProbeElsewhere()
        throws Exception
    {
        final Dialect mysql = mock(Dialect.class);
        when(mysql.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.MYSQL);
        when(mysql.quoteIdentifier(anyString()))
            .thenAnswer(inv -> "`" + inv.getArgument(0) + "`");
        final DataSource ds = stockDataSource();
        inSetLimits(ds, 1);

        assertEquals(
            WEEK_ONLY + " AND " + WEEK_PUSHDOWN, weekWhere(mysql, ds));
    }

    @Test public void testFkPushdownDeclinesADimThatIsNoPlainTable()
        throws Exception
    {
        // a view may return other rows on its second read
        for (String type : Arrays.asList("VIEW", "DICTIONARY", null)) {
            final DataSource ds = stockDataSource();
            tableType(ds, "dim_period", type);
            assertEquals(
                WEEK_ONLY, weekWhere(clickHouseDialect(), ds), "" + type);
        }
    }

    @Test public void testFkPushdownDeclinesASourceThatIsNoPlainTable()
        throws Exception
    {
        // the shards of a Distributed source resolve the subquery's table
        final DataSource ds = stockDataSource();
        tableType(ds, "mart_stock", "REMOTE TABLE");

        assertEquals(WEEK_ONLY, weekWhere(clickHouseDialect(), ds));
    }

    @Test public void testFkPushdownDeclinesAnUntypedColumn()
        throws Exception
    {
        // no type name, so the Enum default cannot be ruled out
        final DataSource ds = typedColumnsDataSource(
            table("mart_stock", "period_start", "qty"),
            typedTable(
                "dim_period", "period_start", "Date", "week_start", null));

        assertEquals(WEEK_ONLY, weekWhere(clickHouseDialect(), ds));
    }

    @Test public void testFkPushdownLeavesFactBoundPredicatesAlone()
        throws Exception
    {
        // Column present on the source: f-bound, no join, no pushdown.
        final DataSource ds = columnsDataSource(
            table("mart_stock", "period_start", "week_start", "qty"),
            table("dim_period", "period_start", "week_start"));

        final NativeSqlFactJoins.Rebase r = rebaseStock(
            ds, periodAtom("Неделя", "week_start", "= '2026-09-07'"));

        assertEquals(
            "f.week_start = '2026-09-07'",
            r.placeholders.get("whereClause"));
        assertEquals("", r.placeholders.get("factJoins"));
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
        when(dimTable.getJoinCondition()).thenReturn(condition);
        when(condition.getLeft()).thenReturn(fk);
        when(condition.getRight()).thenReturn(pk);
        return column;
    }

    private static Map.Entry<String, List<String>> table(
        String name, String... columns)
    {
        return new java.util.AbstractMap.SimpleImmutableEntry<
            String, List<String>>(name, Arrays.asList(columns));
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
        for (Map.Entry<String, List<String>> t : tables) {
            tableType(dataSource, t.getKey(), "TABLE");
        }
        inSetLimits(dataSource, 0);
        return dataSource;
    }

    /** What {@code getTables} reports for the table; null reports nothing. */
    private static void tableType(
        DataSource dataSource, String table, String type)
        throws Exception
    {
        final ResultSet tables = mock(ResultSet.class);
        when(tables.next()).thenReturn(type != null, false);
        when(tables.getString("TABLE_TYPE")).thenReturn(type);
        when(dataSource.getConnection().getMetaData()
                .getTables(null, null, table, null))
            .thenReturn(tables);
    }

    /** How many IN set limits the ClickHouse settings probe finds. */
    private static void inSetLimits(DataSource dataSource, long count)
        throws Exception
    {
        final java.sql.Statement statement =
            mock(java.sql.Statement.class);
        final ResultSet settings = mock(ResultSet.class);
        when(settings.next()).thenReturn(true, false);
        when(settings.getLong(1)).thenReturn(count);
        when(statement.executeQuery(anyString())).thenReturn(settings);
        when(dataSource.getConnection().createStatement())
            .thenReturn(statement);
    }

    /** {@code name, type, name, type, …} pairs of one table. */
    private static Map.Entry<String, List<String>> typedTable(
        String name, String... columnsAndTypes)
    {
        return table(name, columnsAndTypes);
    }

    /**
     * Like {@link #columnsDataSource} but tables built with
     * {@link #typedTable} also report {@code TYPE_NAME}.
     */
    @SafeVarargs
    private static DataSource typedColumnsDataSource(
        Map.Entry<String, List<String>> untyped,
        Map.Entry<String, List<String>>... typed)
        throws Exception
    {
        final DataSource dataSource = columnsDataSource(untyped);
        final DatabaseMetaData metaData =
            dataSource.getConnection().getMetaData();
        for (Map.Entry<String, List<String>> t : typed) {
            final List<String> names = new java.util.ArrayList<String>();
            final List<String> types = new java.util.ArrayList<String>();
            for (int i = 0; i < t.getValue().size(); i += 2) {
                names.add(t.getValue().get(i));
                types.add(t.getValue().get(i + 1));
            }
            final ResultSet columns = columnsResultSet(names);
            when(columns.getString("TYPE_NAME")).thenReturn(
                types.get(0),
                types.subList(1, types.size()).toArray(new String[0]));
            when(metaData.getColumns(null, null, t.getKey(), null))
                .thenReturn(columns);
            tableType(dataSource, t.getKey(), "TABLE");
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
        when(resultSet.getString("TYPE_NAME")).thenReturn("String");
        return resultSet;
    }

    private static Dialect clickHouseDialect() {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
        when(dialect.quoteIdentifier(anyString()))
            .thenAnswer(inv -> "`" + inv.getArgument(0) + "`");
        return dialect;
    }
}
