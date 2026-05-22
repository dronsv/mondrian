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

import mondrian.olap.CacheControl;
import mondrian.olap.Cube;
import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;

// RolapUtil is in the same package; explicit import is unnecessary
// but kept for clarity since the IT depends on RolapUtil.setHook.
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Regression coverage for emondrian-clickhouse#77 — Excel-style
 * {@code FROM (SELECT Filter(... InStr(... member_caption ...)) ...)}
 * subselect must restrict the outer axis.
 *
 * <p>The control variant (direct axis Filter, no subselect) is the
 * upstream-correct baseline. The subselect variants exercise the
 * dynamic-expression pushdown path in
 * {@link mondrian.olap.Query#getSubcubePredicates}.
 */
public class SubcubeFilterPushdownIT extends FoodMartTestCase {

    public SubcubeFilterPushdownIT() {
    }

    public SubcubeFilterPushdownIT(String name) {
        super(name);
    }

    private static final String SUBSTRING = "Carrots";

    private static final String DIRECT_AXIS_FILTER_MDX =
        "SELECT NON EMPTY Filter("
        + "  [Product].[Product Name].AllMembers, "
        + "  InStr(1, [Product].CurrentMember.Properties("
        + "    \"MEMBER_CAPTION\"), \"" + SUBSTRING + "\") > 0"
        + ") ON 0 FROM [Sales] WHERE [Measures].[Unit Sales]";

    private static final String SUBSELECT_FILTER_MDX =
        "SELECT NON EMPTY [Product].[Product Name].AllMembers ON 0 "
        + "FROM (SELECT Filter("
        + "  [Product].[Product Name].AllMembers, "
        + "  InStr(1, [Product].CurrentMember.Properties("
        + "    \"MEMBER_CAPTION\"), \"" + SUBSTRING + "\") > 0"
        + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

    private static final String EMPTY_SUBSELECT_FILTER_MDX =
        "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
        + "FROM (SELECT Filter("
        + "  [Product].[Product Name].Members, "
        + "  InStr(1, [Product].CurrentMember.Properties("
        + "    \"MEMBER_CAPTION\"), \"__NO_SUCH_PRODUCT__\") > 0"
        + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

    private static final String FULL_AXIS_MDX =
        "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
        + "FROM [Sales] WHERE [Measures].[Unit Sales]";

    /**
     * Excel-style subselect with NQE enabled — outer axis must be
     * restricted to the Filter result, matching the direct-axis baseline.
     */
    public void testSubselectFilterInStrRestrictsOuterAxisWithNqe()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        int baseline = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int fullAxis = outerAxisCount(FULL_AXIS_MDX);
        assertTrue(
            "Substring '" + SUBSTRING + "' must select a proper subset "
                + "of the Product Name level — baseline=" + baseline
                + " fullAxis=" + fullAxis,
            baseline > 0 && baseline < fullAxis);

        int subselect = outerAxisCount(SUBSELECT_FILTER_MDX);
        assertEquals(
            "Subselect outer axis must equal direct-axis baseline "
                + "(both should be the InStr-restricted subset)",
            baseline, subselect);
    }

    /**
     * Same subselect with NQE disabled — legacy SqlTupleReader /
     * Segment.load path must honor the same subcube predicate via
     * SqlConstraintUtils.addSubcubeConstraint.
     */
    public void testSubselectFilterInStrRestrictsOuterAxisWithoutNqe()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, false);

        int baseline = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int subselect = outerAxisCount(SUBSELECT_FILTER_MDX);
        assertEquals(
            "Subselect outer axis must equal direct-axis baseline "
                + "under NQE-off legacy path",
            baseline, subselect);
    }

    /**
     * Control: direct-axis Filter (no subselect). This path already
     * works in upstream and must continue to return the same matching
     * subset that the subselect form is now expected to return.
     */
    public void testDirectAxisFilterInStrIsBaseline() throws Exception {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        int matching = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int fullAxis = outerAxisCount(FULL_AXIS_MDX);
        assertTrue(
            "Direct axis Filter must restrict to a proper subset "
                + "(matching=" + matching + " fullAxis=" + fullAxis + ")",
            matching > 0 && matching < fullAxis);
    }

    /**
     * TopCount subselect — outer axis must be restricted to the top
     * N members chosen by the subselect.
     */
    public void testSubselectTopCountRestrictsOuterAxis() throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final int topN = 5;
        final String mdx =
            "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
            + "FROM (SELECT TopCount("
            + "  [Product].[Product Name].Members, " + topN + ", "
            + "  [Measures].[Unit Sales]"
            + ") ON 0 FROM [Sales]) "
            + "WHERE [Measures].[Unit Sales]";

        int actual = outerAxisCount(mdx);
        int fullAxis = outerAxisCount(FULL_AXIS_MDX);
        assertTrue(
            "TopCount subselect must restrict outer axis (actual="
                + actual + " topN=" + topN + " fullAxis=" + fullAxis + ")",
            actual > 0 && actual <= topN && actual < fullAxis);
    }

    /**
     * NonEmpty(Filter(...)) subselect — outer axis must be restricted
     * to a subset of the Filter baseline.
     */
    public void testSubselectNonEmptyRestrictsOuterAxis() throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final String mdx =
            "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
            + "FROM (SELECT NonEmpty("
            + "  Filter("
            + "    [Product].[Product Name].Members, "
            + "    InStr(1, [Product].CurrentMember.Properties("
            + "      \"MEMBER_CAPTION\"), \"" + SUBSTRING + "\") > 0"
            + "  ), [Measures].[Unit Sales]"
            + ") ON 0 FROM [Sales]) "
            + "WHERE [Measures].[Unit Sales]";

        int baseline = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int actual = outerAxisCount(mdx);
        assertTrue(
            "NonEmpty(Filter(...)) subselect must restrict outer axis "
                + "to at most the Filter baseline (actual=" + actual
                + " baseline=" + baseline + ")",
            actual > 0 && actual <= baseline);
    }

    /**
     * Empty dynamic subselect sets must be a contradiction, not "no
     * constraint". Otherwise a label filter with no matches returns the
     * full outer axis.
     */
    public void testSubselectFilterWithNoMatchesReturnsEmptyAxis()
        throws Exception
    {
        assertEmptySubselectFilter(true);
        assertEmptySubselectFilter(false);
    }

    /**
     * NonEmpty's second argument is semantically meaningful. It must be
     * evaluated, not replaced by the first argument's predicate.
     */
    public void testSubselectNonEmptyWithNullMeasureReturnsEmptyAxis()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final String mdx =
            "WITH MEMBER [Measures].[Always Null] AS 'NULL' "
            + "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
            + "FROM (SELECT NonEmpty("
            + "  [Product].[Product Name].Members, "
            + "  [Measures].[Always Null]"
            + ") ON 0 FROM [Sales]) "
            + "WHERE [Measures].[Unit Sales]";

        assertEquals(
            "NonEmpty(set, null measure) subselect must produce an "
                + "empty outer axis",
            0,
            outerAxisCount(mdx));
    }

    private void assertEmptySubselectFilter(boolean nativeQueryEngineEnabled) {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable,
            nativeQueryEngineEnabled);
        assertEquals(
            "Empty subselect Filter must produce an empty outer axis "
                + "(NQE=" + nativeQueryEngineEnabled + ")",
            0,
            outerAxisCount(EMPTY_SUBSELECT_FILTER_MDX));
    }

    // ---------------------------------------------------------------
    // SQL-emission tests for the InStr static handler (#77 perf
    // follow-up). These verify the actual SQL Mondrian emits, not
    // just cell counts — cell counts alone don't distinguish the
    // optimized path from the per-member fallback.
    //
    // These tests use [Store].[Store Name] rather than the
    // [Product].[Product Name] used by the cell-count tests above
    // because FoodMart's [Product] hierarchy is declared with a
    // snowflake <Join> (product joined with product_class), which
    // the static handler skips by design (see spec: snowflake
    // levels are phased follow-up #5). [Store] is a single flat
    // <Table name="store"/> so the static handler engages.
    // ---------------------------------------------------------------

    private static final String STORE_SUBSTRING = "Store 1";
    private static final String STORE_EXACT = "Store 11";
    private static final String STORE_CAPTION_EXPR =
        "[Store].CurrentMember.Properties(\"MEMBER_CAPTION\")";
    private static final String STORE_NAME_EXPR =
        "[Store].CurrentMember.Name";

    private static final String STORE_DIRECT_AXIS_FILTER_MDX =
        "SELECT NON EMPTY Filter("
        + "  [Store].[Store Name].AllMembers, "
        + "  InStr(1, " + STORE_CAPTION_EXPR + ", \""
        + STORE_SUBSTRING + "\") > 0"
        + ") ON 0 FROM [Sales] WHERE [Measures].[Unit Sales]";

    private static final String STORE_SUBSELECT_FILTER_MDX =
        "SELECT NON EMPTY [Store].[Store Name].AllMembers ON 0 "
        + "FROM (SELECT Filter("
        + "  [Store].[Store Name].AllMembers, "
        + "  InStr(1, " + STORE_CAPTION_EXPR + ", \""
        + STORE_SUBSTRING + "\") > 0"
        + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

    /**
     * Excel-style Filter(InStr(member_caption)) subselect on a
     * single-table dim level must be resolved by the static InStr
     * handler — one SQL subquery predicate against the dim table with
     * an INSTR-style WHERE clause, no per-member SqlTupleReader probes.
     */
    public void testInStrSubselectIsBatchedSql() throws Exception {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final SqlCaptureHook hook = new SqlCaptureHook();
        RolapUtil.setHook(hook);
        try {
            int subselect = outerAxisCount(STORE_SUBSELECT_FILTER_MDX);
            int baseline =
                outerAxisCount(STORE_DIRECT_AXIS_FILTER_MDX);
            assertEquals(
                "Subselect outer axis must equal direct-axis baseline",
                baseline, subselect);

            // SQL hook must show the InStr static handler ran:
            // at least one SELECT against the store dim table with
            // an INSTR / positionUTF8 / LOCATE / POSITION predicate
            // on the name column.
            int positionSqls = hook.countMatchingSubstring(
                STORE_SUBSELECT_FILTER_MDX,
                Pattern.compile(
                    "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                        + "\\s*\\(.*['\"]" + STORE_SUBSTRING + "['\"]"));
            assertTrue(
                "Expected the InStr static handler to emit at least "
                    + "one position-fn SQL during the subselect query "
                    + "(captured SQL count by substring="
                    + positionSqls + "): " + hook.getSqlQueries(),
                positionSqls >= 1);
        } finally {
            RolapUtil.setHook(null);
        }
    }

    /**
     * Excel label-filter variants over caption should use the same
     * static handler as the original "contains" case. This covers
     * positive and negative forms that would otherwise enumerate the
     * whole level member-by-member.
     */
    public void testCaptionSubselectLabelVariantsAreBatchedSql()
        throws Exception
    {
        assertStoreSubselectMatchesDirectAndEmitsSql(
            "contains <> 0",
            "InStr(1, " + STORE_CAPTION_EXPR + ", \""
                + STORE_SUBSTRING + "\") <> 0",
            Pattern.compile(
                "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                    + "\\s*\\(.*['\"]" + STORE_SUBSTRING + "['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "contains case-insensitive",
            "InStr(1, " + STORE_CAPTION_EXPR + ", \""
                + STORE_SUBSTRING.toLowerCase(Locale.ROOT) + "\") > 0",
            Pattern.compile(
                "(?is)(LOWER|lowerUTF8).*['\"]"
                    + STORE_SUBSTRING.toLowerCase(Locale.ROOT) + "['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "does not contain",
            "InStr(1, " + STORE_CAPTION_EXPR + ", \""
                + STORE_SUBSTRING + "\") = 0",
            Pattern.compile(
                "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                    + "\\s*\\(.*['\"]" + STORE_SUBSTRING + "['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "equals",
            STORE_NAME_EXPR + " = \"" + STORE_EXACT + "\"",
            Pattern.compile(
                "(?i)=\\s*['\"]" + STORE_EXACT + "['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "does not equal",
            STORE_NAME_EXPR + " <> \"" + STORE_EXACT + "\"",
            Pattern.compile(
                "(?i)=\\s*['\"]" + STORE_EXACT + "['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "begins with",
            "Left(" + STORE_NAME_EXPR + ", Len(\"Store\")) = \"Store\"",
            Pattern.compile(
                "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                    + "\\s*\\(.*['\"]Store['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "does not begin with",
            "Left(" + STORE_NAME_EXPR + ", Len(\""
                + STORE_EXACT + "\")) <> \"" + STORE_EXACT + "\"",
            Pattern.compile(
                "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                    + "\\s*\\(.*['\"]" + STORE_EXACT + "['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "ends with",
            "Right(" + STORE_NAME_EXPR + ", Len(\"1\")) = \"1\"",
            Pattern.compile("(?i)(RIGHT|endsWith)\\s*\\(.*['\"]1['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "does not end with",
            "Right(" + STORE_NAME_EXPR + ", Len(\"11\")) <> \"11\"",
            Pattern.compile("(?i)(RIGHT|endsWith)\\s*\\(.*['\"]11['\"]"));
    }

    /**
     * Range-style label filters are represented as caption comparisons,
     * commonly joined by AND for "between". The static handler should
     * batch those too when both sides constrain the same caption.
     */
    public void testCaptionSubselectRangeIsBatchedSql() throws Exception {
        assertStoreSubselectMatchesDirectAndEmitsSql(
            "between",
            STORE_NAME_EXPR + " >= \"Store 1\" AND "
                + STORE_NAME_EXPR + " <= \"Store 9\"",
            Pattern.compile(
                "(?is)>=\\s*['\"]Store 1['\"].*AND.*<=\\s*['\"]Store 9['\"]"));

        assertStoreSubselectMatchesDirectAndEmitsSql(
            "not between",
            STORE_NAME_EXPR + " < \"" + STORE_EXACT + "\" OR "
                + STORE_NAME_EXPR + " > \"" + STORE_EXACT + "\"",
            Pattern.compile(
                "(?is)>=\\s*['\"]" + STORE_EXACT
                    + "['\"].*AND.*<=\\s*['\"]" + STORE_EXACT + "['\"]"));
    }

    /**
     * Compound condition (InStr AND measure threshold) must NOT
     * match the static caption handler; falls through to the dynamic
     * evalFallbackDisjunction which still produces a correct subset.
     * Guards against the static handler being too greedy.
     */
    public void testInStrSubselectFallsBackOnUnsupportedShape()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        // Compound on a single-table dim (Store): the caption handler
        // would otherwise engage on Store; the measure clause forces it
        // to bail. Behavioral assertion: outer axis is still
        // correctly restricted by the fallback.
        final String compoundMdx =
            "SELECT NON EMPTY [Store].[Store Name].AllMembers ON 0 "
            + "FROM (SELECT Filter("
            + "  [Store].[Store Name].AllMembers, "
            + "  InStr(1, [Store].CurrentMember.Properties("
            + "    \"MEMBER_CAPTION\"), \"" + STORE_SUBSTRING + "\") > 0 "
            + "  AND [Measures].[Unit Sales] > 0"
            + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

        final SqlCaptureHook hook = new SqlCaptureHook();
        RolapUtil.setHook(hook);
        try {
            int actual = outerAxisCount(compoundMdx);
            int baseline =
                outerAxisCount(STORE_DIRECT_AXIS_FILTER_MDX);
            assertTrue(
                "Compound filter must restrict to at most the simple "
                    + "Filter baseline (actual=" + actual
                    + " baseline=" + baseline + ")",
                actual <= baseline);

            // The static handler must NOT have emitted an INSTR-on-dim
            // SQL for this compound query — the AND clause means
            // matchCaptionFilterCondition returns null and the dynamic
            // fallback runs instead. Captured SQL hook should show
            // zero INSTR statements for this STORE_SUBSTRING in this
            // isolated test (this test method only runs ONE query).
            int positionSqls = hook.countMatchingSubstring(
                compoundMdx,
                Pattern.compile(
                    "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                        + "\\s*\\(.*['\"]" + STORE_SUBSTRING
                        + "['\"]"));
            assertEquals(
                "Compound subselect must NOT trigger the static "
                    + "caption handler (captured: "
                    + hook.getSqlQueries() + ")",
                0, positionSqls);
        } finally {
            RolapUtil.setHook(null);
        }
    }

    /**
     * Empty match via the static handler — the SQL subquery returns zero
     * keys, therefore the outer axis is empty. Uses [Store].[Store Name]
     * (flat table) so the static handler engages.
     */
    public void testInStrSubselectEmptySubstringReturnsEmptyAxis()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final String emptyStoreMdx =
            "SELECT NON EMPTY [Store].[Store Name].AllMembers ON 0 "
            + "FROM (SELECT Filter("
            + "  [Store].[Store Name].AllMembers, "
            + "  InStr(1, [Store].CurrentMember.Properties("
            + "    \"MEMBER_CAPTION\"), \"__NO_SUCH_STORE__\") > 0"
            + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

        final SqlCaptureHook hook = new SqlCaptureHook();
        RolapUtil.setHook(hook);
        try {
            assertEquals(
                "Empty substring match must produce an empty outer axis",
                0,
                outerAxisCount(emptyStoreMdx));

            int positionSqls = hook.countMatchingSubstring(
                emptyStoreMdx,
                Pattern.compile(
                    "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                        + "\\s*\\(.*__NO_SUCH_STORE__"));
            assertTrue(
                "InStr static handler must have emitted a SQL subquery "
                    + "for the no-match substring (positionSqls="
                    + positionSqls + "): " + hook.getSqlQueries(),
                positionSqls >= 1);
        } finally {
            RolapUtil.setHook(null);
        }
    }

    /**
     * Non-unique levels are out-of-scope for the static caption handler
     * (the key-only predicate would under-restrict). Handler returns
     * null; fallback path produces the correct result.
     *
     * <p>FoodMart's {@code [Store].[Store City]} is declared with
     * {@code uniqueMembers="false"} (cities repeat across states).
     * Filtering it with InStr triggers the non-unique-level guard.
     */
    public void testInStrSubselectOnNonUniqueLevelFallsBack()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final String mdx =
            "SELECT NON EMPTY [Store].[Store City].Members ON 0 "
            + "FROM (SELECT Filter("
            + "  [Store].[Store City].Members, "
            + "  InStr(1, [Store].CurrentMember.Properties("
            + "    \"MEMBER_CAPTION\"), \"port\") > 0"
            + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

        final SqlCaptureHook hook = new SqlCaptureHook();
        RolapUtil.setHook(hook);
        try {
            // Behavioral check — the fallback path must produce the
            // restricted set, not the full level. We assert the
            // outer axis is smaller than an unfiltered baseline.
            final String fullCity =
                "SELECT NON EMPTY [Store].[Store City].Members "
                + "ON 0 FROM [Sales] WHERE [Measures].[Unit Sales]";
            int restricted = outerAxisCount(mdx);
            int full = outerAxisCount(fullCity);
            assertTrue(
                "Non-unique level fallback must still restrict "
                    + "(restricted=" + restricted + " full=" + full + ")",
                restricted > 0 && restricted < full);

            // The static handler should NOT have emitted an INSTR on
            // the store dim — the unique-level guard returned null
            // and the dynamic fallback ran instead.
            int positionSqls = hook.countMatchingSubstring(
                mdx,
                Pattern.compile(
                    "(?i)(INSTR|POSITION|LOCATE|positionUTF8)"
                        + "\\s*\\(.*'port'"));
            assertEquals(
                "Caption static handler must NOT run on non-unique level. "
                    + "Captured: " + hook.getSqlQueries(),
                0, positionSqls);
        } finally {
            RolapUtil.setHook(null);
        }
    }

    private int outerAxisCount(String mdx) {
        return getTestContext().executeQuery(mdx)
            .getAxes()[0].getPositions().size();
    }

    private void assertStoreSubselectMatchesDirectAndEmitsSql(
        String label,
        String condition,
        Pattern sqlPattern)
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);
        propSaver.set(
            MondrianProperties.instance().DisableCaching, true);
        propSaver.set(
            MondrianProperties.instance().DisableLocalSegmentCache, true);

        final String directMdx = storeDirectAxisFilter(condition);
        final String subselectMdx = storeSubselectFilter(condition);
        final int baseline = outerAxisCount(directMdx);
        assertTrue(
            "Direct-axis baseline must be non-empty for " + label,
            baseline > 0);

        flushMeasureCache();
        final SqlCaptureHook hook = new SqlCaptureHook();
        RolapUtil.setHook(hook);
        try {
            final int subselect = outerAxisCount(subselectMdx);
            assertEquals(
                "Subselect outer axis must equal direct-axis baseline for "
                    + label,
                baseline,
                subselect);
            final int matchedSqls =
                hook.countMatchingSubstring(subselectMdx, sqlPattern);
            assertTrue(
                "Expected static caption-filter SQL for " + label
                    + " (matchedSqls=" + matchedSqls + "): "
                    + hook.getSqlQueries(),
                matchedSqls >= 1);
        } finally {
            RolapUtil.setHook(null);
        }
    }

    private void flushMeasureCache() {
        final CacheControl cacheControl = getTestContext().getCacheControl();
        for (Cube cube : getTestContext().getConnection().getSchema()
            .getCubes())
        {
            cacheControl.flush(cacheControl.createMeasuresRegion(cube));
        }
    }

    private static String storeDirectAxisFilter(String condition) {
        return "SELECT NON EMPTY Filter("
            + "  [Store].[Store Name].AllMembers, "
            + "  " + condition
            + ") ON 0 FROM [Sales] WHERE [Measures].[Unit Sales]";
    }

    private static String storeSubselectFilter(String condition) {
        return "SELECT NON EMPTY [Store].[Store Name].AllMembers ON 0 "
            + "FROM (SELECT Filter("
            + "  [Store].[Store Name].AllMembers, "
            + "  " + condition
            + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";
    }

    /**
     * Capture-only SQL hook. Skips {@code select count(*)} probes
     * (mirrors the pattern in DataSourceChangeListenerTest$SqlLogger).
     */
    private static final class SqlCaptureHook
        implements RolapUtil.ExecuteQueryHook
    {
        private final List<String> sqlQueries = new ArrayList<String>();

        @Override
        public synchronized void onExecuteQuery(String sql) {
            if (sql == null) {
                return;
            }
            if (sql.startsWith("select count(")) {
                return;
            }
            sqlQueries.add(sql);
        }

        synchronized List<String> getSqlQueries() {
            return new ArrayList<String>(sqlQueries);
        }

        /**
         * Counts statements matching the given regex. The {@code mdx}
         * parameter is unused but kept in the signature so test sites
         * can document which MDX shape they're correlating against.
         */
        synchronized int countMatchingSubstring(
            String mdx, Pattern pattern)
        {
            int count = 0;
            for (String sql : sqlQueries) {
                if (pattern.matcher(sql).find()) {
                    count++;
                }
            }
            return count;
        }
    }
}
