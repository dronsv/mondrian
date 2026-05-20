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

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;

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

    private int outerAxisCount(String mdx) {
        return getTestContext().executeQuery(mdx)
            .getAxes()[0].getPositions().size();
    }
}
