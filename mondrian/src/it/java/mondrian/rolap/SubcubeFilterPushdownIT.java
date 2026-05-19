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

    private int outerAxisCount(String mdx) {
        return getTestContext().executeQuery(mdx)
            .getAxes()[0].getPositions().size();
    }
}
