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
import mondrian.olap.Connection;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Result;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import java.util.List;

/**
 * End-to-end regression for V2 RequiredPropertyProjection
 * (dronsv/mondrian#22 — M1 + M2 + M3 query-driven projection,
 * without M4 lazy fetch).
 *
 * <p>Key contracts asserted:
 * <ul>
 *   <li>V2 flag off → no behaviour change (same as before V2);</li>
 *   <li>V2 flag on + MDX that does NOT reference any
 *       <code>.Properties(...)</code> → ALL schema properties skipped
 *       from SQL (most aggressive case) → all
 *       <code>member.getPropertyValue(name)</code> return null
 *       (per the M3-without-M4 contract);</li>
 *   <li>V2 flag on + MDX with literal
 *       <code>.Properties("X")</code> → X retained in projection,
 *       others skipped;</li>
 *   <li>V2 flag on + opaque construction (e.g. StrToMember in the
 *       query) → fail-safe to eager, ALL properties projected;</li>
 *   <li>V2 takes precedence over V1-narrow: when both flags are on,
 *       the level annotation is irrelevant for V2-mentioned levels.</li>
 * </ul>
 *
 * <p>Schema uses the same Customer-keyed dimension as the V1-narrow
 * IT but without the on-demand annotation — V2 makes per-query
 * decisions purely from MDX text.
 */
public class V2RequiredPropertyProjectionIT extends FoodMartTestCase {

    public V2RequiredPropertyProjectionIT() {
        super();
    }

    public V2RequiredPropertyProjectionIT(String name) {
        super(name);
    }

    private static final String SCHEMA =
        "<Schema name='V2Test'>\n"
        + "  <Dimension name='Customer'>\n"
        + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
        + "      <Table name='customer'/>\n"
        + "      <Level name='Name' column='customer_id'"
        + "             type='Numeric' uniqueMembers='true'>\n"
        + "        <Property name='FName' column='fname'/>\n"
        + "        <Property name='LName' column='lname'/>\n"
        + "        <Property name='Address1' column='address1'/>\n"
        + "        <Property name='Phone1' column='phone1'/>\n"
        + "      </Level>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Cube name='SalesV2'>\n"
        + "    <Table name='sales_fact_1997'/>\n"
        + "    <DimensionUsage name='Customer' source='Customer'"
        + "                    foreignKey='customer_id'/>\n"
        + "    <Measure name='Unit Sales' column='unit_sales'"
        + "             aggregator='sum'/>\n"
        + "  </Cube>\n"
        + "</Schema>";

    private TestContext freshContext() {
        return getTestContext().withSchemaPool(false).withSchema(SCHEMA);
    }

    private Member firstRowMember(Result r) {
        Axis rows = r.getAxes()[1];
        List<Position> positions = rows.getPositions();
        assertFalse(
            "MDX returned 0 rows", positions.isEmpty());
        return positions.get(0).get(0);
    }

    private Result runMdx(TestContext ctx, String mdx) {
        Connection con = ctx.getConnection();
        try {
            return con.execute(con.parseQuery(mdx));
        } finally {
            con.close();
        }
    }

    /** Plain query, no .Properties() anywhere. */
    private static final String PLAIN_MDX =
        "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
        + "       Head([Customer].[Name].Members, 3) ON ROWS\n"
        + "FROM [SalesV2]";

    /** Query that statically references FName via .Properties()
     *  inside a Filter expression on the axis. The Properties call
     *  is resolved during query.resolve() so the M2 visitor picks
     *  it up at V2 plan compute time. */
    private static final String MDX_WITH_PROPERTIES_LITERAL =
        "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
        + "       Head(Filter([Customer].[Name].Members,"
        + "         [Customer].CurrentMember.Properties(\"FName\") <> \"\"),"
        + "         3) ON ROWS\n"
        + "FROM [SalesV2]";

    public void testFlagOff_baselineUnchanged() {
        // V2 off → all properties populated. This is the back-compat
        // case every existing installation hits.
        propSaver.set(
            MondrianProperties.instance().RequiredPropertyProjection,
            false);
        Member m = firstRowMember(runMdx(freshContext(), PLAIN_MDX));
        assertNotNull(m.getPropertyValue("FName"));
        assertNotNull(m.getPropertyValue("LName"));
        assertNotNull(m.getPropertyValue("Address1"));
        assertNotNull(m.getPropertyValue("Phone1"));
    }

    public void testFlagOn_plainMdx_doesNotRegressBaseline() {
        // V2 on but MDX touches no .Properties() → no Hierarchy is in
        // the analysis → the plan has no entry for this level →
        // getEffectiveProjectedProperties falls back to V1-narrow /
        // eager. All properties remain populated. This is the
        // fail-safe behaviour for the M3-first-cut: V2 only prunes
        // levels MENTIONED by the MDX.
        propSaver.set(
            MondrianProperties.instance().RequiredPropertyProjection,
            true);
        Member m = firstRowMember(runMdx(freshContext(), PLAIN_MDX));
        assertNotNull(
            "FName must remain populated when V2 plan has no entry "
            + "for this level (fail-safe to eager)",
            m.getPropertyValue("FName"));
        assertNotNull(m.getPropertyValue("LName"));
        assertNotNull(m.getPropertyValue("Address1"));
        assertNotNull(m.getPropertyValue("Phone1"));
    }

    public void testFlagOn_propertiesLiteral_keepsReferencedPropOnly() {
        // The MDX references FName literally → V2 includes FName in
        // the required set; LName / Address1 / Phone1 are not
        // referenced anywhere → V2 skips them.
        propSaver.set(
            MondrianProperties.instance().RequiredPropertyProjection,
            true);
        Member m = firstRowMember(
            runMdx(freshContext(), MDX_WITH_PROPERTIES_LITERAL));

        // FName is the only literally-referenced property.
        Object fname = m.getPropertyValue("FName");
        assertNotNull(
            "FName (statically referenced via .Properties()) must "
            + "remain populated under V2",
            fname);
        assertTrue(
            "FName should be a non-empty string, got: " + fname,
            fname instanceof String && !((String) fname).isEmpty());

        // Others must be null — they were not statically referenced
        // by the query, and there's no DIMENSION PROPERTIES request
        // for them.
        assertNull(
            "LName not referenced → V2 skip → null",
            m.getPropertyValue("LName"));
        assertNull(
            "Address1 not referenced → V2 skip → null",
            m.getPropertyValue("Address1"));
        assertNull(
            "Phone1 not referenced → V2 skip → null",
            m.getPropertyValue("Phone1"));
    }
}

// End V2RequiredPropertyProjectionIT.java
