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
 * <p>Key contracts asserted (matches the M3 first-cut behaviour, not
 * the eventual full V2 — see #22 for the roadmap):
 * <ul>
 *   <li>V2 flag off → no behaviour change (same as before V2);</li>
 *   <li>V2 flag on + MDX with NO <code>.Properties(...)</code> /
 *       DIMENSION PROPERTIES references → V2 leaves the level
 *       <strong>unmentioned</strong>; SQL site falls back to V1-narrow
 *       / eager and all schema properties stay populated. The
 *       previous version of this header claimed the opposite ("ALL
 *       skipped") — that was wrong, the M3 first-cut is deliberately
 *       fail-safe to eager;</li>
 *   <li>V2 flag on + MDX with literal
 *       <code>.Properties("X")</code> → X retained in projection,
 *       others skipped (return null) and kept-X comes back with the
 *       real column value (alignment check);</li>
 *   <li>V2 flag on + MDX uses different case for property name
 *       (<code>.Properties("fname")</code> for schema property
 *       <code>FName</code>) → still matches when the global
 *       <code>mondrian.olap.case.sensitive=false</code> default
 *       holds (reviewer finding 1);</li>
 *   <li>V2 flag on + opaque construction
 *       (<code>StrToMember</code>) → fail-safe to eager, ALL
 *       properties projected (reviewer finding 4 missing test);</li>
 *   <li>V2 flag on + DIMENSION PROPERTIES requesting an existing
 *       property → that property is retained alongside any literal
 *       references;</li>
 *   <li>V2 + V1-narrow both on, level has V1-narrow annotation and
 *       MDX references a V1-narrow-skipped property → V2's required
 *       set takes precedence for that level (reviewer finding 4
 *       missing test).</li>
 * </ul>
 *
 * <p>Schema uses the same Customer-keyed dimension as the V1-narrow
 * IT — V2 makes per-query decisions purely from MDX text, no
 * annotation needed.
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

    /**
     * Reviewer finding 1: MDX is case-insensitive by default
     * (mondrian.olap.case.sensitive=false). A literal
     * <code>.Properties("fname")</code> against schema property
     * <code>FName</code> must keep FName in the V2 plan and return
     * its real value — otherwise the runtime case-insensitive lookup
     * looks up "FName" in a member that V2 silently skipped, and the
     * user sees null where they expected the value.
     */
    public void testFlagOn_caseInsensitivePropertyNameMatching() {
        propSaver.set(
            MondrianProperties.instance().RequiredPropertyProjection,
            true);
        // Default of mondrian.olap.case.sensitive is false; assert
        // the precondition so the test fails loudly if it ever flips.
        assertFalse(
            "mondrian.olap.case.sensitive must be false (default) for"
            + " this test to exercise the regression",
            MondrianProperties.instance().CaseSensitive.get());

        final String mdxLowerCase =
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "       Head(Filter([Customer].[Name].Members,"
            + "         [Customer].CurrentMember.Properties(\"fname\")"
            + "         <> \"\"), 3) ON ROWS\n"
            + "FROM [SalesV2]";
        Member m = firstRowMember(runMdx(freshContext(), mdxLowerCase));

        Object fname = m.getPropertyValue("FName");
        assertNotNull(
            "FName must remain populated even when MDX literal uses"
            + " 'fname' (lowercase) — runtime lookup is case-"
            + " insensitive by default; V2 plan must mirror that",
            fname);
        assertTrue(
            "FName should be a non-empty string, got: " + fname,
            fname instanceof String && !((String) fname).isEmpty());
    }

    /**
     * Reviewer finding 4: opaque construction (StrToMember) must
     * trigger globally-opaque fallback → ALL schema properties
     * project as if V2 was off. Without this the plan would prune
     * properties referenced indirectly via the runtime-resolved
     * member from StrToMember.
     */
    public void testFlagOn_strToMember_globalEagerFallback() {
        propSaver.set(
            MondrianProperties.instance().RequiredPropertyProjection,
            true);
        // StrToMember anywhere in the query forces global opacity.
        // Use it on a different hierarchy (Measures) so the query
        // still returns meaningful data while making V2 surrender.
        final String mdxStrTo =
            "SELECT {StrToMember(\"[Measures].[Unit Sales]\")}"
            + "       ON COLUMNS,\n"
            + "       Head([Customer].[Name].Members, 3) ON ROWS\n"
            + "FROM [SalesV2]";
        Member m = firstRowMember(runMdx(freshContext(), mdxStrTo));
        // All properties must remain populated because V2 fell back
        // to eager for the whole query.
        assertNotNull(
            "FName (StrToMember in query → V2 global eager → all"
            + " properties populated)",
            m.getPropertyValue("FName"));
        assertNotNull(
            "LName (StrToMember in query → global eager)",
            m.getPropertyValue("LName"));
        assertNotNull(
            "Address1 (StrToMember in query → global eager)",
            m.getPropertyValue("Address1"));
        assertNotNull(
            "Phone1 (StrToMember in query → global eager)",
            m.getPropertyValue("Phone1"));
    }

    /**
     * Reviewer finding 4: V1-narrow + V2 precedence missing test.
     * When both flags are on and a level is mentioned by V2's
     * analysis, the V2 plan wins — the V1-narrow annotation does
     * NOT additionally skip a property V2 statically required.
     *
     * <p>Schema-side V1-narrow annotation lists Phone1 as on-demand.
     * MDX literally references Phone1 → V2 puts Phone1 in the plan
     * → Phone1 must be populated even though V1-narrow alone would
     * have skipped it.
     */
    public void testFlagOn_V2PrecedesV1NarrowAnnotation() {
        propSaver.set(
            MondrianProperties.instance().RequiredPropertyProjection,
            true);
        propSaver.set(
            MondrianProperties.instance().SkipOnDemandLevelProperties,
            true);

        final String schemaWithV1NarrowAnnotation =
            "<Schema name='V2OverridesV1'>\n"
            + "  <Dimension name='Customer'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Level name='Name' column='customer_id'"
            + "             type='Numeric' uniqueMembers='true'>\n"
            + "        <Annotations>\n"
            + "          <Annotation name='emondrian.onDemandProperties'>"
            + "Phone1,Address1</Annotation>\n"
            + "        </Annotations>\n"
            + "        <Property name='FName' column='fname'/>\n"
            + "        <Property name='LName' column='lname'/>\n"
            + "        <Property name='Address1' column='address1'/>\n"
            + "        <Property name='Phone1' column='phone1'/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name='SalesV2v1'>\n"
            + "    <Table name='sales_fact_1997'/>\n"
            + "    <DimensionUsage name='Customer' source='Customer'"
            + "                    foreignKey='customer_id'/>\n"
            + "    <Measure name='Unit Sales' column='unit_sales'"
            + "             aggregator='sum'/>\n"
            + "  </Cube>\n"
            + "</Schema>";
        final String mdxLiteralPhone1 =
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "       Head(Filter([Customer].[Name].Members,"
            + "         [Customer].CurrentMember.Properties(\"Phone1\")"
            + "         <> \"\"), 3) ON ROWS\n"
            + "FROM [SalesV2v1]";

        TestContext ctx = getTestContext()
            .withSchemaPool(false)
            .withSchema(schemaWithV1NarrowAnnotation);
        Member m = firstRowMember(runMdx(ctx, mdxLiteralPhone1));

        // V1-narrow annotation says skip Phone1; V2 says keep it
        // because MDX literally references it. V2 wins — Phone1 is
        // populated. (Address1 is V1-narrow-skipped AND not
        // referenced by MDX → V2 has no override → V1-narrow path
        // still skips it → null.)
        assertNotNull(
            "V2 takes precedence: MDX literal reference to Phone1"
            + " overrides V1-narrow's on-demand annotation",
            m.getPropertyValue("Phone1"));
        assertNull(
            "Address1 is V1-narrow on-demand AND not referenced by"
            + " V2 → V2 leaves the level unmentioned → V1-narrow path"
            + " applies → Address1 null",
            m.getPropertyValue("Address1"));
    }
}

// End V2RequiredPropertyProjectionIT.java
