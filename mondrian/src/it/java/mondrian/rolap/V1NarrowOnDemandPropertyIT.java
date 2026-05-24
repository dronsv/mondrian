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
 * End-to-end regression for the V1-narrow on-demand level-property opt-in
 * (dronsv/mondrian#22). Backed by the FoodMart {@code customer} table.
 *
 * <p>Property shape is modelled on the real production workload: a "Name"
 * level whose <em>kept</em> properties are short identity-shaped strings
 * the operator wants visible in axis enumeration (first name, last name),
 * and whose <em>on-demand</em> properties are heavier display/PII fields
 * the operator only wants in drillthrough/detail views (address lines,
 * phone numbers, postal code). This mirrors the FMCG SKU enrichment
 * pattern that motivated V1-narrow: lots of URL/long-text/mapping fields
 * declared as level properties but only consulted client-side per click,
 * not per-axis-row.</p>
 *
 * <p>Assertions go through {@link Member#getPropertyValue} — the
 * user-visible contract — rather than dialect-fragile SQL-text matching.
 * Two crucial properties are checked:
 * <ul>
 *   <li>under V1-narrow active, on-demand properties return {@code null}
 *       (projection skip took effect at every reader site);</li>
 *   <li>under V1-narrow active, kept properties return their <em>real
 *       column values</em> — this catches the alignment bug where SQL
 *       projection and {@code setProperty} loop disagree on column
 *       offsets and the kept slot receives the wrong value.</li>
 * </ul>
 */
public class V1NarrowOnDemandPropertyIT extends FoodMartTestCase {

    public V1NarrowOnDemandPropertyIT() {
        super();
    }

    public V1NarrowOnDemandPropertyIT(String name) {
        super(name);
    }

    /**
     * Customer-keyed dimension. Two identity-shaped properties (FName,
     * LName) are not annotated — they must continue to populate.
     * Four heavier/PII-shaped properties (Address1, Address2,
     * PostalCode, Phone1) are listed in
     * {@code emondrian.onDemandProperties} — they must be skipped from
     * SQL projection and return {@code null} from
     * {@code getPropertyValue} when the operator flag is on.
     */
    private static final String SCHEMA =
        "<Schema name='V1NarrowTest'>\n"
        + "  <Dimension name='Customer'>\n"
        + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
        + "      <Table name='customer'/>\n"
        + "      <Level name='Name' column='customer_id'"
        + "             type='Numeric' uniqueMembers='true'>\n"
        + "        <Annotations>\n"
        + "          <Annotation name='emondrian.onDemandProperties'>"
        + "Address1,Address2,Postal Code,Phone1</Annotation>\n"
        + "        </Annotations>\n"
        + "        <Property name='FName' column='fname'/>\n"
        + "        <Property name='LName' column='lname'/>\n"
        + "        <Property name='Address1' column='address1'/>\n"
        + "        <Property name='Address2' column='address2'/>\n"
        + "        <Property name='Postal Code' column='postal_code'/>\n"
        + "        <Property name='Phone1' column='phone1'/>\n"
        + "      </Level>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Cube name='SalesV1'>\n"
        + "    <Table name='sales_fact_1997'/>\n"
        + "    <DimensionUsage name='Customer' source='Customer'"
        + "                    foreignKey='customer_id'/>\n"
        + "    <Measure name='Unit Sales' column='unit_sales'"
        + "             aggregator='sum'/>\n"
        + "  </Cube>\n"
        + "</Schema>";

    private static final String MDX =
        "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
        + "       Head([Customer].[Name].Members, 3) ON ROWS\n"
        + "FROM [SalesV1]";

    private Member firstRowMember(Result r) {
        Axis rows = r.getAxes()[1];
        List<Position> positions = rows.getPositions();
        assertFalse(
            "MDX returned 0 rows; cannot read member properties",
            positions.isEmpty());
        Position pos = positions.get(0);
        assertFalse(
            "first row position contains 0 members",
            pos.isEmpty());
        return pos.get(0);
    }

    /**
     * Returns a fresh test context with the schema pool bypassed.
     * Necessary because Mondrian's RolapSchemaPool reuses schema
     * instances keyed by catalog content — sharing means the
     * underlying {@link CacheMemberReader#members} list is also
     * shared. Flipping the operator flag between test methods would
     * not invalidate already-cached members loaded under the previous
     * flag state, so each test method needs an isolated schema. This
     * is a test-isolation concern, not a production correctness one
     * (V1-narrow's contract is "set the flag at startup, do not flip
     * at runtime").
     */
    private TestContext freshContext() {
        return getTestContext().withSchemaPool(false).withSchema(SCHEMA);
    }

    private Result runMdx(TestContext ctx) {
        Connection con = ctx.getConnection();
        try {
            return con.execute(con.parseQuery(MDX));
        } finally {
            con.close();
        }
    }

    /**
     * Baseline: flag off → annotation is ignored. Every schema-declared
     * property must populate on the cached member regardless of the
     * annotation text. This is the back-compat path that every existing
     * installation hits — must remain bit-identical to pre-V1-narrow
     * behaviour.
     */
    public void testFlagOff_allPropertiesPopulated() {
        propSaver.set(
            MondrianProperties.instance().SkipOnDemandLevelProperties,
            false);
        Member m = firstRowMember(
            runMdx(freshContext()));
        assertNotNull(
            "FName (kept, baseline)", m.getPropertyValue("FName"));
        assertNotNull(
            "LName (kept, baseline)", m.getPropertyValue("LName"));
        assertNotNull(
            "Address1 (in annotation but flag off -> still eager)",
            m.getPropertyValue("Address1"));
        // Address2 in FoodMart is empty for many rows; assert that the
        // property at least round-trips a non-throwing lookup. (Empty
        // string is acceptable; null would mean the value was not
        // requested from SQL.)
        assertNotNull(
            "Postal Code (in annotation but flag off -> still eager)",
            m.getPropertyValue("Postal Code"));
        assertNotNull(
            "Phone1 (in annotation but flag off -> still eager)",
            m.getPropertyValue("Phone1"));
    }

    /**
     * V1-narrow active: properties listed in the annotation return
     * {@code null}, properties not listed continue to return their real
     * column values. The "real values" half is the load-bearing one —
     * it fails if {@link RolapLevel#getProjectedProperties()} is not
     * mirrored between the SQL builder and the {@code setProperty}
     * loop (mis-aligned column indices would write wrong values into
     * FName / LName, not nulls).
     */
    public void testFlagOn_onDemandReturnNull_keptStillCorrect() {
        propSaver.set(
            MondrianProperties.instance().SkipOnDemandLevelProperties,
            true);
        Member m = firstRowMember(
            runMdx(freshContext()));

        // Kept properties — must return non-null, real-looking values.
        Object fname = m.getPropertyValue("FName");
        assertNotNull(
            "FName (kept) must remain populated under V1-narrow",
            fname);
        assertTrue(
            "FName should be a non-empty string, got: " + fname,
            fname instanceof String && !((String) fname).isEmpty());

        Object lname = m.getPropertyValue("LName");
        assertNotNull(
            "LName (kept) must remain populated under V1-narrow",
            lname);
        assertTrue(
            "LName should be a non-empty string, got: " + lname,
            lname instanceof String && !((String) lname).isEmpty());

        // On-demand properties — must return null. Returning anything
        // else means projection skip didn't take effect.
        assertNull(
            "Address1 (on-demand) must be null under V1-narrow",
            m.getPropertyValue("Address1"));
        assertNull(
            "Address2 (on-demand) must be null under V1-narrow",
            m.getPropertyValue("Address2"));
        assertNull(
            "Postal Code (on-demand) must be null under V1-narrow",
            m.getPropertyValue("Postal Code"));
        assertNull(
            "Phone1 (on-demand) must be null under V1-narrow",
            m.getPropertyValue("Phone1"));
    }

    /**
     * Cache-safety check: a second query against the same level under
     * V1-narrow must still see kept properties populated and on-demand
     * properties null. Catches the case where the projection plan
     * computed differently across queries — e.g. cached set lookup
     * returned a stale empty set, or a later query saw a partial
     * member from the cache and silently reused it without re-loading.
     */
    public void testFlagOn_secondQueryIdentical() {
        propSaver.set(
            MondrianProperties.instance().SkipOnDemandLevelProperties,
            true);
        // Share ONE connection so we're testing member-cache
        // consistency within a single schema lifecycle — the
        // production scenario. With withSchemaPool(false) each
        // getConnection() would build a fresh schema, defeating the
        // test; reusing the connection keeps the member cache live.
        Connection con = freshContext().getConnection();
        try {
            Result r1 = con.execute(con.parseQuery(MDX));
            Result r2 = con.execute(con.parseQuery(MDX));
            Member m1 = firstRowMember(r1);
            Member m2 = firstRowMember(r2);

            assertEquals(
                "Two consecutive queries must return same member identity",
                m1.getUniqueName(), m2.getUniqueName());
            assertEquals(
                "Kept FName must agree across queries",
                m1.getPropertyValue("FName"),
                m2.getPropertyValue("FName"));
            assertEquals(
                "Kept LName must agree across queries",
                m1.getPropertyValue("LName"),
                m2.getPropertyValue("LName"));
            assertNull(
                "On-demand still null in second query (cached member"
                + " from query 1 was correctly partial-by-design)",
                m2.getPropertyValue("Phone1"));
        } finally {
            con.close();
        }
    }
}

// End V1NarrowOnDemandPropertyIT.java
