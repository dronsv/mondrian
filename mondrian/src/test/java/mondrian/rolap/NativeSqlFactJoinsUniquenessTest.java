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

import mondrian.olap.Hierarchy;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.spi.Dialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** #101: joins that can select several dimension rows must decline rebasing. */
public class NativeSqlFactJoinsUniquenessTest {
    private static final String TEMPLATE =
        "SELECT SUM(f.qty) AS val FROM %s f ${factJoins}"
        + " WHERE ${whereClause}";

    private java.sql.Connection database;
    private RolapConnection connection;
    private boolean previousNativeSql;

    @BeforeEach void open() throws Exception {
        previousNativeSql = MondrianProperties.instance().NativeSqlEnable.get();
        MondrianProperties.instance().NativeSqlEnable.set(true);
        String jdbc = "jdbc:h2:mem:fact_join_unique_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE geo (store_id INT, region_id INT, country VARCHAR)");
            sql.execute("INSERT INTO geo VALUES (1,10,'US'),(2,10,'US'),(3,20,'EU')");
            sql.execute("CREATE TABLE region_fact (region_id INT, qty INT)");
            sql.execute("INSERT INTO region_fact VALUES (10,10),(20,20)");
            sql.execute("CREATE TABLE store_fact (store_id INT, qty INT)");
            sql.execute("INSERT INTO store_fact VALUES (1,10),(3,20)");
            sql.execute("CREATE TABLE wide_fact (country VARCHAR, qty INT)");
            sql.execute("INSERT INTO wide_fact VALUES ('US',10),('EU',20)");
            sql.execute("CREATE TABLE employee (employee_id INT, supervisor_id INT)");
            sql.execute("INSERT INTO employee VALUES (1,0),(2,1),(3,2),(4,1)");
            sql.execute("CREATE TABLE employee_closure (supervisor_id INT, employee_id INT)");
            sql.execute("INSERT INTO employee_closure VALUES (1,1),(1,2),(2,2),(1,3),(2,3),(3,3),(1,4),(4,4)");
            sql.execute("CREATE TABLE employee_fact (employee_id INT, qty INT)");
            sql.execute("INSERT INTO employee_fact VALUES (3,10),(4,20)");
            sql.execute("CREATE TABLE wide_employee_fact (supervisor_id INT, qty INT)");
            sql.execute("INSERT INTO wide_employee_fact VALUES (2,10)");
        }
        Util.PropertyList props =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="FactJoinUniqueness">
              <Dimension name="Geo">
                <Hierarchy hasAll="true" primaryKey="store_id"><Table name="geo"/>
                  <Level name="Country" column="country" uniqueMembers="true"/>
                  <Level name="Region" column="region_id" type="Numeric" uniqueMembers="true"/>
                  <Level name="Store" column="store_id" type="Numeric" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="SharedGeo">
                <Hierarchy hasAll="true" primaryKey="region_id"><Table name="geo"/>
                  <Level name="Country" column="country" uniqueMembers="true"/>
                  <Level name="Region" column="region_id" type="Numeric" uniqueMembers="true"/>
                  <Level name="Store" column="store_id" type="Numeric" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Regions"><Table name="region_fact"/>
                <DimensionUsage name="Geo" source="Geo" foreignKey="region_id" level="Region"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
                %s
              </Cube>
              <Cube name="Stores"><Table name="store_fact"/>
                <DimensionUsage name="Geo" source="Geo" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
              </Cube>
              <Cube name="SharedUsage"><Table name="region_fact"/>
                <DimensionUsage name="Geo" source="SharedGeo" foreignKey="region_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
              <Cube name="Employees"><Table name="employee_fact"/>
                <Dimension name="Employee" foreignKey="employee_id">
                  <Hierarchy hasAll="true" primaryKey="employee_id"><Table name="employee"/>
                    <Level name="Employee" column="employee_id" parentColumn="supervisor_id"
                           nullParentValue="0" type="Numeric" uniqueMembers="true">
                      <Closure parentColumn="supervisor_id" childColumn="employee_id">
                        <Table name="employee_closure"/>
                      </Closure>
                    </Level>
                  </Hierarchy>
                </Dimension>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
            </Schema>
            """.formatted(
                nativeMeasure("Chain", "region_fact", true),
                nativeMeasure("Only", "region_fact", false),
                nativeMeasure("Only", "store_fact", false)));
        connection = (RolapConnection)
            mondrian.olap.DriverManager.getConnection(props, null);
    }

    @AfterEach void close() throws Exception {
        MondrianProperties.instance().NativeSqlEnable.set(previousNativeSql);
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }

    @Test void closureAxisDeclinesClickHouseAnyJoin() {
        assertSkipped(rebase("employee_fact", closureLevel(), false, true));
    }

    @Test void closurePredicateDeclinesPlainLeftJoin() {
        assertSkipped(rebase("employee_fact", closureLevel(), true, false));
    }

    @Test void levelUsageAxisDeclinesPlainLeftJoin() {
        assertSkipped(rebase("region_fact", level("Regions", "Country"), false, false));
    }

    @Test void levelUsagePredicateDeclinesClickHouseAnyJoin() {
        assertSkipped(rebase("region_fact", level("Regions", "Country"), true, true));
    }

    @Test void safeUsageCannotRestoreUnsafeSharedStarJoin() {
        RolapCubeLevel unsafe = level("Regions", "Country");
        RolapCubeLevel declaredPrimaryKey = level("SharedUsage", "Country");
        // Both cubes share this physical star table and equality join.
        assertSame(unsafe.getStarKeyColumn().getTable(),
            declaredPrimaryKey.getStarKeyColumn().getTable());
        assertSkipped(rebase("region_fact", declaredPrimaryKey, false, false));
    }

    @Test void ordinaryPrimaryKeyStillRebases() {
        NativeSqlFactJoins.Rebase result =
            rebase("store_fact", level("Stores", "Country"), false, true);
        assertNull(result.skip);
        assertTrue(result.placeholders.get("factJoins").contains("LEFT ANY JOIN"));
        assertTrue(result.axisBindings.get(0).qualifiedColumn.startsWith("nscd0."));
    }

    @Test void closureColumnAlreadyOnSourceDoesNotNeedAJoin() {
        NativeSqlFactJoins.Rebase result =
            rebase("wide_employee_fact", closureLevel(), false, false);
        assertNull(result.skip);
        assertEquals("", result.placeholders.get("factJoins"));
        assertEquals("f.supervisor_id", result.axisBindings.get(0).qualifiedColumn);
    }

    @Test void unsafeTemplateTriesWideFallback() {
        // A plain LEFT JOIN on region_id doubles US from 10 to 20.
        // The next template has the country at the correct fact grain.
        assertEquals(10d, queryValue("Regions", "Chain"));
    }

    @Test void exhaustedUnsafeTemplateUsesMdxFallback() {
        assertEquals(99d, queryValue("Regions", "Only"));
    }

    @Test void primaryKeyTemplateStillExecutesNatively() {
        assertEquals(10d, queryValue("Stores", "Only"));
    }

    private double queryValue(String cube, String measure) {
        Result result = connection.execute(connection.parseQuery(
            "SELECT {[Measures].[" + measure + "]} ON 0 FROM [" + cube
            + "] WHERE [Geo].[US]"));
        return ((Number) result.getCell(new int[] {0}).getValue()).doubleValue();
    }

    private RolapCubeLevel closureLevel() {
        return level("Employees", "Employee").getClosedPeer();
    }

    private RolapCubeLevel level(String cubeName, String levelName) {
        RolapCube cube = (RolapCube) connection.getSchema()
            .lookupCube(cubeName, true);
        for (Hierarchy hierarchy : cube.getHierarchies()) {
            for (mondrian.olap.Level level : hierarchy.getLevels()) {
                if (level.getName().equals(levelName)) {
                    return (RolapCubeLevel) level;
                }
            }
        }
        throw new AssertionError("Missing level " + cubeName + "." + levelName);
    }

    private NativeSqlFactJoins.Rebase rebase(
        String source, RolapCubeLevel level, boolean predicate, boolean clickHouse)
    {
        RolapStar.Column starColumn = level.getStarKeyColumn();
        String column = ((MondrianDef.Column) starColumn.getExpression()).name;
        Dialect dialect = connection.getSchema().getDialect();
        if (clickHouse) {
            dialect = mock(Dialect.class);
            when(dialect.getDatabaseProduct()).thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
            when(dialect.quoteIdentifier(anyString()))
                .thenAnswer(inv -> "`" + inv.getArgument(0) + "`");
        }
        return NativeSqlFactJoins.resolveTemplate(
            TEMPLATE.formatted(source), 0, "Native",
            Map.of("whereClause", "1 = 1"),
            predicate ? List.of() : List.of(
                new NativeSqlCalc.AxisBinding(
                    level.getHierarchy(), level.getHierarchy().getName(),
                    "f." + column, column, "k0", starColumn)),
            predicate ? List.of(new NativeSqlCalc.AtomicPredicateInfo(
                level.getDimension().getName(), level.getHierarchy().getName(),
                column, "IS NOT NULL", starColumn, null)) : List.of(),
            dialect, connection.getDataSource());
    }

    private static void assertSkipped(NativeSqlFactJoins.Rebase result) {
        assertNotNull(result.skip, "non-unique schema join must skip the template");
        assertEquals(NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH, result.skip.reason());
        assertNull(result.placeholders);
    }

    private static String nativeMeasure(String name, String source, boolean chain) {
        return """
            <CalculatedMember name="%s" dimension="Measures">
              <Annotations>
                <Annotation name="nativeSql.enabled">true</Annotation>
                <Annotation name="nativeSql.scalar">true</Annotation>
                <Annotation name="nativeSql.template">%s</Annotation>
                %s
              </Annotations>
              <Formula>99</Formula>
            </CalculatedMember>
            """.formatted(name, TEMPLATE.formatted(source), chain
                ? "<Annotation name=\"nativeSql.template.1\">"
                    + TEMPLATE.formatted("wide_fact") + "</Annotation>"
                : "");
    }
}
