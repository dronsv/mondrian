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
            sql.execute("CREATE TABLE role_fact (bill_store_id INT, ship_store_id INT, qty INT)");
            sql.execute("INSERT INTO role_fact VALUES (1,3,10),(3,1,20)");
            sql.execute("CREATE TABLE coded_geo (store_id INT, store_code VARCHAR, region_id INT, country VARCHAR)");
            sql.execute("INSERT INTO coded_geo VALUES (1,'X',10,'US'),(2,'Y',10,'US'),(3,'X',20,'EU')");
            sql.execute("CREATE TABLE code_fact (store_code VARCHAR, qty INT)");
            sql.execute("INSERT INTO code_fact VALUES ('X',10)");
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
            sql.execute("CREATE TABLE pclass (class_id INT, class_name VARCHAR)");
            sql.execute("INSERT INTO pclass VALUES (1,'Food'),(2,'Drink')");
            sql.execute("CREATE TABLE prod (product_id INT, class_id INT, product_name VARCHAR)");
            sql.execute("INSERT INTO prod VALUES (1,1,'Apple'),(2,1,'Bread'),(3,2,'Milk')");
            // class_id is a same-named column the schema never joins on.
            sql.execute("CREATE TABLE product_fact (product_id INT, class_id INT, qty INT)");
            sql.execute("INSERT INTO product_fact VALUES (1,2,10),(3,1,20)");
            sql.execute("CREATE TABLE class_fact (class_id INT, qty INT)");
            sql.execute("INSERT INTO class_fact VALUES (1,10),(2,20)");
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
              <Dimension name="RoleGeo">
                <Hierarchy hasAll="true" primaryKey="store_id" primaryKeyTable="geo"><Table name="geo"/>
                  <Level name="Country" column="country" uniqueMembers="true"/>
                  <Level name="Store" column="store_id" type="Numeric" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="CodedGeo">
                <Hierarchy hasAll="true" primaryKey="store_id"><Table name="coded_geo"/>
                  <Level name="Country" column="country" uniqueMembers="true"/>
                  <Level name="Region" column="region_id" type="Numeric" uniqueMembers="true"/>
                  <Level name="Store" column="store_code" uniqueMembers="false"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="product_id" primaryKeyTable="prod">
                  <Join leftKey="class_id" rightKey="class_id"><Table name="prod"/><Table name="pclass"/></Join>
                  <Level name="Class" table="pclass" column="class_name" uniqueMembers="true"/>
                  <Level name="Product" table="prod" column="product_name" uniqueMembers="true"/>
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
              <Cube name="LeafStores"><Table name="store_fact"/>
                <DimensionUsage name="Geo" source="Geo" foreignKey="store_id" level="Store"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
              </Cube>
              <Cube name="SharedUsage"><Table name="region_fact"/>
                <DimensionUsage name="Geo" source="SharedGeo" foreignKey="region_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
              <Cube name="RoleStores"><Table name="role_fact"/>
                <DimensionUsage name="Billing" source="RoleGeo" foreignKey="bill_store_id" level="Store"/>
                <DimensionUsage name="Shipping" source="RoleGeo" foreignKey="ship_store_id" level="Store"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
              </Cube>
              <Cube name="CodedStores"><Table name="code_fact"/>
                <DimensionUsage name="Geo" source="CodedGeo" foreignKey="store_code" level="Store"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
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
              <Cube name="Products"><Table name="product_fact"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
              </Cube>
            </Schema>
            """.formatted(
                nativeMeasure("Chain", "region_fact", true),
                nativeMeasure("Only", "region_fact", false),
                nativeMeasure("Only", "store_fact", false),
                nativeMeasure("Only", "store_fact", false),
                nativeMeasure("Only", "role_fact", false),
                nativeMeasure("Only", "code_fact", false),
                nativeMeasure("Only", "product_fact", false)));
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

    @Test void explicitLeafLevelRetainsOrdinaryPrimaryKeyRebasing() {
        for (boolean clickHouse : new boolean[] {false, true}) {
            NativeSqlFactJoins.Rebase result =
                rebase("store_fact", level("LeafStores", "Country"), false, clickHouse);
            assertNull(result.skip);
            assertTrue(result.placeholders.get("factJoins").contains("LEFT"));
        }
    }

    @Test void explicitLeafLevelCannotPoisonAnotherCubesSharedStar() {
        RolapCubeLevel ordinary = level("Stores", "Country");
        RolapCubeLevel explicitLeaf = level("LeafStores", "Country");
        assertSame(ordinary.getStarKeyColumn().getTable(),
            explicitLeaf.getStarKeyColumn().getTable());
        assertNull(rebase("store_fact", ordinary, true, true).skip);
    }

    @Test void explicitLeafTemplateExecutesNativelyAtTheDeclaredGrain() {
        assertEquals(10d, queryValue("LeafStores", "Only"));
    }

    @Test void rolePlayingPrimaryKeyLeafRetainsNativeEligibility() {
        RolapCubeLevel billing = level("RoleStores", "Billing", "Country");
        RolapCubeLevel shipping = level("RoleStores", "Shipping", "Country");
        assertEquals("geo", billing.getKeyExp().getTableAlias());
        assertEquals("geo_1", shipping.getKeyExp().getTableAlias());
        assertEquals("geo", shipping.getHierarchy().getXmlHierarchy().primaryKeyTable);
        for (RolapCubeLevel role : List.of(billing, shipping)) {
            for (boolean predicate : new boolean[] {false, true}) {
                for (boolean clickHouse : new boolean[] {false, true}) {
                    NativeSqlFactJoins.Rebase result =
                        rebase("role_fact", role, predicate, clickHouse);
                    assertNull(result.skip);
                    assertTrue(result.placeholders.get("factJoins").contains("LEFT"));
                }
            }
        }
    }

    @Test void rolePlayingPrimaryKeyLeafTemplatesExecuteNatively() {
        assertEquals(10d, queryValue("RoleStores", "Only", "[Billing].[US]"));
        assertEquals(20d, queryValue("RoleStores", "Only", "[Shipping].[US]"));
    }

    @Test void nonUniqueLeafKeyDifferentFromPrimaryKeyStillDeclines() {
        assertSkipped(rebase("code_fact", level("CodedStores", "Country"), false, true));
        assertSkipped(rebase("code_fact", level("CodedStores", "Country"), true, false));
    }

    @Test void nonUniqueLeafTemplateUsesTheMdxFallback() {
        assertEquals(99d, queryValue("CodedStores", "Only"));
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

    @Test void snowflakeJoinsEveryHopFromTheFactForeignKey() {
        // pclass hangs off prod; its join key is prod.class_id, not a
        // fact column, even though product_fact has a class_id column.
        NativeSqlFactJoins.Rebase result =
            rebase("product_fact", level("Products", "Class"), false, true);
        assertNull(result.skip);
        assertEquals(
            "LEFT ANY JOIN `prod` nscd0 ON f.`product_id` = nscd0.`product_id`\n"
            + "LEFT ANY JOIN `pclass` nscd1 ON nscd0.`class_id` = nscd1.`class_id`",
            result.placeholders.get("factJoins"));
        assertEquals("nscd1.`class_name`", result.axisBindings.get(0).qualifiedColumn);
    }

    @Test void snowflakeSharesTheFirstHopWithItsOwnTable() {
        NativeSqlFactJoins.Rebase result = NativeSqlFactJoins.resolveTemplate(
            TEMPLATE.formatted("product_fact"), 0, "Native",
            Map.of("whereClause", "1 = 1"),
            List.of(
                axis(level("Products", "Product"), "k0"),
                axis(level("Products", "Class"), "k1")),
            List.of(), clickHouse(), connection.getDataSource());
        assertNull(result.skip);
        assertEquals(
            "LEFT ANY JOIN `prod` nscd0 ON f.`product_id` = nscd0.`product_id`\n"
            + "LEFT ANY JOIN `pclass` nscd1 ON nscd0.`class_id` = nscd1.`class_id`",
            result.placeholders.get("factJoins"));
        assertEquals("nscd0.`product_name`", result.axisBindings.get(0).qualifiedColumn);
        assertEquals("nscd1.`class_name`", result.axisBindings.get(1).qualifiedColumn);
    }

    @Test void snowflakeSourceWithoutTheFactForeignKeyDeclines() {
        // class_fact carries a class_id, but the star reaches pclass
        // only through prod; a same-named column is not a join key.
        NativeSqlFactJoins.Rebase result =
            rebase("class_fact", level("Products", "Class"), false, true);
        assertNotNull(result.skip);
        assertEquals(
            NativeSqlCalc.TemplateSkipReason.FK_MISSING_ON_SOURCE,
            result.skip.reason());
        assertTrue(result.skip.missingColumns().contains("product_id"));
    }

    @Test void snowflakeTemplateExecutesAtTheDeclaredGrain() {
        assertEquals(10d, queryValue("Products", "Only", "[Product].[Food]"));
        assertEquals(20d, queryValue("Products", "Only", "[Product].[Drink]"));
    }

    private double queryValue(String cube, String measure) {
        return queryValue(cube, measure, "[Geo].[US]");
    }

    private double queryValue(String cube, String measure, String slicer) {
        Result result = connection.execute(connection.parseQuery(
            "SELECT {[Measures].[" + measure + "]} ON 0 FROM [" + cube
            + "] WHERE " + slicer));
        return ((Number) result.getCell(new int[] {0}).getValue()).doubleValue();
    }

    private RolapCubeLevel closureLevel() {
        return level("Employees", "Employee").getClosedPeer();
    }

    private RolapCubeLevel level(String cubeName, String levelName) {
        return level(cubeName, null, levelName);
    }

    private RolapCubeLevel level(
        String cubeName, String dimensionName, String levelName)
    {
        RolapCube cube = (RolapCube) connection.getSchema()
            .lookupCube(cubeName, true);
        for (Hierarchy hierarchy : cube.getHierarchies()) {
            if (dimensionName != null
                && !dimensionName.equals(hierarchy.getDimension().getName()))
            {
                continue;
            }
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
        return NativeSqlFactJoins.resolveTemplate(
            TEMPLATE.formatted(source), 0, "Native",
            Map.of("whereClause", "1 = 1"),
            predicate ? List.of() : List.of(axis(level, "k0")),
            predicate ? List.of(new NativeSqlCalc.AtomicPredicateInfo(
                level.getDimension().getName(), level.getHierarchy().getName(),
                keyColumn(level), "IS NOT NULL", starColumn, null)) : List.of(),
            clickHouse ? clickHouse() : connection.getSchema().getDialect(),
            connection.getDataSource());
    }

    private static NativeSqlCalc.AxisBinding axis(RolapCubeLevel level, String key) {
        String column = keyColumn(level);
        return new NativeSqlCalc.AxisBinding(
            level.getHierarchy(), level.getHierarchy().getName(),
            "f." + column, column, key, level.getStarKeyColumn());
    }

    private static String keyColumn(RolapCubeLevel level) {
        return ((MondrianDef.Column) level.getStarKeyColumn().getExpression()).name;
    }

    private static Dialect clickHouse() {
        Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct()).thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
        when(dialect.quoteIdentifier(anyString()))
            .thenAnswer(inv -> "`" + inv.getArgument(0) + "`");
        return dialect;
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
