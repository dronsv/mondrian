package mondrian.rolap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.Test;

/**
 * Minimal red test for emondrian-clickhouse#103 (shape 1). THROWAWAY / proposal,
 * never committed by the investigation.
 *
 * Two products, two stores, one stored measure. P1 sells 4 in S1 only, P2 sells
 * 2 in S2 only. Sum over the stores of the stored measure must equal the
 * product total; with the NQE prefetch attached every store iteration is served
 * the (All stores) value, so the result is multiplied by the number of stores.
 */
public class Issue103PrefetchContextDriftTmpTest {
    private static final String NQE = "mondrian.native.queryEngine.enable";

    private static final String SUM_OVER_STORES =
        "WITH MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[Quantity]) "
        + "SELECT {[Measures].[S]} ON COLUMNS, [Product].[Name].Members ON ROWS FROM [Sales]";

    /** A tuple that moves a non-axis hierarchy: P1 never sells in S2, so the cell is empty. */
    private static final String TUPLE_OTHER_STORE =
        "WITH MEMBER [Measures].[T] AS ([Measures].[Quantity], [Store].[S2]) "
        + "SELECT {[Measures].[T]} ON COLUMNS, [Product].[Name].Members ON ROWS FROM [Sales]";

    @Test void sumOverSetOfStoredMeasure() throws Exception {
        assertEquals(List.of("P1=4", "P2=2"), run(SUM_OVER_STORES, false), "oracle (NQE off)");
        assertEquals(List.of("P1=4", "P2=2"), run(SUM_OVER_STORES, true), "NQE on");
    }

    @Test void tupleMovingNonAxisHierarchy() throws Exception {
        assertEquals(List.of("P1=null", "P2=2"), run(TUPLE_OTHER_STORE, false), "oracle (NQE off)");
        assertEquals(List.of("P1=null", "P2=2"), run(TUPLE_OTHER_STORE, true), "NQE on");
    }

    private static List<String> run(String mdx, boolean nqe) throws Exception {
        String previous = MondrianProperties.instance().getProperty(NQE);
        MondrianProperties.instance().setProperty(NQE, Boolean.toString(nqe));
        mondrian.olap.Connection connection = open();
        try {
            Result result = connection.execute(connection.parseQuery(mdx));
            List<String> cells = new ArrayList<>();
            List<Position> rows = result.getAxes()[1].getPositions();
            for (int r = 0; r < rows.size(); r++) {
                Object value = result.getCell(new int[] {0, r}).getValue();
                cells.add(rows.get(r).get(0).getName() + "="
                    + (value instanceof Number n ? String.valueOf(n.longValue()) : String.valueOf(value)));
            }
            return cells;
        } finally {
            connection.close();
            if (previous == null) {
                MondrianProperties.instance().remove(NQE);
            } else {
                MondrianProperties.instance().setProperty(NQE, previous);
            }
        }
    }

    private static mondrian.olap.Connection open() throws Exception {
        String jdbc = "jdbc:h2:mem:i103_" + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'P1'),(2,'P2')");
            sql.execute("CREATE TABLE store (id INT, name VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2')");
            sql.execute("CREATE TABLE fact (product_id INT, store_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,1,4),(2,2,2)");
        }
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="Issue103">
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="store"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
            </Schema>
            """);
        return mondrian.olap.DriverManager.getConnection(props, null);
    }
}
