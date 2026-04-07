package mondrian.rolap;

import junit.framework.TestCase;

public class NativeQueryResultContextTest extends TestCase {

    public void testGetReturnsStoredValue() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "M1|B1|C1|2025", "sales_qty", 42.0);
        assertEquals(42.0, ctx.get("Identity", "M1|B1|C1|2025", "sales_qty"));
    }

    public void testGetReturnsNullForMissingKey() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "M1|B1|C1|2025", "sales_qty", 42.0);
        assertNull(ctx.get("Identity", "M1|B1|C1|2025", "sales_rub"));
        assertNull(ctx.get("Identity", "OTHER_KEY", "sales_qty"));
        assertNull(ctx.get("ProductReset", "M1|B1|C1|2025", "sales_qty"));
    }

    public void testSizeTracking() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        assertEquals(0, ctx.size());
        ctx.put("Identity", "k1", "m1", 1.0);
        ctx.put("Identity", "k1", "m2", 2.0);
        ctx.put("Reset", "k2", "m1", 3.0);
        assertEquals(3, ctx.size());
    }

    public void testProjectedKeyLookup() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("ProductReset", "C1|2025", "sales_rub_reset", 100.0);
        assertEquals(100.0, ctx.get("ProductReset", "C1|2025", "sales_rub_reset"));
    }

    public void testNullValueStoredAndRetrievable() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "m1", null);
        assertTrue(ctx.containsKey("Identity", "k1", "m1"));
        assertNull(ctx.get("Identity", "k1", "m1"));
    }
}
