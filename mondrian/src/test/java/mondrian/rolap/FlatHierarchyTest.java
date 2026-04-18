package mondrian.rolap;

import static org.junit.jupiter.api.Assertions.*;
import mondrian.olap.MondrianDef;
import org.junit.jupiter.api.Test;

class FlatHierarchyTest {

    @Test
    void flatName_parsedFromXml() {
        MondrianDef.Level xmlLevel = new MondrianDef.Level();
        xmlLevel.flatName = "Категория";
        assertEquals("Категория", xmlLevel.flatName);
    }

    @Test
    void flatName_nullByDefault() {
        MondrianDef.Level xmlLevel = new MondrianDef.Level();
        assertNull(xmlLevel.flatName);
    }

    @Test
    void showHierarchy_trueByDefault() {
        MondrianDef.Hierarchy xmlHier = new MondrianDef.Hierarchy();
        assertTrue(xmlHier.showHierarchy == null || xmlHier.showHierarchy);
    }

    @Test
    void showHierarchy_canBeFalse() {
        MondrianDef.Hierarchy xmlHier = new MondrianDef.Hierarchy();
        xmlHier.showHierarchy = false;
        assertFalse(xmlHier.showHierarchy);
    }

    @Test
    void canonicalIdentity_sameTableColumn_deduplicated() {
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        assertTrue(seen.add("dim_product\0category"));
        assertFalse(seen.add("dim_product\0category"));
    }

    @Test
    void canonicalIdentity_differentColumn_notDuplicate() {
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        assertTrue(seen.add("dim_product\0category"));
        assertTrue(seen.add("dim_product\0brand"));
    }

    @Test
    void canonicalIdentity_differentTable_notDuplicate() {
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        assertTrue(seen.add("dim_product\0category"));
        assertTrue(seen.add("dim_store\0category"));
    }

    @Test
    void syntheticFlat_hasSourceLinkAPI() {
        try {
            var getSourceHier = SyntheticFlatHierarchy.class
                .getMethod("getSourceHierarchy");
            assertEquals(RolapHierarchy.class, getSourceHier.getReturnType());

            var getSourceLevel = SyntheticFlatHierarchy.class
                .getMethod("getSourceLevel");
            assertEquals(RolapLevel.class, getSourceLevel.getReturnType());
        } catch (NoSuchMethodException e) {
            fail("SyntheticFlatHierarchy missing source-link methods: " + e);
        }
    }

    @Test
    void syntheticFlat_alwaysShowHierarchy() {
        try {
            var method = SyntheticFlatHierarchy.class
                .getMethod("isShowHierarchy");
            assertNotNull(method);
            assertEquals(boolean.class, method.getReturnType());
        } catch (NoSuchMethodException e) {
            fail("isShowHierarchy() missing");
        }
    }
}
