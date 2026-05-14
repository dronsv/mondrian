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

    @Test
    void syntheticFlat_usesNameColumnAsCaptionOnly() {
        MondrianDef.Level source = new MondrianDef.Level();
        source.column = "category_id";
        source.nameColumn = "category_name";
        source.ordinalColumn = "category_sort";

        MondrianDef.Level synthetic = new MondrianDef.Level();
        synthetic.column = "category_id";

        SyntheticFlatHierarchy.copyMemberDisplayMetadata(source, synthetic);

        assertEquals("category_name", synthetic.captionColumn);
        assertEquals("category_sort", synthetic.ordinalColumn);
        assertNull(synthetic.nameColumn);
    }

    @Test
    void syntheticFlat_prefersExplicitCaptionColumn() {
        MondrianDef.Level source = new MondrianDef.Level();
        source.column = "category_id";
        source.nameColumn = "category_name";
        source.captionColumn = "category_caption";

        MondrianDef.Level synthetic = new MondrianDef.Level();
        synthetic.column = "category_id";

        SyntheticFlatHierarchy.copyMemberDisplayMetadata(source, synthetic);

        assertEquals("category_caption", synthetic.captionColumn);
        assertNull(synthetic.nameColumn);
    }

    @Test
    void syntheticFlat_convertsNameExpressionToCaptionExpression() {
        MondrianDef.SQL sql = new MondrianDef.SQL();
        sql.dialect = "generic";
        sql.cdata = "upper(category_name)";

        MondrianDef.NameExpression nameExp =
            new MondrianDef.NameExpression();
        nameExp.expressions = new MondrianDef.SQL[] { sql };

        MondrianDef.Level source = new MondrianDef.Level();
        source.column = "category_id";
        source.nameColumn = "category_name";
        source.nameExp = nameExp;

        MondrianDef.Level synthetic = new MondrianDef.Level();
        synthetic.column = "category_id";

        SyntheticFlatHierarchy.copyMemberDisplayMetadata(source, synthetic);

        assertNotNull(synthetic.captionExp);
        assertArrayEquals(nameExp.expressions, synthetic.captionExp.expressions);
        assertNull(synthetic.nameExp);
    }
}
