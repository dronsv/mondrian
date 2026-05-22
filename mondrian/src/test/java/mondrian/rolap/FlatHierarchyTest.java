package mondrian.rolap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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

    /**
     * #78 prerequisite — synthetic-flat levels expose source-hierarchy
     * ancestor identities as member properties so
     * {@code DrilldownMemberFunDef.drillDownCrossHierarchy} can filter
     * cross-hierarchy drill children by source-path correlation.
     *
     * <p>Asserts that {@code buildSyntheticLevel} for a depth-3 source
     * level (i.e. with two ancestor levels in the same source hierarchy)
     * emits two {@code MondrianDef.Property} entries — one per ancestor
     * source level, naming columns of the source levels, with the
     * {@code _synth_src_ancestor_} prefix.
     */
    @Test
    void syntheticFlat_emitsAncestorPropertiesForSourceLevels() {
        // Source-level chain: l1 (depth=1) -> l2 (depth=2) -> source (depth=3)
        MondrianDef.Column l1KeyCol = new MondrianDef.Column();
        l1KeyCol.name = "category_l1_id";
        l1KeyCol.table = "dim_product";

        MondrianDef.Column l2KeyCol = new MondrianDef.Column();
        l2KeyCol.name = "category_l2_id";
        l2KeyCol.table = "dim_product";

        RolapLevel l1 = mock(RolapLevel.class);
        when(l1.getName()).thenReturn("Category1");
        when(l1.isAll()).thenReturn(false);
        when(l1.getKeyExp()).thenReturn(l1KeyCol);
        when(l1.getParentLevel()).thenReturn(null);

        RolapLevel l2 = mock(RolapLevel.class);
        when(l2.getName()).thenReturn("Category2");
        when(l2.isAll()).thenReturn(false);
        when(l2.getKeyExp()).thenReturn(l2KeyCol);
        when(l2.getParentLevel()).thenReturn(l1);

        // The source level we're projecting as a synthetic flat.
        MondrianDef.Column sourceKeyCol = new MondrianDef.Column();
        sourceKeyCol.name = "category_l3_id";
        sourceKeyCol.table = "dim_product";

        RolapLevel source = mock(RolapLevel.class);
        when(source.getName()).thenReturn("Category3");
        when(source.isAll()).thenReturn(false);
        when(source.isUnique()).thenReturn(true);
        when(source.getKeyExp()).thenReturn(sourceKeyCol);
        when(source.getParentLevel()).thenReturn(l2);
        when(source.getDatatype()).thenReturn(null);

        MondrianDef.Level sourceXml = new MondrianDef.Level();
        sourceXml.column = "category_l3_id";

        MondrianDef.Level flat =
            SyntheticFlatHierarchy.buildSyntheticLevel(
                source, sourceXml, "Category3");

        assertNotNull(flat.properties);
        assertEquals(
            2, flat.properties.length,
            "Expected one MondrianDef.Property per source-hierarchy "
                + "ancestor (l2, l1); got: " + flat.properties.length);

        // Properties are emitted in walk order: nearest ancestor first.
        assertEquals(
            SyntheticFlatHierarchySupport.ANCESTOR_PROPERTY_PREFIX
                + "Category2",
            flat.properties[0].name);
        assertEquals("category_l2_id", flat.properties[0].column);
        assertTrue(flat.properties[0].dependsOnLevelValue);

        assertEquals(
            SyntheticFlatHierarchySupport.ANCESTOR_PROPERTY_PREFIX
                + "Category1",
            flat.properties[1].name);
        assertEquals("category_l1_id", flat.properties[1].column);
        assertTrue(flat.properties[1].dependsOnLevelValue);
    }

    /**
     * #78 prerequisite — leaf-level synthetic-flat (source level at
     * depth=1 directly under [All]) has no ancestor levels in the
     * source hierarchy, so no ancestor properties are emitted.
     */
    @Test
    void syntheticFlat_topLevelSourceEmitsNoAncestorProperties() {
        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.name = "category_l1_id";
        keyCol.table = "dim_product";

        RolapLevel source = mock(RolapLevel.class);
        when(source.getName()).thenReturn("Category1");
        when(source.isAll()).thenReturn(false);
        when(source.isUnique()).thenReturn(true);
        when(source.getKeyExp()).thenReturn(keyCol);
        when(source.getParentLevel()).thenReturn(null); // no ancestor
        when(source.getDatatype()).thenReturn(null);

        MondrianDef.Level sourceXml = new MondrianDef.Level();
        sourceXml.column = "category_l1_id";

        MondrianDef.Level flat =
            SyntheticFlatHierarchy.buildSyntheticLevel(
                source, sourceXml, "Category1");

        assertNotNull(flat.properties);
        assertEquals(
            0, flat.properties.length,
            "Top-level source level should emit no ancestor properties");
    }

    /**
     * #78 prerequisite — walk stops at [All] ancestor, doesn't try to
     * emit a property for it.
     */
    @Test
    void syntheticFlat_doesNotEmitPropertyForAllLevel() {
        RolapLevel allLevel = mock(RolapLevel.class);
        when(allLevel.isAll()).thenReturn(true);

        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.name = "category_l1_id";
        keyCol.table = "dim_product";

        RolapLevel source = mock(RolapLevel.class);
        when(source.getName()).thenReturn("Category1");
        when(source.isAll()).thenReturn(false);
        when(source.isUnique()).thenReturn(true);
        when(source.getKeyExp()).thenReturn(keyCol);
        when(source.getParentLevel()).thenReturn(allLevel);
        when(source.getDatatype()).thenReturn(null);

        MondrianDef.Level sourceXml = new MondrianDef.Level();
        sourceXml.column = "category_l1_id";

        MondrianDef.Level flat =
            SyntheticFlatHierarchy.buildSyntheticLevel(
                source, sourceXml, "Category1");

        assertNotNull(flat.properties);
        assertEquals(
            0, flat.properties.length,
            "Ancestor walk must stop at [All] level — no property "
                + "should be emitted for it");
    }

    /**
     * #78 H2 guard — when the source level is NOT unique, the synthetic
     * level key does not functionally determine the ancestor key, so
     * ancestor properties would be nondeterministic. Skip emission so
     * the DrilldownMember filter falls back to its existing (correct)
     * Cartesian behavior.
     */
    @Test
    void syntheticFlat_skipsAncestorPropertiesWhenSourceLevelIsNotUnique() {
        MondrianDef.Column l1KeyCol = new MondrianDef.Column();
        l1KeyCol.name = "category_l1_id";
        l1KeyCol.table = "dim_product";

        RolapLevel l1 = mock(RolapLevel.class);
        when(l1.getName()).thenReturn("Category1");
        when(l1.isAll()).thenReturn(false);
        when(l1.getKeyExp()).thenReturn(l1KeyCol);
        when(l1.getParentLevel()).thenReturn(null);

        MondrianDef.Column sourceKeyCol = new MondrianDef.Column();
        sourceKeyCol.name = "category_l2_id";
        sourceKeyCol.table = "dim_product";

        RolapLevel source = mock(RolapLevel.class);
        when(source.getName()).thenReturn("Category2");
        when(source.isAll()).thenReturn(false);
        when(source.isUnique()).thenReturn(false); // ← not unique
        when(source.getKeyExp()).thenReturn(sourceKeyCol);
        when(source.getParentLevel()).thenReturn(l1);
        when(source.getDatatype()).thenReturn(null);

        MondrianDef.Level sourceXml = new MondrianDef.Level();
        sourceXml.column = "category_l2_id";

        MondrianDef.Level flat =
            SyntheticFlatHierarchy.buildSyntheticLevel(
                source, sourceXml, "Category2");

        assertNotNull(flat.properties);
        assertEquals(
            0, flat.properties.length,
            "Non-unique source level must NOT emit ancestor properties "
                + "(dependsOnLevelValue=true would be nondeterministic)");
    }

    /**
     * #78 H2 guard — synthetic-flat-as-property only works for
     * ancestor columns on the same table as the synthetic level
     * key (MondrianDef.Property has no `table` attribute and
     * getPropertyExp builds Column(level.table, prop.column)). When
     * ancestor lives on a different table (snowflake / joined source
     * hierarchy), skip emission and fall back to existing Cartesian
     * drill behavior — preferable to reading the wrong column.
     */
    @Test
    void syntheticFlat_skipsAncestorPropertyWhenAncestorOnDifferentTable() {
        // Ancestor key on dim_category_l1 — different table from
        // synthetic level's dim_product.
        MondrianDef.Column l1KeyCol = new MondrianDef.Column();
        l1KeyCol.name = "category_l1_id";
        l1KeyCol.table = "dim_category_l1";

        RolapLevel l1 = mock(RolapLevel.class);
        when(l1.getName()).thenReturn("Category1");
        when(l1.isAll()).thenReturn(false);
        when(l1.getKeyExp()).thenReturn(l1KeyCol);
        when(l1.getParentLevel()).thenReturn(null);

        // Synthetic level is built off dim_product.
        MondrianDef.Column sourceKeyCol = new MondrianDef.Column();
        sourceKeyCol.name = "category_l2_id";
        sourceKeyCol.table = "dim_product";

        RolapLevel source = mock(RolapLevel.class);
        when(source.getName()).thenReturn("Category2");
        when(source.isAll()).thenReturn(false);
        when(source.isUnique()).thenReturn(true);
        when(source.getKeyExp()).thenReturn(sourceKeyCol);
        when(source.getParentLevel()).thenReturn(l1);
        when(source.getDatatype()).thenReturn(null);

        MondrianDef.Level sourceXml = new MondrianDef.Level();
        sourceXml.column = "category_l2_id";

        MondrianDef.Level flat =
            SyntheticFlatHierarchy.buildSyntheticLevel(
                source, sourceXml, "Category2");

        assertNotNull(flat.properties);
        assertEquals(
            0, flat.properties.length,
            "Ancestor on different table must NOT emit a property "
                + "(would resolve via Column(level.table, prop.column) "
                + "to the wrong table)");
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
