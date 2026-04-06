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

import junit.framework.TestCase;
import java.util.*;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianException;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.MondrianDef;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.rolap.agg.ValueColumnPredicate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NativeSqlCalc} template substitution logic.
 */
public class NativeSqlCalcTest extends TestCase {

    public void testSubstitutePlaceholders_basic() {
        String template =
            "SELECT ${axisExpr1} AS k1, sum(${factAlias}.${wt}) AS val "
            + "FROM ${factTable} ${factAlias} "
            + "${joinClauses} WHERE ${whereClause} GROUP BY k1";

        Map<String, String> placeholders = new LinkedHashMap<String, String>();
        placeholders.put("factTable", "mart_konfet_monthly");
        placeholders.put("factAlias", "f");
        placeholders.put("joinClauses",
            "JOIN dim_konfet_store s ON f.store_key = s.store_key");
        placeholders.put("whereClause", "s.chain_group = 'Магнит'");
        placeholders.put("axisExpr1", "p.manufacturer_group");
        placeholders.put("axisCount", "1");
        placeholders.put("wt", "sales_rub");

        String result = NativeSqlCalc.substitutePlaceholders(
            template, placeholders);

        assertTrue(result.contains("mart_konfet_monthly f"));
        assertTrue(result.contains("p.manufacturer_group AS k1"));
        assertTrue(result.contains("sum(f.sales_rub)"));
        assertTrue(result.contains("s.chain_group = 'Магнит'"));
        assertFalse(result.contains("${"));
    }

    public void testSubstitutePlaceholders_emptyWhere() {
        String template = "WHERE ${whereClause}";
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("whereClause", "1 = 1");
        String result = NativeSqlCalc.substitutePlaceholders(template, ph);
        assertEquals("WHERE 1 = 1", result);
    }

    public void testSubstitutePlaceholders_unresolvedPlaceholder() {
        String template = "SELECT ${unknownVar} AS x";
        Map<String, String> ph = Collections.emptyMap();
        try {
            NativeSqlCalc.substitutePlaceholders(template, ph);
            fail("Expected exception for unresolved placeholder");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("unknownVar"));
        }
    }

    public void testSubstitutePlaceholders_axisExprBeyondRange() {
        // axisExprN beyond actual axis count → fail-fast
        String template = "SELECT ${axisExpr3} AS k3";
        Map<String, String> ph = Collections.emptyMap();
        try {
            NativeSqlCalc.substitutePlaceholders(template, ph);
            fail("Expected exception for unresolved axisExpr3");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("axisExpr3"));
        }
    }

    public void testEncodeRowKey() {
        assertEquals("A|B|C",
            NativeSqlCalc.encodeRowKey(
                java.util.Arrays.asList("A", "B", "C")));
        assertEquals("X",
            NativeSqlCalc.encodeRowKey(
                java.util.Arrays.asList("X")));
        assertEquals("",
            NativeSqlCalc.encodeRowKey(
                Collections.<String>emptyList()));
    }

    public void testCollectAxisKeyParts_preservesAxisOrderAndPads() {
        final Dimension measuresDim = mock(Dimension.class);
        final RolapHierarchy measuresHier = mock(RolapHierarchy.class);
        final Dimension categoryDim = mock(Dimension.class);
        final RolapHierarchy categoryHier = mock(RolapHierarchy.class);
        final Dimension brandDim = mock(Dimension.class);
        final RolapHierarchy brandHier = mock(RolapHierarchy.class);
        final RolapMember measure = mock(RolapMember.class);
        final RolapMember brand = mock(RolapMember.class);
        final RolapMember category = mock(RolapMember.class);

        when(measuresHier.getDimension()).thenReturn(measuresDim);
        when(categoryHier.getDimension()).thenReturn(categoryDim);
        when(brandHier.getDimension()).thenReturn(brandDim);

        when(measure.isMeasure()).thenReturn(true);
        when(measure.getHierarchy()).thenReturn(measuresHier);

        when(brand.isMeasure()).thenReturn(false);
        when(brand.isAll()).thenReturn(false);
        when(brand.getHierarchy()).thenReturn(brandHier);
        when(brand.getKey()).thenReturn("Brand X");

        when(category.isMeasure()).thenReturn(false);
        when(category.isAll()).thenReturn(false);
        when(category.getHierarchy()).thenReturn(categoryHier);
        when(category.getKey()).thenReturn("Chocolate");

        final Set<Hierarchy> axisHierarchies = new LinkedHashSet<Hierarchy>(
            Arrays.asList(measuresHier, categoryHier, brandHier));

        final List<String> parts = NativeSqlCalc.collectAxisKeyParts(
            new Member[] {measure, brand, category},
            axisHierarchies,
            2);

        assertEquals(Arrays.asList("Chocolate", "Brand X"), parts);
    }

    public void testCollectAxisKeyParts_padsMissingAxisWithNull() {
        final Dimension categoryDim = mock(Dimension.class);
        final RolapHierarchy categoryHier = mock(RolapHierarchy.class);
        final RolapMember category = mock(RolapMember.class);

        when(categoryHier.getDimension()).thenReturn(categoryDim);
        when(category.isMeasure()).thenReturn(false);
        when(category.isAll()).thenReturn(false);
        when(category.getHierarchy()).thenReturn(categoryHier);
        when(category.getKey()).thenReturn("Chocolate");

        final Set<Hierarchy> axisHierarchies =
            new LinkedHashSet<Hierarchy>(Collections.singletonList(categoryHier));

        final List<String> parts = NativeSqlCalc.collectAxisKeyParts(
            new Member[] {category},
            axisHierarchies,
            2);

        assertEquals(Arrays.asList("Chocolate", "null"), parts);
    }

    public void testCollectAxisHierarchies_tupleType() {
        final Dimension d1 = mock(Dimension.class);
        final Dimension d2 = mock(Dimension.class);
        final Hierarchy h1 = mock(Hierarchy.class);
        final Hierarchy h2 = mock(Hierarchy.class);
        when(h1.getDimension()).thenReturn(d1);
        when(h2.getDimension()).thenReturn(d2);
        final TupleType tupleType = new TupleType(new mondrian.olap.type.Type[] {
            new MemberType(d1, h1, null, null),
            new MemberType(d2, h2, null, null)
        });
        final Set<Hierarchy> result = new LinkedHashSet<Hierarchy>();

        NativeSqlCalc.collectAxisHierarchies(tupleType, result);

        assertEquals(2, result.size());
        assertTrue(result.contains(h1));
        assertTrue(result.contains(h2));
    }

    public void testResolveAxisHierarchies_queryTupleAxis() {
        final Dimension d1 = mock(Dimension.class);
        final Dimension d2 = mock(Dimension.class);
        final Hierarchy h1 = mock(Hierarchy.class);
        final Hierarchy h2 = mock(Hierarchy.class);
        when(h1.getDimension()).thenReturn(d1);
        when(h2.getDimension()).thenReturn(d2);
        final TupleType tupleType = new TupleType(new mondrian.olap.type.Type[] {
            new MemberType(d1, h1, null, null),
            new MemberType(d2, h2, null, null)
        });
        final Exp setExp = mock(Exp.class);
        when(setExp.getType()).thenReturn(new SetType(tupleType));

        final QueryAxis axis = mock(QueryAxis.class);
        when(axis.getSet()).thenReturn(setExp);

        final Query query = mock(Query.class);
        when(query.getAxes()).thenReturn(new QueryAxis[] {axis});

        final Set<Hierarchy> result =
            NativeSqlCalc.resolveAxisHierarchies(query);

        assertEquals(2, result.size());
        assertTrue(result.contains(h1));
        assertTrue(result.contains(h2));
    }

    public void testBuildWhereFromPredicates_excludesAtomicHierarchy() {
        final List<NativeSqlCalc.PredicateInfo> predicates =
            Arrays.<NativeSqlCalc.PredicateInfo>asList(
                new NativeSqlCalc.AtomicPredicateInfo(
                    "Продукт", "Категория", "f.category = 'Шоколад'"),
                new NativeSqlCalc.AtomicPredicateInfo(
                    "Продукт", "Бренд", "f.brand = 'X'"));

        final String sql = NativeSqlCalc.buildWhereFromPredicates(
            predicates,
            new LinkedHashSet<String>(
                Arrays.asList("Продукт.Бренд")));

        assertEquals("f.category = 'Шоколад'", sql);
    }

    public void testBuildWhereFromPredicates_orOfAndExclusion() {
        final NativeSqlCalc.PredicateInfo categoryA =
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Категория", "f.category = 'Шоколад'");
        final NativeSqlCalc.PredicateInfo brandX =
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "f.brand = 'X'");
        final NativeSqlCalc.PredicateInfo categoryB =
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Категория", "f.category = 'Шоколад'");
        final NativeSqlCalc.PredicateInfo brandY =
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "f.brand = 'Y'");

        final NativeSqlCalc.PredicateInfo branch1 =
            new NativeSqlCalc.CompositePredicateInfo(
                "AND",
                Arrays.asList(categoryA, brandX));
        final NativeSqlCalc.PredicateInfo branch2 =
            new NativeSqlCalc.CompositePredicateInfo(
                "AND",
                Arrays.asList(categoryB, brandY));
        final List<NativeSqlCalc.PredicateInfo> predicates =
            Arrays.<NativeSqlCalc.PredicateInfo>asList(
                new NativeSqlCalc.CompositePredicateInfo(
                    "OR",
                    Arrays.asList(branch1, branch2)));

        final String sql = NativeSqlCalc.buildWhereFromPredicates(
            predicates,
            new LinkedHashSet<String>(
                Arrays.asList("Продукт.Бренд")));

        assertEquals(
            "(f.category = 'Шоколад' OR f.category = 'Шоколад')",
            sql);
    }

    public void testResolvePredicateColumnSql_addsJoinForDimColumn() {
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        final RolapStar.Table dimTable = mock(RolapStar.Table.class);
        final RolapStar.Column dimColumn = mock(RolapStar.Column.class);
        final RolapStar.Condition joinCond = mock(RolapStar.Condition.class);
        final MondrianDef.Column expr = new MondrianDef.Column(null, "month_fd");
        final MondrianDef.Column left = new MondrianDef.Column(null, "period_month");
        final MondrianDef.Column right = new MondrianDef.Column(null, "period_month");

        when(star.getFactTable()).thenReturn(factTable);
        when(dimColumn.getExpression()).thenReturn(expr);
        when(dimColumn.getName()).thenReturn("month_fd");
        when(dimColumn.getTable()).thenReturn(dimTable);
        when(dimTable.getAlias()).thenReturn("per");
        when(dimTable.getTableName()).thenReturn("dim_konfet_period");
        when(dimTable.getJoinCondition()).thenReturn(joinCond);
        when(joinCond.getLeft()).thenReturn(left);
        when(joinCond.getRight()).thenReturn(right);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();
        final NativeSqlCalc.ResolvedColumnSql resolved =
            NativeSqlCalc.resolvePredicateColumnSql(
                dimColumn, star, "f", joins, seenJoins);

        assertEquals("per.month_fd", resolved.qualifiedColumn);
        assertEquals(1, joins.size());
        assertEquals(
            "JOIN dim_konfet_period per ON f.period_month = per.period_month",
            joins.get(0));
    }

    public void testResolveLevelColumnSql_usesStarJoinPath() {
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        final RolapStar.Table dimTable = mock(RolapStar.Table.class);
        final RolapStar.Column dimColumn = mock(RolapStar.Column.class);
        final RolapStar.Condition joinCond = mock(RolapStar.Condition.class);
        final MondrianDef.Column keyExp = new MondrianDef.Column("per", "month_fd");
        final MondrianDef.Column expr = new MondrianDef.Column("per", "month_fd");
        final MondrianDef.Column left = new MondrianDef.Column(null, "period_month");
        final MondrianDef.Column right = new MondrianDef.Column(null, "period_month");

        when(star.lookupColumn("per", "month_fd")).thenReturn(dimColumn);
        when(star.getFactTable()).thenReturn(factTable);
        when(dimColumn.getExpression()).thenReturn(expr);
        when(dimColumn.getName()).thenReturn("month_fd");
        when(dimColumn.getTable()).thenReturn(dimTable);
        when(dimTable.getAlias()).thenReturn("per");
        when(dimTable.getTableName()).thenReturn("dim_konfet_period");
        when(dimTable.getJoinCondition()).thenReturn(joinCond);
        when(joinCond.getLeft()).thenReturn(left);
        when(joinCond.getRight()).thenReturn(right);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();
        final NativeSqlCalc.ResolvedColumnSql resolved =
            NativeSqlCalc.resolveLevelColumnSql(
                keyExp, star, "f", joins, seenJoins);

        assertEquals("per.month_fd", resolved.qualifiedColumn);
        assertEquals(1, joins.size());
        assertEquals(
            "JOIN dim_konfet_period per ON f.period_month = per.period_month",
            joins.get(0));
    }

    public void testResolveLevelAndPredicateColumnSql_shareJoinContract() {
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        final RolapStar.Table dimTable = mock(RolapStar.Table.class);
        final RolapStar.Column dimColumn = mock(RolapStar.Column.class);
        final RolapStar.Condition joinCond = mock(RolapStar.Condition.class);
        final MondrianDef.Column keyExp = new MondrianDef.Column("per", "month_fd");
        final MondrianDef.Column expr = new MondrianDef.Column("per", "month_fd");
        final MondrianDef.Column left = new MondrianDef.Column(null, "period_month");
        final MondrianDef.Column right = new MondrianDef.Column(null, "period_month");

        when(star.lookupColumn("per", "month_fd")).thenReturn(dimColumn);
        when(star.getFactTable()).thenReturn(factTable);
        when(dimColumn.getExpression()).thenReturn(expr);
        when(dimColumn.getName()).thenReturn("month_fd");
        when(dimColumn.getTable()).thenReturn(dimTable);
        when(dimTable.getAlias()).thenReturn("per");
        when(dimTable.getTableName()).thenReturn("dim_konfet_period");
        when(dimTable.getJoinCondition()).thenReturn(joinCond);
        when(joinCond.getLeft()).thenReturn(left);
        when(joinCond.getRight()).thenReturn(right);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        NativeSqlCalc.resolveLevelColumnSql(
            keyExp, star, "f", joins, seenJoins);
        NativeSqlCalc.resolvePredicateColumnSql(
            dimColumn, star, "f", joins, seenJoins);

        assertEquals(1, joins.size());
        assertEquals(
            "JOIN dim_konfet_period per ON f.period_month = per.period_month",
            joins.get(0));
    }

    public void testResolvePredicateMetadata_fromColumnMatch() {
        final RolapCube baseCube = mock(RolapCube.class);
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        final Dimension dimension = mock(Dimension.class);
        final RolapLevel level = mock(RolapLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final MondrianDef.Column expr = new MondrianDef.Column("per", "month_fd");

        when(baseCube.getHierarchies()).thenReturn(
            Collections.singletonList(hierarchy));
        when(hierarchy.getDimension()).thenReturn(dimension);
        when(dimension.getName()).thenReturn("Период");
        when(hierarchy.getName()).thenReturn("Месяц");
        when(hierarchy.getLevels()).thenReturn(new Level[] {level});
        when(level.getKeyExp()).thenReturn(new MondrianDef.Column("per", "month_fd"));
        when(column.getExpression()).thenReturn(expr);

        final NativeSqlCalc.PredicateMetadata metadata =
            NativeSqlCalc.resolvePredicateMetadata(null, column, baseCube);

        assertEquals("Период", metadata.dimensionName);
        assertEquals("Месяц", metadata.hierarchyName);
    }

    public void testResolvePredicateMetadata_aliasMismatchReturnsUnknown() {
        final RolapCube baseCube = mock(RolapCube.class);
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        final Dimension dimension = mock(Dimension.class);
        final RolapLevel level = mock(RolapLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final MondrianDef.Column expr = new MondrianDef.Column("prod", "manufacturer_group");

        when(baseCube.getHierarchies()).thenReturn(
            Collections.singletonList(hierarchy));
        when(hierarchy.getDimension()).thenReturn(dimension);
        when(dimension.getName()).thenReturn("Продукт");
        when(hierarchy.getName()).thenReturn("Производитель");
        when(hierarchy.getLevels()).thenReturn(new Level[] {level});
        when(level.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));
        when(column.getExpression()).thenReturn(expr);

        final NativeSqlCalc.PredicateMetadata metadata =
            NativeSqlCalc.resolvePredicateMetadata(null, column, baseCube);

        assertEquals("unknown", metadata.dimensionName);
        assertEquals("unknown", metadata.hierarchyName);
    }

    public void testResolvePredicateMetadata_sameTableFallbackPrefersFlatHierarchy() {
        final RolapCube baseCube = mock(RolapCube.class);
        final RolapHierarchy hierarchy1 = mock(RolapHierarchy.class);
        final RolapHierarchy hierarchy2 = mock(RolapHierarchy.class);
        final Dimension dimension = mock(Dimension.class);
        final RolapLevel level1 = mock(RolapLevel.class);
        final RolapLevel level2 = mock(RolapLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapStar.Table starTable = mock(RolapStar.Table.class);
        final MondrianDef.Column expr =
            new MondrianDef.Column("prod", "manufacturer_group");
        final MondrianDef.Table table1 = new MondrianDef.Table();
        final MondrianDef.Table table2 = new MondrianDef.Table();

        table1.name = "dim_konfet_product";
        table2.name = "dim_konfet_product";

        when(baseCube.getHierarchies()).thenReturn(
            Arrays.asList(hierarchy1, hierarchy2));

        when(hierarchy1.getDimension()).thenReturn(dimension);
        when(hierarchy2.getDimension()).thenReturn(dimension);
        when(dimension.getName()).thenReturn("Продукт");

        when(hierarchy1.getName()).thenReturn("Марка");
        when(hierarchy1.getLevels()).thenReturn(new Level[] {level1});
        when(hierarchy1.getRelation()).thenReturn(table1);
        when(level1.getName()).thenReturn("Производитель");
        when(level1.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(hierarchy2.getName()).thenReturn("Производитель");
        when(hierarchy2.getLevels()).thenReturn(new Level[] {level2});
        when(hierarchy2.getRelation()).thenReturn(table2);
        when(level2.getName()).thenReturn("Производитель");
        when(level2.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(column.getExpression()).thenReturn(expr);
        when(column.getTable()).thenReturn(starTable);
        when(starTable.getTableName()).thenReturn("dim_konfet_product");

        final NativeSqlCalc.PredicateMetadata metadata =
            NativeSqlCalc.resolvePredicateMetadata(null, column, baseCube);

        assertEquals("Продукт", metadata.dimensionName);
        assertEquals("Производитель", metadata.hierarchyName);
        assertTrue(metadata.exclusionNames.contains("Продукт"));
        assertTrue(metadata.exclusionNames.contains("Продукт.Марка"));
        assertTrue(metadata.exclusionNames.contains("Продукт.Производитель"));
    }

    public void testMergePredicateMetadata_unionsExclusionAliases() {
        final NativeSqlCalc.PredicateMetadata memberMetadata =
            new NativeSqlCalc.PredicateMetadata(
                "Продукт",
                "Марка");
        final NativeSqlCalc.PredicateMetadata columnMetadata =
            new NativeSqlCalc.PredicateMetadata(
                "Продукт",
                "Производитель");

        final NativeSqlCalc.PredicateMetadata merged =
            NativeSqlCalc.mergePredicateMetadata(
                memberMetadata,
                columnMetadata);

        assertEquals("Продукт", merged.dimensionName);
        assertEquals("Марка", merged.hierarchyName);
        assertTrue(merged.exclusionNames.contains("Продукт.Марка"));
        assertTrue(merged.exclusionNames.contains("Продукт.Производитель"));
    }

    public void testPredicateMetadata_doesNotDoublePrefixQualifiedHierarchyName() {
        final NativeSqlCalc.PredicateMetadata metadata =
            new NativeSqlCalc.PredicateMetadata(
                "Продукт",
                "Продукт.Производитель");

        assertTrue(metadata.exclusionNames.contains("Продукт"));
        assertTrue(metadata.exclusionNames.contains("Продукт.Производитель"));
        assertFalse(metadata.exclusionNames.contains(
            "Продукт.Продукт.Производитель"));
    }

    public void testCollectSiblingHierarchyExclusionNames_sameDimensionSameColumn() {
        final RolapCube baseCube = mock(RolapCube.class);
        final RolapMember member = mock(RolapMember.class);
        final RolapHierarchy memberHierarchy = mock(RolapHierarchy.class);
        final Dimension dimension = mock(Dimension.class);
        final RolapHierarchy hierarchy1 = mock(RolapHierarchy.class);
        final RolapHierarchy hierarchy2 = mock(RolapHierarchy.class);
        final RolapLevel level1 = mock(RolapLevel.class);
        final RolapLevel level2 = mock(RolapLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);

        when(member.getHierarchy()).thenReturn(memberHierarchy);
        when(memberHierarchy.getDimension()).thenReturn(dimension);
        when(dimension.getName()).thenReturn("Продукт");
        when(baseCube.getHierarchies()).thenReturn(
            Arrays.asList(hierarchy1, hierarchy2));

        when(hierarchy1.getDimension()).thenReturn(dimension);
        when(hierarchy1.getName()).thenReturn("Марка");
        when(hierarchy1.getLevels()).thenReturn(new Level[] {level1});
        when(level1.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(hierarchy2.getDimension()).thenReturn(dimension);
        when(hierarchy2.getName()).thenReturn("Производитель");
        when(hierarchy2.getLevels()).thenReturn(new Level[] {level2});
        when(level2.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(column.getExpression()).thenReturn(
            new MondrianDef.Column("prod", "manufacturer_group"));

        final Set<String> exclusionNames =
            NativeSqlCalc.collectSiblingHierarchyExclusionNames(
                member,
                column,
                baseCube);

        assertTrue(exclusionNames.contains("Продукт"));
        assertTrue(exclusionNames.contains("Продукт.Марка"));
        assertTrue(exclusionNames.contains("Продукт.Производитель"));
    }

    public void testCollectSiblingHierarchyExclusionNames_ignoresOtherDimensions() {
        final RolapCube baseCube = mock(RolapCube.class);
        final RolapMember member = mock(RolapMember.class);
        final RolapHierarchy memberHierarchy = mock(RolapHierarchy.class);
        final Dimension productDimension = mock(Dimension.class);
        final Dimension storeDimension = mock(Dimension.class);
        final RolapHierarchy productHierarchy = mock(RolapHierarchy.class);
        final RolapHierarchy storeHierarchy = mock(RolapHierarchy.class);
        final RolapLevel productLevel = mock(RolapLevel.class);
        final RolapLevel storeLevel = mock(RolapLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);

        when(member.getHierarchy()).thenReturn(memberHierarchy);
        when(memberHierarchy.getDimension()).thenReturn(productDimension);
        when(productDimension.getName()).thenReturn("Продукт");
        when(storeDimension.getName()).thenReturn("ТТ");
        when(baseCube.getHierarchies()).thenReturn(
            Arrays.asList(productHierarchy, storeHierarchy));

        when(productHierarchy.getDimension()).thenReturn(productDimension);
        when(productHierarchy.getName()).thenReturn("Производитель");
        when(productHierarchy.getLevels()).thenReturn(new Level[] {productLevel});
        when(productLevel.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(storeHierarchy.getDimension()).thenReturn(storeDimension);
        when(storeHierarchy.getName()).thenReturn("Сеть");
        when(storeHierarchy.getLevels()).thenReturn(new Level[] {storeLevel});
        when(storeLevel.getKeyExp()).thenReturn(
            new MondrianDef.Column("s", "manufacturer_group"));

        when(column.getExpression()).thenReturn(
            new MondrianDef.Column("prod", "manufacturer_group"));

        final Set<String> exclusionNames =
            NativeSqlCalc.collectSiblingHierarchyExclusionNames(
                member,
                column,
                baseCube);

        assertTrue(exclusionNames.contains("Продукт"));
        assertTrue(exclusionNames.contains("Продукт.Производитель"));
        assertFalse(exclusionNames.contains("ТТ"));
        assertFalse(exclusionNames.contains("ТТ.Сеть"));
    }

    public void testResolvePredicateMetadata_exactMatchPrefersFlatHierarchy() {
        final RolapCube baseCube = mock(RolapCube.class);
        final RolapHierarchy hierarchy1 = mock(RolapHierarchy.class);
        final RolapHierarchy hierarchy2 = mock(RolapHierarchy.class);
        final Dimension dimension = mock(Dimension.class);
        final RolapLevel level1 = mock(RolapLevel.class);
        final RolapLevel level2 = mock(RolapLevel.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final MondrianDef.Column expr =
            new MondrianDef.Column("p", "manufacturer_group");

        when(baseCube.getHierarchies()).thenReturn(
            Arrays.asList(hierarchy1, hierarchy2));

        when(hierarchy1.getDimension()).thenReturn(dimension);
        when(hierarchy2.getDimension()).thenReturn(dimension);
        when(dimension.getName()).thenReturn("Продукт");

        when(hierarchy1.getName()).thenReturn("Марка");
        when(hierarchy1.getLevels()).thenReturn(new Level[] {level1});
        when(level1.getName()).thenReturn("Производитель");
        when(level1.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(hierarchy2.getName()).thenReturn("Производитель");
        when(hierarchy2.getLevels()).thenReturn(new Level[] {level2});
        when(level2.getName()).thenReturn("Производитель");
        when(level2.getKeyExp()).thenReturn(
            new MondrianDef.Column("p", "manufacturer_group"));

        when(column.getExpression()).thenReturn(expr);

        final NativeSqlCalc.PredicateMetadata metadata =
            NativeSqlCalc.resolvePredicateMetadata(null, column, baseCube);

        assertEquals("Продукт", metadata.dimensionName);
        assertEquals("Производитель", metadata.hierarchyName);
    }

    public void testSubstitutePlaceholders_multipleAxes() {
        String template =
            "SELECT ${axisExpr1} AS k1, ${axisExpr2} AS k2, "
            + "sum(${factAlias}.val) AS val "
            + "FROM ${factTable} ${factAlias} "
            + "GROUP BY k1, k2";

        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("factTable", "fact_sales");
        ph.put("factAlias", "f");
        ph.put("axisExpr1", "f.region");
        ph.put("axisExpr2", "d.brand_name");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertTrue(result.contains("f.region AS k1"));
        assertTrue(result.contains("d.brand_name AS k2"));
        assertTrue(result.contains("fact_sales f"));
        assertFalse(result.contains("${"));
    }

    public void testSubstitutePlaceholders_noPlaceholders() {
        String template = "SELECT 1 AS val";
        Map<String, String> ph = Collections.emptyMap();
        String result = NativeSqlCalc.substitutePlaceholders(template, ph);
        assertEquals("SELECT 1 AS val", result);
    }

    public void testSubstitutePlaceholders_duplicatePlaceholder() {
        String template =
            "SELECT ${col} AS k1, sum(${col}) AS val FROM t";
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("col", "amount");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertEquals(
            "SELECT amount AS k1, sum(amount) AS val FROM t",
            result);
    }

    public void testSubstitutePlaceholders_staticVariables() {
        String template =
            "SELECT ${axisExpr1} AS k1, "
            + "sum(${factAlias}.${weightMeasure}) * ${multiplier} AS val "
            + "FROM ${factTable} ${factAlias}";

        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("factTable", "mart_weekly");
        ph.put("factAlias", "f");
        ph.put("axisExpr1", "f.brand");
        ph.put("weightMeasure", "sales_rub");
        ph.put("multiplier", "100");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertTrue(result.contains("sum(f.sales_rub) * 100"));
        assertTrue(result.contains("f.brand AS k1"));
    }

    public void testFormatLiteral_number() {
        assertEquals("42", NativeSqlCalc.formatLiteral(42));
        assertEquals("3.14", NativeSqlCalc.formatLiteral(3.14));
    }

    public void testFormatLiteral_string() {
        assertEquals("'hello'", NativeSqlCalc.formatLiteral("hello"));
    }

    public void testFormatLiteral_stringWithQuotes() {
        assertEquals("'it''s'", NativeSqlCalc.formatLiteral("it's"));
    }

    public void testFormatLiteral_null() {
        assertEquals("NULL", NativeSqlCalc.formatLiteral(null));
    }

    public void testFormatLiteral_cyrillic() {
        assertEquals("'Магнит'", NativeSqlCalc.formatLiteral("Магнит"));
    }

}
