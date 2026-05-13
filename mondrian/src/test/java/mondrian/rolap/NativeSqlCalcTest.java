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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import javax.sql.DataSource;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
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
import mondrian.rolap.aggmatcher.AggStar;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NativeSqlCalc} template substitution logic.
 */
public class NativeSqlCalcTest {

    @Test public void testSubstitutePlaceholders_basic() {
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

    @Test public void testSubstitutePlaceholders_emptyWhere() {
        String template = "WHERE ${whereClause}";
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("whereClause", "1 = 1");
        String result = NativeSqlCalc.substitutePlaceholders(template, ph);
        assertEquals("WHERE 1 = 1", result);
    }

    @Test public void testSubstitutePlaceholders_unresolvedPlaceholder() {
        String template = "SELECT ${unknownVar} AS x";
        Map<String, String> ph = Collections.emptyMap();
        try {
            NativeSqlCalc.substitutePlaceholders(template, ph);
            fail("Expected exception for unresolved placeholder");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("unknownVar"));
        }
    }

    @Test public void testSubstitutePlaceholders_axisExprBeyondRange() {
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

    @Test public void testEncodeRowKey() {
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

    @Test public void testCollectAxisKeyParts_preservesAxisOrder() {
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

        final List<NativeSqlCalc.AxisBinding> axisBindings =
            Arrays.asList(
                new NativeSqlCalc.AxisBinding(
                    categoryHier,
                    "Категория",
                    "f.category",
                    "category",
                    "k0"),
                new NativeSqlCalc.AxisBinding(
                    brandHier,
                    "Бренд",
                    "d.brand_name",
                    "brand_name",
                    "k1"));

        final List<String> parts = NativeSqlCalc.collectAxisKeyParts(
            new Member[] {measure, brand, category},
            axisBindings);

        assertEquals(Arrays.asList("Chocolate", "Brand X"), parts);
    }

    @Test public void testCollectAxisKeyParts_skipsMissingAxisWithoutPadding() {
        final Dimension categoryDim = mock(Dimension.class);
        final Dimension brandDim = mock(Dimension.class);
        final RolapHierarchy categoryHier = mock(RolapHierarchy.class);
        final RolapHierarchy brandHier = mock(RolapHierarchy.class);
        final RolapMember category = mock(RolapMember.class);

        when(categoryHier.getDimension()).thenReturn(categoryDim);
        when(brandHier.getDimension()).thenReturn(brandDim);
        when(category.isMeasure()).thenReturn(false);
        when(category.isAll()).thenReturn(false);
        when(category.getHierarchy()).thenReturn(categoryHier);
        when(category.getKey()).thenReturn("Chocolate");

        final List<NativeSqlCalc.AxisBinding> axisBindings =
            Arrays.asList(
                new NativeSqlCalc.AxisBinding(
                    categoryHier,
                    "Категория",
                    "f.category",
                    "category",
                    "k0"),
                new NativeSqlCalc.AxisBinding(
                    brandHier,
                    "Бренд",
                    "d.brand_name",
                    "brand_name",
                    "k1"));

        final List<String> parts = NativeSqlCalc.collectAxisKeyParts(
            new Member[] {category},
            axisBindings);

        assertEquals(Collections.singletonList("Chocolate"), parts);
    }

    @Test public void testCollectAxisHierarchies_tupleType() {
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

    @Test public void testResolveAxisHierarchies_queryTupleAxis() {
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

    @Test public void testBuildWhereFromPredicates_excludesAtomicHierarchy() {
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

    @Test public void testBuildWhereFromPredicates_orOfAndExclusion() {
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

    @Test public void testResolvePredicateColumnSql_addsJoinForDimColumn() {
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

    @Test public void testResolveLevelColumnSql_usesStarJoinPath() {
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

    @Test public void testResolveLevelAndPredicateColumnSql_shareJoinContract() {
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

    @Test public void testResolvePredicateMetadata_fromColumnMatch() {
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

    @Test public void testResolvePredicateMetadata_aliasMismatchReturnsUnknown() {
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

    @Test public void testResolvePredicateMetadata_sameTableFallbackPrefersFlatHierarchy() {
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

    @Test public void testMergePredicateMetadata_unionsExclusionAliases() {
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

    @Test public void testPredicateMetadata_doesNotDoublePrefixQualifiedHierarchyName() {
        final NativeSqlCalc.PredicateMetadata metadata =
            new NativeSqlCalc.PredicateMetadata(
                "Продукт",
                "Продукт.Производитель");

        assertTrue(metadata.exclusionNames.contains("Продукт"));
        assertTrue(metadata.exclusionNames.contains("Продукт.Производитель"));
        assertFalse(metadata.exclusionNames.contains(
            "Продукт.Продукт.Производитель"));
    }

    @Test public void testCollectSiblingHierarchyExclusionNames_sameDimensionSameColumn() {
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

    @Test public void testCollectSiblingHierarchyExclusionNames_ignoresOtherDimensions() {
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

    @Test public void testResolvePredicateMetadata_exactMatchPrefersFlatHierarchy() {
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

    @Test public void testSubstitutePlaceholders_multipleAxes() {
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

    @Test public void testRenderAxisPlaceholderLists() {
        final List<NativeSqlCalc.AxisBinding> axisBindings =
            Arrays.asList(
                new NativeSqlCalc.AxisBinding(
                    null,
                    "Категория",
                    "f.category",
                    "category",
                    "k0"),
                new NativeSqlCalc.AxisBinding(
                    null,
                    "Производитель",
                    "p.manufacturer_group",
                    "manufacturer_group",
                    "k1"),
                new NativeSqlCalc.AxisBinding(
                    null,
                    "Квартал",
                    "d.quarter",
                    "quarter",
                    "k2"));

        assertEquals(
            ",\n    f.category AS k0,\n    p.manufacturer_group AS k1,\n    d.quarter AS k2",
            NativeSqlCalc.renderAxisPresenceSelectList(axisBindings));
        assertEquals(
            "  pr.k0 AS k0,\n  pr.k1 AS k1,\n  pr.k2 AS k2,\n",
            NativeSqlCalc.renderAxisResultSelectList(axisBindings, "pr"));
        assertEquals(
            "pr.k0, pr.k1, pr.k2, ",
            NativeSqlCalc.renderAxisGroupByList(axisBindings, "pr"));
    }

    @Test public void testSubstitutePlaceholders_noPlaceholders() {
        String template = "SELECT 1 AS val";
        Map<String, String> ph = Collections.emptyMap();
        String result = NativeSqlCalc.substitutePlaceholders(template, ph);
        assertEquals("SELECT 1 AS val", result);
    }

    @Test public void testSubstitutePlaceholders_duplicatePlaceholder() {
        String template =
            "SELECT ${col} AS k1, sum(${col}) AS val FROM t";
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("col", "amount");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertEquals(
            "SELECT amount AS k1, sum(amount) AS val FROM t",
            result);
    }

    @Test public void testSubstitutePlaceholders_staticVariables() {
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

    @Test public void testFormatLiteral_number() {
        assertEquals("42", NativeSqlCalc.formatLiteral(42));
        assertEquals("3.14", NativeSqlCalc.formatLiteral(3.14));
    }

    @Test public void testFormatLiteral_string() {
        assertEquals("'hello'", NativeSqlCalc.formatLiteral("hello"));
    }

    @Test public void testFormatLiteral_stringWithQuotes() {
        assertEquals("'it''s'", NativeSqlCalc.formatLiteral("it's"));
    }

    @Test public void testFormatLiteral_null() {
        assertEquals("NULL", NativeSqlCalc.formatLiteral(null));
    }

    @Test public void testFormatLiteral_cyrillic() {
        assertEquals("'Магнит'", NativeSqlCalc.formatLiteral("Магнит"));
    }

    // ---- resolveFirstViableTemplate tests ----

    @Test public void testResolveFirstViableTemplate_primarySucceeds() {
        String template0 = "SELECT ${brand_key} FROM agg_table";
        String template1 = "SELECT ${brand_key} FROM fact_table";
        List<String> templates = Arrays.asList(template0, template1);

        Map<String, String> placeholders = new LinkedHashMap<>();
        placeholders.put("brand_key", "d.brand_name");

        String result = NativeSqlCalc.resolveFirstViableTemplate(
            templates, placeholders, null);
        assertEquals("SELECT d.brand_name FROM agg_table", result);
    }

    @Test public void testResolveFirstViableTemplate_fallbackOnUnresolved() {
        String template0 = "SELECT ${sku_key}, ${brand_key} FROM agg_table";
        String template1 = "SELECT ${brand_key} FROM fact_table";
        List<String> templates = Arrays.asList(template0, template1);

        Map<String, String> placeholders = new LinkedHashMap<>();
        placeholders.put("brand_key", "d.brand_name");

        String result = NativeSqlCalc.resolveFirstViableTemplate(
            templates, placeholders, null);
        assertNotNull(result);
        assertEquals("SELECT d.brand_name FROM fact_table", result);
    }

    @Test public void testResolveFirstViableTemplate_allFail() {
        String template0 = "SELECT ${sku_key} FROM agg_table";
        String template1 = "SELECT ${sku_key} FROM fact_table";
        List<String> templates = Arrays.asList(template0, template1);

        Map<String, String> placeholders = new LinkedHashMap<>();
        placeholders.put("brand_key", "d.brand_name");

        String result = NativeSqlCalc.resolveFirstViableTemplate(
            templates, placeholders, null);
        assertNull(result);
    }

    @Test
    public void testRenderAxisSelectListNoPrefix() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(null, "Brand", "goods.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(null, "Region", "store.region", "region", "k1")
        );
        String result = NativeSqlCalc.renderAxisSelectListNoPrefix(bindings);
        assertTrue(result.contains("k0,"));
        assertTrue(result.contains("k1,"));
        assertFalse(result.contains("pr."));
        assertFalse(result.contains("goods."));
    }

    @Test
    public void testDenominatorProjection_allExcluded_scalar() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(null, "Prod.Brand", "dim.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(null, "Prod.Mfr", "dim.mfr", "mfr", "k1")
        );
        Set<String> except = new LinkedHashSet<String>(Arrays.asList("Prod.Brand", "Prod.Mfr"));
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(bindings, except);
        assertTrue(dp.isScalar());
        assertEquals(0, dp.getKeptBindings().size());
    }

    @Test
    public void testDenominatorProjection_partialExclude() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(null, "Prod.Brand", "dim.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(null, "Store.Region", "dim.region", "region", "k1")
        );
        Set<String> except = new LinkedHashSet<String>(Arrays.asList("Prod.Brand"));
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(bindings, except);
        assertFalse(dp.isScalar());
        assertEquals(1, dp.getKeptBindings().size());
        assertEquals("k1", dp.getKeptBindings().get(0).keyAlias);
        assertEquals("region", dp.getKeptBindings().get(0).columnName);
    }

    @Test
    public void testDenominatorProjection_preservesAxisOrder() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(null, "Prod.Brand", "dim.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(null, "Store.Region", "dim.region", "region", "k1"),
            new NativeSqlCalc.AxisBinding(null, "Time.Quarter", "dim.quarter", "quarter", "k2")
        );
        Set<String> except = new LinkedHashSet<String>(Arrays.asList("Prod.Brand"));
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(bindings, except);
        assertEquals(2, dp.getKeptBindings().size());
        assertEquals("k1", dp.getKeptBindings().get(0).keyAlias);
        assertEquals("k2", dp.getKeptBindings().get(1).keyAlias);
    }

    @Test
    public void testDenominatorProjection_canonicalIdentity() {
        // Two bindings with same hierarchyName but different qualifiedColumn
        // Both excluded by one except entry
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(null, "Prod.Brand", "dim.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(null, "Prod.Brand", "agg.brand_name", "brand_name", "k1"),
            new NativeSqlCalc.AxisBinding(null, "Store.Region", "dim.region", "region", "k2")
        );
        Set<String> except = new LinkedHashSet<String>(Arrays.asList("Prod.Brand"));
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(bindings, except);
        assertEquals(1, dp.getKeptBindings().size());
        assertEquals("k2", dp.getKeptBindings().get(0).keyAlias);
    }

    @Test
    public void testParseExceptNames_basic() {
        Set<String> result = NativeSqlCalc.parseExceptNames("Prod.Brand,Store.Region");
        assertEquals(2, result.size());
        assertTrue(result.contains("Prod.Brand"));
        assertTrue(result.contains("Store.Region"));
    }

    @Test
    public void testParseExceptNames_trimming() {
        Set<String> result = NativeSqlCalc.parseExceptNames("  Prod.Brand , Store.Region  ");
        assertEquals(2, result.size());
        assertTrue(result.contains("Prod.Brand"));
        assertTrue(result.contains("Store.Region"));
    }

    @Test
    public void testParseExceptNames_nullAndEmpty() {
        assertTrue(NativeSqlCalc.parseExceptNames(null).isEmpty());
        assertTrue(NativeSqlCalc.parseExceptNames("").isEmpty());
        assertTrue(NativeSqlCalc.parseExceptNames("  ,  , ").isEmpty());
    }

    // ---------------------------------------------------------------
    // renderDenominatorSelect
    // ---------------------------------------------------------------

    @Test
    public void testRenderDenominatorSelect_scalar() {
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                Collections.<NativeSqlCalc.AxisBinding>emptyList(),
                Collections.<String>emptySet());
        assertEquals("", NativeSqlCalc.renderDenominatorSelect(dp));
    }

    @Test
    public void testRenderDenominatorSelect_qualifiedColumn_dimOnly() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Time.Year", "dim_konfet_period.year", "year", "k1")
        );
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                bindings, Collections.<String>emptySet());
        String result = NativeSqlCalc.renderDenominatorSelect(dp);
        assertTrue(result.contains("dim_konfet_store.region AS k0,"));
        assertTrue(result.contains("dim_konfet_period.year AS k1,"));
    }

    @Test
    public void testRenderDenominatorSelect_qualifiedColumn_fkOnFact() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Time.Month", "f.period_month", "period_month", "k0")
        );
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                bindings, Collections.<String>emptySet());
        String result = NativeSqlCalc.renderDenominatorSelect(dp);
        assertTrue(result.contains("f.period_month AS k0,"));
    }

    @Test
    public void testRenderDenominatorSelect_qualifiedColumn_mixed() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Prod.Brand", "dim_konfet_product.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k1"),
            new NativeSqlCalc.AxisBinding(
                null, "Time.Month", "f.period_month", "period_month", "k2")
        );
        Set<String> except = new LinkedHashSet<String>(
            Arrays.asList("Prod.Brand"));
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(bindings, except);
        String result = NativeSqlCalc.renderDenominatorSelect(dp);
        assertTrue(result.contains("dim_konfet_store.region AS k1,"));
        assertTrue(result.contains("f.period_month AS k2,"));
        assertFalse(result.contains("brand"));
    }

    // ---------------------------------------------------------------
    // renderDenominatorGroupBy
    // ---------------------------------------------------------------

    @Test
    public void testRenderDenominatorGroupBy_scalar() {
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                Collections.<NativeSqlCalc.AxisBinding>emptyList(),
                Collections.<String>emptySet());
        assertEquals("", NativeSqlCalc.renderDenominatorGroupBy(dp, "src"));
    }

    @Test
    public void testRenderDenominatorGroupBy_twoKeys() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "ds2.region", "region", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Time.Quarter", "dp2.quarter", "quarter", "k1")
        );
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                bindings, Collections.<String>emptySet());
        assertEquals(
            "src.k0, src.k1, ",
            NativeSqlCalc.renderDenominatorGroupBy(dp, "src"));
    }

    @Test
    public void testRenderDenominatorGroupBy_bareAlias_scalar() {
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                Collections.<NativeSqlCalc.AxisBinding>emptyList(),
                Collections.<String>emptySet());
        assertEquals("", NativeSqlCalc.renderDenominatorGroupBy(dp, null));
    }

    @Test
    public void testRenderDenominatorGroupBy_bareAlias_twoKeys() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Time.Month", "f.period_month", "period_month", "k1")
        );
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                bindings, Collections.<String>emptySet());
        assertEquals("k0, k1, ",
            NativeSqlCalc.renderDenominatorGroupBy(dp, null));
    }

    // ---------------------------------------------------------------
    // renderDenominatorJoin
    // ---------------------------------------------------------------

    @Test
    public void testRenderDenominatorJoin_scalar() {
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                Collections.<NativeSqlCalc.AxisBinding>emptyList(),
                Collections.<String>emptySet());
        assertEquals(
            "CROSS JOIN d",
            NativeSqlCalc.renderDenominatorJoin(dp, "pr", "d"));
    }

    @Test
    public void testRenderDenominatorJoin_twoKeys() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "ds2.region", "region", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Time.Quarter", "dp2.quarter", "quarter", "k1")
        );
        NativeSqlCalc.DenominatorProjection dp =
            NativeSqlCalc.DenominatorProjection.build(
                bindings, Collections.<String>emptySet());
        assertEquals(
            "JOIN d ON pr.k0 = d.k0 AND pr.k1 = d.k1",
            NativeSqlCalc.renderDenominatorJoin(dp, "pr", "d"));
    }

    // ---------------------------------------------------------------
    // substitutePlaceholders — denominator macros (v2: qualifiedColumn)
    // ---------------------------------------------------------------

    @Test
    public void testSubstitutePlaceholders_denominatorSelect_noSrcAlias() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Prod.Brand", "dim_konfet_product.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k1")
        );

        String template =
            "SELECT ${denominatorSelect:Prod.Brand} sum(x) AS total";

        String result = NativeSqlCalc.substitutePlaceholders(
            template, Collections.<String, String>emptyMap(), null, bindings);

        assertTrue(result.contains("dim_konfet_store.region AS k1,"));
        assertFalse(result.contains("brand"));
        assertFalse(result.contains("dim_konfet_product"));
    }

    @Test
    public void testSubstitutePlaceholders_denominatorGroupBy_bareMode() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Prod.Brand", "dim_konfet_product.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k1")
        );

        String template =
            "GROUP BY f.store_key, ${denominatorGroupBy:Prod.Brand}1";

        String result = NativeSqlCalc.substitutePlaceholders(
            template, Collections.<String, String>emptyMap(), null, bindings);

        assertTrue(result.contains("k1, 1"));
        assertFalse(result.contains("src.k1"));
    }

    @Test
    public void testSubstitutePlaceholders_denominatorGroupBy_prefixedMode() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Prod.Brand", "dim_konfet_product.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k1")
        );

        String template =
            "SELECT ${denominatorGroupBy:src:Prod.Brand} sum(spt) AS total";

        String result = NativeSqlCalc.substitutePlaceholders(
            template, Collections.<String, String>emptyMap(), null, bindings);

        assertTrue(result.contains("src.k1,"));
        assertFalse(result.contains("brand"));
    }

    @Test
    public void testSubstitutePlaceholders_denominatorJoin_unchanged() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Prod.Brand", "dim_konfet_product.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k1")
        );

        String template = "FROM pr ${denominatorJoin:pr:d:Prod.Brand}";

        String result = NativeSqlCalc.substitutePlaceholders(
            template, Collections.<String, String>emptyMap(), null, bindings);

        assertTrue(result.contains("JOIN d ON pr.k1 = d.k1"));
    }

    @Test
    public void testSubstitutePlaceholders_denominatorScalar_allExcepted() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Prod.Brand", "dim_konfet_product.brand", "brand", "k0")
        );

        String template =
            "SELECT ${denominatorSelect:Prod.Brand} sum(x) "
            + "GROUP BY ${denominatorGroupBy:Prod.Brand} 1 "
            + "GROUP BY ${denominatorGroupBy:src:Prod.Brand} 1 "
            + "FROM pr ${denominatorJoin:pr:d:Prod.Brand}";

        String result = NativeSqlCalc.substitutePlaceholders(
            template, Collections.<String, String>emptyMap(), null, bindings);

        assertTrue(result.contains("SELECT  sum(x)"));
        assertTrue(result.contains("GROUP BY  1"));
        assertTrue(result.contains("CROSS JOIN d"));
    }

    @Test
    public void testSubstitutePlaceholders_mixedMacrosAndPlaceholders() {
        List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Store.Region", "dim_konfet_store.region", "region", "k0")
        );
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("factTable", "mart_sales");
        ph.put("whereClause", "1 = 1");

        String template =
            "SELECT ${denominatorSelect:} sum(x) "
            + "FROM ${factTable} WHERE ${whereClause}";
        String result = NativeSqlCalc.substitutePlaceholders(
            template, ph, null, bindings);

        assertTrue(result.contains("dim_konfet_store.region AS k0,"));
        assertTrue(result.contains("mart_sales"));
        assertTrue(result.contains("1 = 1"));
        assertFalse(result.contains("${"));
    }

    @Test public void testMarkerConstants_distinctAndNonEmpty() {
        assertNotNull(NativeSqlCalc.ALL_MEMBER_MARKER);
        assertNotNull(NativeSqlCalc.NULL_KEY_MARKER);
        assertNotEquals(NativeSqlCalc.ALL_MEMBER_MARKER,
                        NativeSqlCalc.NULL_KEY_MARKER);
        assertEquals("(all)", NativeSqlCalc.ALL_MEMBER_MARKER);
        assertEquals('\0', NativeSqlCalc.NULL_KEY_MARKER.charAt(0));
    }

    @Test public void testNormalizeAxisKey_canonicalForms() {
        assertEquals("2024-01-15", NativeSqlCalc.normalizeAxisKey(
            java.sql.Date.valueOf("2024-01-15"), null));
        assertEquals("2024-01-15", NativeSqlCalc.normalizeAxisKey(
            java.time.LocalDate.parse("2024-01-15"), null));
        assertEquals("1", NativeSqlCalc.normalizeAxisKey(
            new java.math.BigDecimal("1.00"), null));
        assertEquals("1", NativeSqlCalc.normalizeAxisKey(
            new java.math.BigDecimal("1"), null));
        assertEquals("1.5", NativeSqlCalc.normalizeAxisKey(
            new java.math.BigDecimal("1.50"), null));
        assertEquals("1", NativeSqlCalc.normalizeAxisKey(Integer.valueOf(1), null));
        assertEquals("1", NativeSqlCalc.normalizeAxisKey(Long.valueOf(1L), null));
        assertEquals("hello", NativeSqlCalc.normalizeAxisKey("hello", null));
        assertEquals(NativeSqlCalc.NULL_KEY_MARKER,
            NativeSqlCalc.normalizeAxisKey(null, null));
    }

    @Test public void testEscapeAxisKeyPart_separatorAndBackslash() {
        assertEquals("hello", NativeSqlCalc.escapeAxisKeyPart("hello"));
        assertEquals("a\\|b", NativeSqlCalc.escapeAxisKeyPart("a|b"));
        assertEquals("a\\\\b", NativeSqlCalc.escapeAxisKeyPart("a\\b"));
        assertEquals("a\\\\\\|b", NativeSqlCalc.escapeAxisKeyPart("a\\|b"));
    }

    @Test public void testRowKey_separatorEscaping_noCollision() {
        String k1 = NativeSqlCalc.escapeAxisKeyPart("A|B")
            + "|" + NativeSqlCalc.escapeAxisKeyPart("C");
        String k2 = NativeSqlCalc.escapeAxisKeyPart("A")
            + "|" + NativeSqlCalc.escapeAxisKeyPart("B|C");
        assertNotEquals(k1, k2);
        assertEquals("A\\|B|C", k1);
        assertEquals("A|B\\|C", k2);
    }

    @Test public void testEncodeRowKey_allMemberOnAxis_encodesAsAllMarker() {
        // Two axis bindings: hierarchy A (evaluator: All-member), hierarchy B
        // (evaluator: specific). Expected rowKey: "(all)|<specific-key>".
        RolapHierarchy hA = mock(RolapHierarchy.class);
        RolapHierarchy hB = mock(RolapHierarchy.class);

        RolapMember allMemA = mock(RolapMember.class);
        when(allMemA.isAll()).thenReturn(true);
        when(allMemA.getHierarchy()).thenReturn(hA);

        RolapMember specMemB = mock(RolapMember.class);
        when(specMemB.isAll()).thenReturn(false);
        when(specMemB.getHierarchy()).thenReturn(hB);
        when(specMemB.getKey()).thenReturn("Алтайский Выпечка");

        Evaluator evaluator = mock(Evaluator.class);
        when(evaluator.getContext(hA)).thenReturn(allMemA);
        when(evaluator.getContext(hB)).thenReturn(specMemB);

        NativeSqlCalc.AxisBinding bA = new NativeSqlCalc.AxisBinding(
            hA, "[FD].[FD]", "f.fd", "fd", "k0");
        NativeSqlCalc.AxisBinding bB = new NativeSqlCalc.AxisBinding(
            hB, "[Mfr].[Mfr]", "f.mfr", "mfr", "k1");

        String rowKey = NativeSqlCalc.encodeRowKey(
            evaluator, java.util.Arrays.asList(bA, bB));

        assertEquals("(all)|Алтайский Выпечка", rowKey);
    }

    @Test public void testEncodeRowKey_specificMembers_normalizedAndEscaped() {
        RolapHierarchy hA = mock(RolapHierarchy.class);

        RolapMember memA = mock(RolapMember.class);
        when(memA.isAll()).thenReturn(false);
        when(memA.getHierarchy()).thenReturn(hA);
        when(memA.getKey()).thenReturn("with|pipe");  // requires escaping

        Evaluator evaluator = mock(Evaluator.class);
        when(evaluator.getContext(hA)).thenReturn(memA);

        NativeSqlCalc.AxisBinding b = new NativeSqlCalc.AxisBinding(
            hA, "[A].[A]", "f.a", "a", "k0");

        String rowKey = NativeSqlCalc.encodeRowKey(
            evaluator, java.util.Collections.singletonList(b));

        assertEquals("with\\|pipe", rowKey);
    }

    @Test public void testParseResultSet_groupingFlagSet_encodesAsAllMarker()
        throws Exception
    {
        // Schema: 2 axis cols (k0=brand, k1=fd), 2 grouping flags
        // (k0_isAll, k1_isAll), 1 val col.
        // Row 1: ("BrandA", "СЗФО", 0, 0, 31.34) → detail; rowKey "BrandA|СЗФО"
        // Row 2: ("BrandA", null,   0, 1, 50.0)  → grouping subtotal on FD;
        //        rowKey "BrandA|(all)"
        // Row 3: (null,     null,   1, 1, 100.0) → full grand total;
        //        rowKey "(all)|(all)"

        java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
        when(rs.next()).thenReturn(true, true, true, false);

        when(rs.getObject(1)).thenReturn("BrandA", "BrandA", null);
        when(rs.getObject(2)).thenReturn("СЗФО",   null,     null);
        when(rs.getInt(3)).thenReturn(0, 0, 1);
        when(rs.getInt(4)).thenReturn(0, 1, 1);
        when(rs.getObject(5)).thenReturn(31.34, 50.0, 100.0);

        List<NativeSqlCalc.AxisBinding> bindings = java.util.Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                mock(Hierarchy.class), "[B].[B]", "f.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                mock(Hierarchy.class), "[FD].[FD]", "f.fd", "fd", "k1"));

        Map<String, Object> result =
            NativeSqlCalc.parseResultSetWithGroupingFlags(rs, bindings);

        assertEquals(31.34, result.get("BrandA|СЗФО"));
        assertEquals(50.0,  result.get("BrandA|(all)"));
        assertEquals(100.0, result.get("(all)|(all)"));
        assertEquals(3, result.size());
    }

    @Test public void testParseResultSet_realDataNullVsAllMarker()
        throws Exception
    {
        // Two rows, both with k0_isAll=0 (no grouping subtotal):
        //   ("Brand", null, 0, 0, 100.0) → real-data NULL on k1,
        //                                  rowKey "Brand|<NUL>NULL"
        //   ("Brand", "X",  0, 0, 200.0) → specific value, rowKey "Brand|X"
        // Plus one row that IS a grouping subtotal:
        //   ("Brand", null, 0, 1, 300.0) → "Brand|(all)"
        // All three rowKeys MUST be distinct.

        java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
        when(rs.next()).thenReturn(true, true, true, false);

        when(rs.getObject(1)).thenReturn("Brand", "Brand", "Brand");
        when(rs.getObject(2)).thenReturn(null,   "X",     null);
        when(rs.getInt(3)).thenReturn(0, 0, 0);
        when(rs.getInt(4)).thenReturn(0, 0, 1);
        when(rs.getObject(5)).thenReturn(100.0, 200.0, 300.0);

        List<NativeSqlCalc.AxisBinding> bindings = java.util.Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                mock(Hierarchy.class), "[B].[B]", "f.brand", "brand", "k0"),
            new NativeSqlCalc.AxisBinding(
                mock(Hierarchy.class), "[X].[X]", "f.x", "x", "k1"));

        Map<String, Object> result =
            NativeSqlCalc.parseResultSetWithGroupingFlags(rs, bindings);

        assertEquals(100.0, result.get("Brand|" + NativeSqlCalc.NULL_KEY_MARKER));
        assertEquals(200.0, result.get("Brand|X"));
        assertEquals(300.0, result.get("Brand|(all)"));
        assertEquals(3, result.size());
        // NULL_KEY_MARKER and ALL_MEMBER_MARKER are DIFFERENT keys
        assertNotEquals(
            result.get("Brand|" + NativeSqlCalc.NULL_KEY_MARKER),
            result.get("Brand|(all)"));
    }

    // ------------------------------------------------------------------
    // Task 9: resolveSyntheticBinding (rollupAxes path)
    // ------------------------------------------------------------------

    @Test public void testResolveSyntheticBinding_factColumn_noJoin() {
        // keyExp on the fact table → returns "f.column" with no JOIN.
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        final RolapStar.Column factColumn = mock(RolapStar.Column.class);
        final MondrianDef.Column keyExp =
            new MondrianDef.Column("fact_alias", "category");
        final MondrianDef.Column expr =
            new MondrianDef.Column("fact_alias", "category");

        when(star.getFactTable()).thenReturn(factTable);
        when(star.lookupColumn("fact_alias", "category"))
            .thenReturn(factColumn);
        when(factColumn.getExpression()).thenReturn(expr);
        when(factColumn.getName()).thenReturn("category");
        when(factColumn.getTable()).thenReturn(factTable);

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[Category]");
        when(hierarchy.getLevels()).thenReturn(
            new Level[] { allLevel, dataLevel });
        when(dataLevel.getUniqueName()).thenReturn("[Category].[Category]");
        when(dataLevel.getKeyExp()).thenReturn(keyExp);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 0,
                /*candidateAggs*/ null);

        assertEquals("f.category", binding.qualifiedColumn);
        assertEquals("category", binding.columnName);
        assertEquals("k0", binding.keyAlias);
        assertEquals("[Category]", binding.hierarchyName);
        assertEquals(0, joins.size());
    }

    @Test public void testResolveSyntheticBinding_dimColumn_emitsJoin() {
        // keyExp on a dim table → "dim_alias.column" + JOIN clause registered.
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        final RolapStar.Table dimTable = mock(RolapStar.Table.class);
        final RolapStar.Column dimColumn = mock(RolapStar.Column.class);
        final RolapStar.Condition joinCond = mock(RolapStar.Condition.class);
        final MondrianDef.Column keyExp =
            new MondrianDef.Column("per", "month_fd");
        final MondrianDef.Column expr =
            new MondrianDef.Column("per", "month_fd");
        final MondrianDef.Column left =
            new MondrianDef.Column(null, "period_month");
        final MondrianDef.Column right =
            new MondrianDef.Column(null, "period_month");

        when(star.getFactTable()).thenReturn(factTable);
        when(star.lookupColumn("per", "month_fd")).thenReturn(dimColumn);
        when(dimColumn.getExpression()).thenReturn(expr);
        when(dimColumn.getName()).thenReturn("month_fd");
        when(dimColumn.getTable()).thenReturn(dimTable);
        when(dimTable.getAlias()).thenReturn("per");
        when(dimTable.getTableName()).thenReturn("dim_konfet_period");
        when(dimTable.getJoinCondition()).thenReturn(joinCond);
        when(joinCond.getLeft()).thenReturn(left);
        when(joinCond.getRight()).thenReturn(right);

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[Period]");
        when(hierarchy.getLevels()).thenReturn(
            new Level[] { allLevel, dataLevel });
        when(dataLevel.getUniqueName()).thenReturn("[Period].[Month]");
        when(dataLevel.getKeyExp()).thenReturn(keyExp);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 2,
                /*candidateAggs*/ null);

        // Per the synthetic-resolver contract (commit ad0e07ee5):
        // always returns factAlias.columnName regardless of whether the
        // level's keyExp points at a fact or dim column. Templates own
        // their own FROM/JOIN scope and an agg with the column inlined
        // is the only valid target.
        assertEquals("f.month_fd", binding.qualifiedColumn);
        assertEquals("month_fd", binding.columnName);
        assertEquals("k2", binding.keyAlias);
        assertEquals("[Period]", binding.hierarchyName);
        // No JOINs are ever emitted by the synthetic resolver.
        assertEquals(0, joins.size());
    }

    @Test public void testResolveSyntheticBinding_noNonAllLevel_throws() {
        // Hierarchy with only the All-level → no data level to resolve.
        final RolapStar star = mock(RolapStar.class);
        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        when(hierarchy.getUniqueName()).thenReturn("[Empty]");
        when(hierarchy.getLevels()).thenReturn(new Level[] { allLevel });

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        try {
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 0,
                /*candidateAggs*/ null);
            fail("expected MondrianException");
        } catch (MondrianException ex) {
            assertTrue(
                ex.getMessage().contains("[Empty]"),
                "message should mention hierarchy unique name: "
                    + ex.getMessage());
            assertTrue(
                ex.getMessage().contains("no non-All level")
                    || ex.getMessage().contains("non-All"),
                "message should mention missing non-All level: "
                    + ex.getMessage());
        }
    }

    @Test public void testResolveSyntheticBinding_resolverReturnsNull_throws() {
        // keyExp is non-resolvable (ExpressionView, no tableAlias) →
        // resolver returns null → method must throw with a clear message.
        final RolapStar star = mock(RolapStar.class);
        final MondrianDef.ExpressionView nonResolvable =
            mock(MondrianDef.ExpressionView.class);

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[Computed]");
        when(hierarchy.getLevels()).thenReturn(
            new Level[] { allLevel, dataLevel });
        when(dataLevel.getUniqueName())
            .thenReturn("[Computed].[Computed]");
        when(dataLevel.getKeyExp()).thenReturn(nonResolvable);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        try {
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 0,
                /*candidateAggs*/ null);
            fail("expected MondrianException");
        } catch (MondrianException ex) {
            assertTrue(
                ex.getMessage().contains("[Computed]"),
                "message should mention hierarchy unique name: "
                    + ex.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Task 44: synthetic-binding pre-validation against candidate aggs
    // ------------------------------------------------------------------

    @Test public void testExtractAggTableNamesFromTemplates_basic() {
        // Mixed templates referencing two distinct aggs and a CTE alias.
        // Subqueries (FROM (SELECT ...)) must NOT match.
        List<String> templates = Arrays.asList(
            "SELECT k1, sum(val) FROM agg_store_cat_brand f WHERE x = 1",
            "WITH cte AS (SELECT * FROM agg_chain_brand) "
                + "SELECT k1 FROM cte JOIN agg_chain_brand AS f ON 1=1",
            "SELECT * FROM (SELECT 1) x",
            "SELECT * FROM `agg_quoted_name`"
        );
        Set<String> names =
            NativeSqlCalc.extractAggTableNamesFromTemplates(templates);
        assertTrue(names.contains("agg_store_cat_brand"),
            "expected agg_store_cat_brand in: " + names);
        assertTrue(names.contains("agg_chain_brand"),
            "expected agg_chain_brand in: " + names);
        assertTrue(names.contains("cte"),
            "CTE alias is captured (filtered out by aggStar lookup): "
                + names);
        assertTrue(names.contains("agg_quoted_name"),
            "back-quoted table name should be unquoted: " + names);
    }

    @Test public void testExtractAggTableNamesFromTemplates_emptyOrNull() {
        // Null and empty inputs must produce an empty set, not NPE.
        assertTrue(
            NativeSqlCalc.extractAggTableNamesFromTemplates(null).isEmpty(),
            "null templates → empty set");
        assertTrue(
            NativeSqlCalc.extractAggTableNamesFromTemplates(
                Collections.<String>emptyList()).isEmpty(),
            "empty list → empty set");
    }

    @Test
    public void testResolveSyntheticBinding_columnAbsentOnAggs_returnsNull() {
        // Task 44 pre-validation: when candidateAggs is non-empty AND none
        // carry the synthetic level's column, the resolver must return null
        // so the caller can short-circuit (no SQL fired against any agg).
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(star.getFactTable()).thenReturn(factTable);

        // Build an AggStar whose fact-table columns DO NOT include the
        // hierarchy's keyExp column ("region"). Mock only the surface
        // touched by anyAggHasColumn: getFactTable().getColumns().getName().
        final mondrian.rolap.aggmatcher.AggStar agg =
            mock(mondrian.rolap.aggmatcher.AggStar.class);
        @SuppressWarnings("unchecked")
        final mondrian.rolap.aggmatcher.AggStar.FactTable aggFact =
            mock(mondrian.rolap.aggmatcher.AggStar.FactTable.class);
        final mondrian.rolap.aggmatcher.AggStar.Table.Column otherCol =
            mock(mondrian.rolap.aggmatcher.AggStar.Table.Column.class);
        when(agg.getFactTable()).thenReturn(aggFact);
        when(aggFact.getName()).thenReturn("agg_no_region");
        when(aggFact.getColumns()).thenReturn(
            new ArrayList<mondrian.rolap.aggmatcher.AggStar.Table.Column>(
                Arrays.asList(otherCol)));
        when(otherCol.getName()).thenReturn("brand");

        final Set<mondrian.rolap.aggmatcher.AggStar> candidates =
            new LinkedHashSet<mondrian.rolap.aggmatcher.AggStar>(
                Arrays.asList(agg));

        final MondrianDef.Column keyExp =
            new MondrianDef.Column("fact_alias", "region");

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[Region]");
        when(hierarchy.getLevels()).thenReturn(
            new Level[] { allLevel, dataLevel });
        when(dataLevel.getUniqueName()).thenReturn("[Region].[Region]");
        when(dataLevel.getKeyExp()).thenReturn(keyExp);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 0, candidates);

        assertNull(binding,
            "binding should be null when no candidate agg carries 'region'");
        assertEquals(0, joins.size(),
            "no JOINs should be registered on a null-binding return");
    }

    @Test
    public void testResolveSyntheticBinding_columnPresentOnAgg_succeeds() {
        // Task 44 pre-validation: when at least one candidate agg has the
        // column, the binding succeeds and remains byte-identical to the
        // legacy contract — f.<columnName>, no JOINs.
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(star.getFactTable()).thenReturn(factTable);

        final mondrian.rolap.aggmatcher.AggStar agg =
            mock(mondrian.rolap.aggmatcher.AggStar.class);
        final mondrian.rolap.aggmatcher.AggStar.FactTable aggFact =
            mock(mondrian.rolap.aggmatcher.AggStar.FactTable.class);
        final mondrian.rolap.aggmatcher.AggStar.Table.Column regionCol =
            mock(mondrian.rolap.aggmatcher.AggStar.Table.Column.class);
        when(agg.getFactTable()).thenReturn(aggFact);
        when(aggFact.getName()).thenReturn("agg_store_cat_brand");
        when(aggFact.getColumns()).thenReturn(
            new ArrayList<mondrian.rolap.aggmatcher.AggStar.Table.Column>(
                Arrays.asList(regionCol)));
        when(regionCol.getName()).thenReturn("region");

        final Set<mondrian.rolap.aggmatcher.AggStar> candidates =
            new LinkedHashSet<mondrian.rolap.aggmatcher.AggStar>(
                Arrays.asList(agg));

        final MondrianDef.Column keyExp =
            new MondrianDef.Column("fact_alias", "region");

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[Region]");
        when(hierarchy.getLevels()).thenReturn(
            new Level[] { allLevel, dataLevel });
        when(dataLevel.getUniqueName()).thenReturn("[Region].[Region]");
        when(dataLevel.getKeyExp()).thenReturn(keyExp);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 0, candidates);

        assertNotNull(binding,
            "binding must succeed when an agg carries the column");
        assertEquals("f.region", binding.qualifiedColumn,
            "successful binding stays byte-identical with legacy contract");
        assertEquals("region", binding.columnName);
        assertEquals("k0", binding.keyAlias);
        assertEquals(0, joins.size());
    }

    @Test
    public void testResolveSyntheticBinding_emptyCandidates_legacyBehavior() {
        // When candidateAggs is empty (no FROM matched the agg-matcher set
        // — e.g. the template uses the literal fact table or a CTE alias),
        // pre-validation is skipped and the resolver falls back to the
        // legacy "always succeed" contract. Preserves existing behavior
        // for queries that don't go through agg routing.
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(star.getFactTable()).thenReturn(factTable);

        final MondrianDef.Column keyExp =
            new MondrianDef.Column("fact_alias", "category");

        final Hierarchy hierarchy = mock(Hierarchy.class);
        final Level allLevel = mock(Level.class);
        final RolapLevel dataLevel = mock(RolapLevel.class);
        when(hierarchy.getUniqueName()).thenReturn("[Category]");
        when(hierarchy.getLevels()).thenReturn(
            new Level[] { allLevel, dataLevel });
        when(dataLevel.getUniqueName()).thenReturn("[Category].[Category]");
        when(dataLevel.getKeyExp()).thenReturn(keyExp);

        final List<String> joins = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        final NativeSqlCalc.AxisBinding binding =
            NativeSqlCalc.resolveSyntheticBinding(
                hierarchy, star, "f", joins, seenJoins, 0,
                Collections.<mondrian.rolap.aggmatcher.AggStar>emptySet());

        assertNotNull(binding, "empty candidates set → legacy behavior");
        assertEquals("f.category", binding.qualifiedColumn);
    }

    @Test public void testExtractTableNamesForAlias_filtersAlias() {
        final String sql =
            "WITH p AS (SELECT f.city FROM default.agg_brand_store f) "
            + "SELECT * FROM p pr JOIN `agg_store` AS g ON 1 = 1";

        final Set<String> fTables =
            NativeSqlCalc.extractTableNamesForAlias(sql, "f");
        final Set<String> gTables =
            NativeSqlCalc.extractTableNamesForAlias(sql, "g");

        assertEquals(
            new LinkedHashSet<String>(Arrays.asList("agg_brand_store")),
            fTables);
        assertEquals(
            new LinkedHashSet<String>(Arrays.asList("agg_store")),
            gTables);
    }

    @Test public void testCollectRequiredTemplateColumns_intersectsRenderedSql() {
        final String sql =
            "SELECT f.city AS k0, any(f.store_period_total) AS spt "
            + "FROM agg_brand_store f WHERE f.brand = 'A'";
        final List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Город", "f.city", "city", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Категория", "f.category", "category", "k1"));
        final List<NativeSqlCalc.PredicateInfo> predicates = Arrays.asList(
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "f.brand = 'A'"),
            new NativeSqlCalc.AtomicPredicateInfo(
                "ТТ", "Адрес", "f.address = 'X'"));

        final Set<String> required =
            NativeSqlCalc.collectRequiredTemplateColumns(
                sql, bindings, predicates);

        assertEquals(
            new LinkedHashSet<String>(Arrays.asList("city", "brand")),
            required);
    }

    @Test public void testShouldSkipTemplateForMissingDbColumns_missingAxis()
        throws Exception
    {
        final DataSource dataSource =
            mockColumnDataSource("agg_brand_store", "brand");
        final String sql =
            "SELECT f.city AS k0 FROM agg_brand_store f "
            + "WHERE f.brand = 'A'";
        final List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Город", "f.city", "city", "k0"));
        final List<NativeSqlCalc.PredicateInfo> predicates = Arrays.asList(
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "f.brand = 'A'"));

        assertTrue(
            NativeSqlCalc.shouldSkipTemplateForMissingDbColumns(
                sql, bindings, predicates, dataSource));
    }

    @Test public void testShouldSkipTemplateForMissingDbColumns_presentAxis()
        throws Exception
    {
        final DataSource dataSource =
            mockColumnDataSource("agg_store", "brand", "city");
        final String sql =
            "SELECT f.city AS k0 FROM agg_store f WHERE f.brand = 'A'";
        final List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Город", "f.city", "city", "k0"));
        final List<NativeSqlCalc.PredicateInfo> predicates = Arrays.asList(
            new NativeSqlCalc.AtomicPredicateInfo(
                "Продукт", "Бренд", "f.brand = 'A'"));

        assertFalse(
            NativeSqlCalc.shouldSkipTemplateForMissingDbColumns(
                sql, bindings, predicates, dataSource));
    }

    // ------------------------------------------------------------------
    // Task 11: cube macro renderers
    // ------------------------------------------------------------------

    @Test public void testRenderAxisCubeSelectFlags_emitsGroupingProjections() {
        final List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Категория", "f.category", "category", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Производитель", "f.manufacturer",
                "manufacturer", "k1"));

        // Trailing-comma idiom — mirrors renderAxisResultSelectList so it
        // can sit between axisResultSelectList and the next SELECT-list
        // expression without comma adjacency issues.
        assertEquals(
            "  GROUPING(pr.k0) AS k0_isAll,\n"
                + "  GROUPING(pr.k1) AS k1_isAll,\n",
            NativeSqlCalc.renderAxisCubeSelectFlags(bindings, "pr"));
    }

    @Test public void testRenderAxisCubeSelectFlags_emptyBindings_returnsEmpty() {
        assertEquals(
            "",
            NativeSqlCalc.renderAxisCubeSelectFlags(
                Collections.<NativeSqlCalc.AxisBinding>emptyList(), "pr"));
    }

    @Test public void testRenderAxisGroupByListCube_emitsCubeSyntax() {
        final List<NativeSqlCalc.AxisBinding> bindings = Arrays.asList(
            new NativeSqlCalc.AxisBinding(
                null, "Категория", "f.category", "category", "k0"),
            new NativeSqlCalc.AxisBinding(
                null, "Производитель", "f.manufacturer",
                "manufacturer", "k1"));

        // Form C: bare CUBE(...) — no trailing space, no tuple() anchor.
        assertEquals(
            "CUBE(pr.k0, pr.k1)",
            NativeSqlCalc.renderAxisGroupByListCube(bindings, "pr"));
    }

    @Test public void testRenderAxisGroupByListCube_emptyBindings_returnsEmpty() {
        assertEquals(
            "",
            NativeSqlCalc.renderAxisGroupByListCube(
                Collections.<NativeSqlCalc.AxisBinding>emptyList(), "pr"));
    }

    @Test public void testShouldFallbackForAxisCap() {
        NativeSqlConfig.NativeSqlDef rollupDef =
            mock(NativeSqlConfig.NativeSqlDef.class);
        when(rollupDef.isRollupAxes()).thenReturn(true);
        NativeSqlConfig.NativeSqlDef nonRollupDef =
            mock(NativeSqlConfig.NativeSqlDef.class);
        when(nonRollupDef.isRollupAxes()).thenReturn(false);

        assertFalse(NativeSqlCalc.shouldFallbackForAxisCap(rollupDef, 0));
        assertFalse(NativeSqlCalc.shouldFallbackForAxisCap(rollupDef, 3));
        assertTrue(NativeSqlCalc.shouldFallbackForAxisCap(rollupDef, 4));
        assertTrue(NativeSqlCalc.shouldFallbackForAxisCap(rollupDef, 10));
        assertFalse(NativeSqlCalc.shouldFallbackForAxisCap(nonRollupDef, 10));
    }

    private static AggStar mockAgg(String name, String... columns) {
        final AggStar agg = mock(AggStar.class);
        final AggStar.FactTable fact = mock(AggStar.FactTable.class);
        final List<AggStar.Table.Column> aggColumns =
            new ArrayList<AggStar.Table.Column>();
        for (String column : columns) {
            final AggStar.Table.Column aggColumn =
                mock(AggStar.Table.Column.class);
            when(aggColumn.getName()).thenReturn(column);
            aggColumns.add(aggColumn);
        }
        when(agg.getFactTable()).thenReturn(fact);
        when(fact.getName()).thenReturn(name);
        when(fact.getColumns()).thenReturn(aggColumns);
        return agg;
    }

    private static DataSource mockColumnDataSource(
        String tableName,
        String... columns)
        throws Exception
    {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getColumns(null, null, tableName, null))
            .thenReturn(resultSet);

        final Boolean[] nextResults = new Boolean[columns.length + 1];
        Arrays.fill(nextResults, Boolean.TRUE);
        nextResults[nextResults.length - 1] = Boolean.FALSE;
        when(resultSet.next()).thenReturn(
            nextResults[0],
            Arrays.copyOfRange(nextResults, 1, nextResults.length));
        if (columns.length > 0) {
            when(resultSet.getString("COLUMN_NAME")).thenReturn(
                columns[0],
                Arrays.copyOfRange(columns, 1, columns.length));
        }
        return dataSource;
    }

}
