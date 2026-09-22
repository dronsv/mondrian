package mondrian.rolap;

import java.util.List;
import java.util.Set;

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.ResourceLimitExceededException;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.DrilldownLevelCrossJoinArg;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IndependentTargetSplitTest {
    private static MondrianDef.Table table(String name) {
        return new MondrianDef.Table(null, name, name, new MondrianDef.Hint[0]);
    }

    private static CrossJoinArg arg(String name, MondrianDef.RelationOrJoin relation) {
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        when(hierarchy.getRelation()).thenReturn(relation);
        RolapLevel level = mock(RolapLevel.class);
        when(level.getHierarchy()).thenReturn(hierarchy);
        when(level.getUniqueName()).thenReturn(name);
        return new DescendantsCrossJoinArg(level, null);
    }

    private static IndependentTargetSplit plan(CrossJoinArg... args) {
        return IndependentTargetSplit.plan(args, args);
    }

    @Test void sharedTableUsesOneGroup() {
        assertNull(plan(arg("Brand", table("product")), arg("Product", table("product"))));
    }

    @Test void separateRelationsAndAdjacentSharedHierarchyKeepTheirPositions() {
        IndependentTargetSplit plan = plan(arg("Store", table("store")),
            arg("Brand", table("product")), arg("Product", table("product")));
        assertEquals(List.of(List.of(0), List.of(1, 2)), plan.indexes);
    }

    @Test void interleavedRelationGroupKeepsLegacyOrder() {
        assertNull(plan(arg("Brand", table("product")), arg("Store", table("store")),
            arg("Product", table("product"))));
    }

    @Test void nullRelationAndAllLevelDecline() {
        assertNull(plan(arg("Store", table("store")), arg("Unknown", null)));
        CrossJoinArg all = arg("All", table("product"));
        when(all.getLevel().isAll()).thenReturn(true);
        assertNull(plan(arg("Store", table("store")), all));
    }

    @Test void snowflakeAndBridgingConstraintCannotBecomeIndependentGroups() {
        MondrianDef.Join join = new MondrianDef.Join();
        join.left = table("product");
        join.right = table("brand");
        assertNull(plan(arg("Product", join), arg("Brand", table("brand"))));
        CrossJoinArg[] targets = {arg("Product", table("product")), arg("Brand", table("brand"))};
        assertNull(IndependentTargetSplit.plan(targets, new CrossJoinArg[] {arg("Bridge", join)}));
    }

    @Test void foreignMemberlessArgumentDoesNotAddATargetGroup() {
        CrossJoinArg[] targets = {arg("Product", table("product")), arg("Store", table("store"))};
        IndependentTargetSplit plan = IndependentTargetSplit.plan(targets,
            new CrossJoinArg[] {targets[0], targets[1], arg("Week", table("calendar"))});
        assertEquals(2, plan.indexes.size());
    }

    @Test void onlyMemberlessArgumentsAndAllDrillsAreEligible() {
        CrossJoinArg leaf = arg("Store", table("store"));
        RolapMember all = mock(RolapMember.class);
        when(all.isAll()).thenReturn(true);
        assertTrue(IndependentTargetSplit.eligible(new DrilldownLevelCrossJoinArg(leaf, all)));
        assertFalse(IndependentTargetSplit.eligible(new DrilldownLevelCrossJoinArg(leaf, mock(RolapMember.class))));
        CrossJoinArg children = new DescendantsCrossJoinArg(leaf.getLevel(), all);
        assertFalse(IndependentTargetSplit.eligible(children));
        assertFalse(IndependentTargetSplit.eligible(new DrilldownLevelCrossJoinArg(children, all)));
        assertFalse(IndependentTargetSplit.eligible(mock(CrossJoinArg.class)));
    }

    private static StarPredicate predicate(MondrianDef.Table table, int bit) {
        RolapStar.Table starTable = mock(RolapStar.Table.class);
        when(starTable.getRelation()).thenReturn(table);
        RolapStar.Column column = mock(RolapStar.Column.class);
        when(column.getTable()).thenReturn(starTable);
        when(column.getBitPosition()).thenReturn(bit);
        return new ValueColumnPredicate(column, "selection");
    }

    @Test void nestedAndIsSeparableButCrossGroupOrIsNot() {
        MondrianDef.Table store = table("store"), product = table("product");
        List<Set<MondrianDef.Relation>> groups = List.of(Set.of(store), Set.of(product));
        StarPredicate s = predicate(store, 0), p = predicate(product, 1);
        assertTrue(SqlDimensionContextConstraint.isSeparable(
            new AndPredicate(List.of(new AndPredicate(List.of(s)), p)), groups));
        assertFalse(SqlDimensionContextConstraint.isSeparable(new OrPredicate(List.of(s, p)), groups));
        assertFalse(SqlDimensionContextConstraint.isSeparable(predicate(table("foreign"), 2), groups));
    }

    @Test void projectionCannotLoseAHierarchySpanningGroups() {
        MondrianDef.Table store = table("store"), product = table("product");
        MondrianDef.Join join = new MondrianDef.Join();
        join.left = store;
        join.right = product;
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        when(hierarchy.getRelation()).thenReturn(join);
        mondrian.olap.Dimension dimension = mock(mondrian.olap.Dimension.class);
        when(dimension.getHierarchies()).thenReturn(new mondrian.olap.Hierarchy[] {hierarchy});
        RolapCube cube = mock(RolapCube.class);
        when(cube.getDimensions()).thenReturn(new mondrian.olap.Dimension[] {dimension});
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        when(evaluator.getCube()).thenReturn(cube);
        when(evaluator.getMembers()).thenReturn(new RolapMember[0]);
        mondrian.olap.Query query = mock(mondrian.olap.Query.class);
        when(evaluator.getQuery()).thenReturn(query);
        when(evaluator.getSchemaReader()).thenReturn(mock(mondrian.olap.SchemaReader.class));
        // Even a predicate on only one column of this hierarchy is omitted by
        // each group's addAvailableContext: neither contains the full join.
        assertFalse(SqlDimensionContextConstraint.isSeparable(evaluator, List.of(Set.of(store), Set.of(product))));
    }

    @Test void productGuardIsExactOverflowSafeAndZeroDominates() {
        IndependentTargetSplit plan = plan(arg("Store", table("store")), arg("Product", table("product")));
        MondrianProperties props = MondrianProperties.instance();
        int cap = props.CrossJoinFactlessSplitMaxCandidates.get(), limit = props.ResultLimit.get();
        try {
            props.CrossJoinFactlessSplitMaxCandidates.set(35);
            props.ResultLimit.set(0);
            assertEquals(35, plan.checkSize(List.of(5L, 7L)));
            assertThrows(ResourceLimitExceededException.class, () -> plan.checkSize(List.of(5L, 8L)));
            assertThrows(ResourceLimitExceededException.class, () -> plan.checkSize(List.of(Long.MAX_VALUE, 2L)));
            assertEquals(0, plan.checkSize(List.of(Long.MAX_VALUE, 2L, 0L)));
            props.ResultLimit.set(30);
            assertThrows(ResourceLimitExceededException.class, () -> plan.checkSize(List.of(5L, 7L)));
            props.CrossJoinFactlessSplitMaxCandidates.set(0);
            assertThrows(ResourceLimitExceededException.class, () -> plan.checkSize(List.of(5L, 7L)));
        } finally {
            props.CrossJoinFactlessSplitMaxCandidates.set(cap);
            props.ResultLimit.set(limit);
        }
    }
}
