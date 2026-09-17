package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import mondrian.calc.TupleList;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Level;
import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.LiteralStarPredicate;
import mondrian.rolap.agg.MemberColumnPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.PredicateCanonicalizer;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

/** Member navigation over a dimension, never over populated fact cells. */
class SqlDimensionContextConstraint extends DefaultMemberChildrenConstraint {
    private final RolapCube cube;
    private final Dimension dimension;
    private final StarPredicate contextPredicate;
    private final List<Object> cacheKey;

    SqlDimensionContextConstraint(RolapEvaluator evaluator, Dimension dimension) {
        this.cube = evaluator.getCube();
        this.dimension = dimension;
        Set<Hierarchy> included = new LinkedHashSet<>(Arrays.asList(dimension.getHierarchies()));
        contextPredicate = contextPredicate(evaluator, included, true);
        cacheKey = List.of(getClass(), cube, dimension, evaluator.getSchemaReader().getRole(),
            PredicateCanonicalizer.canonicalize(contextPredicate));
    }

    /** Project context onto dimension relations already used by a member query. */
    static void addAvailableContext(SqlQuery query, RolapEvaluator evaluator, boolean strict) {
        RolapCube cube = evaluator.getCube();
        Set<Hierarchy> included = new LinkedHashSet<>();
        for (Dimension dimension : cube.getDimensions()) {
            if (dimension.isMeasures()) {
                continue;
            }
            for (Hierarchy hierarchy : dimension.getHierarchies()) {
                boolean available = true;
                for (Level level : hierarchy.getLevels()) {
                    if (level.isAll()) {
                        continue;
                    }
                    RolapStar.Column column = ((RolapCubeLevel) level).getBaseStarKeyColumn(cube);
                    if (column == null || !query.containsRelation(column.getTable().getRelation())) {
                        available = false;
                        break;
                    }
                }
                if (available) {
                    included.add(hierarchy);
                }
            }
        }
        addPredicate(query, contextPredicate(evaluator, included, strict));
    }

    private static StarPredicate contextPredicate(
        RolapEvaluator evaluator, Set<Hierarchy> included, boolean strict)
    {
        RolapCube cube = evaluator.getCube();
        List<StarPredicate> predicates = new ArrayList<>();
        for (Member member : evaluator.getMembers()) {
            if (included.contains(member.getHierarchy())
                && !(member instanceof RolapResult.CompoundSlicerRolapMember))
            {
                predicates.add(memberPredicate(cube, member, strict));
            }
        }
        TupleList tuples = evaluator.getOptimizedSlicerTuples(cube);
        if (tuples != null && !tuples.isEmpty()) {
            List<StarPredicate> alternatives = new ArrayList<>();
            for (List<Member> tuple : tuples) {
                List<StarPredicate> conjunction = new ArrayList<>();
                for (Member member : tuple) {
                    if (included.contains(member.getHierarchy())) {
                        conjunction.add(memberPredicate(cube, member, strict));
                    }
                }
                alternatives.add(and(conjunction));
            }
            predicates.add(new OrPredicate(alternatives));
        }
        Set<Hierarchy> ignored = new LinkedHashSet<>();
        for (Dimension dimension : cube.getDimensions()) {
            for (Hierarchy hierarchy : dimension.getHierarchies()) {
                if (!included.contains(hierarchy)) {
                    ignored.add(hierarchy);
                }
            }
        }
        StarPredicate subcube = evaluator.getQuery().getSubcubePredicates(cube, ignored, evaluator);
        if (subcube != null) {
            predicates.add(subcube);
        }
        // Preserve applicable role limits without introducing a fact join.
        for (var entry : SqlConstraintUtils.getRoleConstraintMembers(
            evaluator.getSchemaReader(), evaluator.getMembers()).entrySet())
        {
            if (included.contains(entry.getKey().getHierarchy())) {
                List<StarPredicate> alternatives = new ArrayList<>();
                for (RolapMember member : entry.getValue()) {
                    alternatives.add(memberPredicate(cube, member, strict));
                }
                predicates.add(alternatives.isEmpty()
                    ? new LiteralStarPredicate(null, false) : new OrPredicate(alternatives));
            }
        }
        return and(predicates);
    }

    private static StarPredicate memberPredicate(RolapCube cube, Member member, boolean strict) {
        if (member.isNull()) {
            return new LiteralStarPredicate(null, false);
        }
        if (member.isCalculated()) {
            if (!strict) {
                return new LiteralStarPredicate(null, true);
            }
            throw Util.newError("Cannot navigate dimension context of calculated member "
                + member.getUniqueName());
        }
        List<StarPredicate> predicates = new ArrayList<>();
        for (Member current = member; current != null && !current.isAll(); current = current.getParentMember()) {
            RolapCubeLevel level = (RolapCubeLevel) current.getLevel();
            RolapStar.Column column = level.getBaseStarKeyColumn(cube);
            if (column == null) {
                throw Util.newError("No dimension key for " + current.getUniqueName());
            }
            predicates.add(new MemberColumnPredicate(column, (RolapMember) current));
            if (level.isUnique()) {
                break;
            }
        }
        return and(predicates);
    }

    private static StarPredicate and(List<StarPredicate> predicates) {
        return predicates.isEmpty() ? new LiteralStarPredicate(null, true) : new AndPredicate(predicates);
    }

    private static void addPredicate(SqlQuery query, StarPredicate predicate) {
        for (RolapStar.Column column : predicate.getConstrainedColumnList()) {
            if (!query.containsRelation(column.getTable().getRelation())) {
                throw Util.newError("Dimension context requires an unavailable relation: "
                    + column.getTable().getAlias());
            }
        }
        StringBuilder where = new StringBuilder();
        predicate.toSql(query, where);
        query.addWhere(where.toString());
    }

    @Override
    public void addMemberConstraint(SqlQuery query, RolapCube baseCube, AggStar aggStar, RolapMember parent) {
        addMemberConstraint(query, baseCube, aggStar, List.of(parent));
    }

    @Override
    public void addMemberConstraint(SqlQuery query, RolapCube baseCube, AggStar aggStar, List<RolapMember> parents) {
        if (parents.isEmpty()) {
            query.addWhere("1 = 0");
            return;
        }
        parents.get(0).getHierarchy().addToFrom(query, (MondrianDef.Expression) null);
        List<StarPredicate> alternatives = new ArrayList<>();
        for (RolapMember parent : parents) {
            alternatives.add(memberPredicate(cube, parent, true));
        }
        addPredicate(query, new OrPredicate(alternatives));
        addPredicate(query, contextPredicate);
    }

    @Override
    public Object getCacheKey() {
        return cacheKey;
    }
}
