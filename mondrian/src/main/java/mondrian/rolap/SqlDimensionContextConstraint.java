package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import mondrian.calc.TupleList;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
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
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

/** Member navigation over a dimension, never over populated fact cells. */
class SqlDimensionContextConstraint extends DefaultMemberChildrenConstraint
    implements TupleConstraint
{
    private final RolapCube cube;
    private final RolapCube bindingCube;
    private final StarPredicate contextPredicate;
    private final List<Object> cacheKey;

    /** Restricts by every hierarchy of the dimension, including its current members. */
    SqlDimensionContextConstraint(RolapEvaluator evaluator, Dimension dimension) {
        this(evaluator, dimension, null);
    }

    /**
     * Navigation from an explicit member of {@code anchored}: that member, not
     * the evaluator's current member of the same hierarchy, positions the
     * result (e.g. {@code CurrentMember.PrevMember.LastChild}). Slicer sets,
     * subcubes and role limits on {@code anchored} still apply.
     */
    SqlDimensionContextConstraint(RolapEvaluator evaluator, Dimension dimension, Hierarchy anchored) {
        this.cube = evaluator.getCube();
        Set<Hierarchy> included = new LinkedHashSet<>(Arrays.asList(dimension.getHierarchies()));
        bindingCube = resolveBindingCube(cube, included);
        // An unsupported calculated context member (e.g. "AS 1") is not a
        // member set: leave its hierarchy unrestricted instead of failing.
        contextPredicate = contextPredicate(evaluator, bindingCube, included, anchored, false);
        cacheKey = List.of(getClass(), cube, bindingCube, dimension, evaluator.getSchemaReader().getRole(),
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
                // Establish the projection from the member query, not from a
                // nullable virtual star key. A missing physical binding must
                // fail closed rather than make a restriction disappear.
                if (isAvailable(query, cube, (RolapHierarchy) hierarchy)) {
                    included.add(hierarchy);
                }
            }
        }
        if (!included.isEmpty()) {
            RolapCube bindingCube = resolveBindingCube(cube, included);
            addPredicate(query, contextPredicate(evaluator, bindingCube, included, null, strict));
        }
    }

    /** Use one physical star for every predicate in the projected context. */
    private static RolapCube resolveBindingCube(RolapCube cube, Set<Hierarchy> included) {
        if (!cube.isVirtual()) {
            return cube;
        }
        for (RolapCube candidate : cube.getBaseCubes()) {
            boolean compatible = true;
            for (Hierarchy hierarchy : included) {
                RolapCubeHierarchy virtualHierarchy = (RolapCubeHierarchy) hierarchy;
                for (Level level : hierarchy.getLevels()) {
                    if (level.isAll()) {
                        continue;
                    }
                    RolapCubeLevel baseLevel = candidate.findBaseCubeLevel((RolapLevel) level);
                    // Matching by name alone is insufficient: private dimensions
                    // in different base cubes may use unrelated relations/keys.
                    // Alias remapping also needs an explicit implementation; do
                    // not render a predicate against another relation by guess.
                    if (baseLevel == null
                        || baseLevel.getStarKeyColumn() == null
                        || !baseLevel.getHierarchy().getRelation().equals(virtualHierarchy.getRelation())
                        || !baseLevel.getKeyExp().equals(((RolapLevel) level).getKeyExp()))
                    {
                        compatible = false;
                        break;
                    }
                }
                if (!compatible) {
                    break;
                }
            }
            if (compatible) {
                return candidate;
            }
        }
        throw Util.newError("No coherent physical dimension binding for " + included
            + " in virtual cube " + cube.getName());
    }

    private static boolean isAvailable(SqlQuery query, RolapCube cube, RolapHierarchy hierarchy) {
        if (containsRelation(query, hierarchy.getRelation())) {
            return true;
        }
        // Virtual native member queries may already use a base hierarchy's
        // aliased relation. Include it so an unsupported mapping is rejected,
        // rather than silently omitting that hierarchy's restrictions.
        if (cube.isVirtual()) {
            for (RolapCube candidate : cube.getBaseCubes()) {
                RolapHierarchy base = candidate.findBaseCubeHierarchy(hierarchy);
                if (base != null && containsRelation(query, base.getRelation())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean containsRelation(SqlQuery query, MondrianDef.RelationOrJoin relation) {
        if (relation instanceof MondrianDef.Join join) {
            return containsRelation(query, join.left) && containsRelation(query, join.right);
        }
        return relation instanceof MondrianDef.Relation
            && query.containsRelation((MondrianDef.Relation) relation);
    }

    private static StarPredicate contextPredicate(
        RolapEvaluator evaluator, RolapCube bindingCube, Set<Hierarchy> included, Hierarchy anchored,
        boolean strict)
    {
        RolapCube cube = evaluator.getCube();
        List<StarPredicate> predicates = new ArrayList<>();
        for (Member member : evaluator.getMembers()) {
            if (included.contains(member.getHierarchy())
                && !member.getHierarchy().equals(anchored)
                && !(member instanceof RolapResult.CompoundSlicerRolapMember))
            {
                predicates.add(contextMemberPredicate(evaluator, bindingCube, member, strict));
            }
        }
        TupleList tuples = evaluator.getOptimizedSlicerTuples(bindingCube);
        if (tuples != null && !tuples.isEmpty()) {
            List<StarPredicate> alternatives = new ArrayList<>();
            for (List<Member> tuple : tuples) {
                List<StarPredicate> conjunction = new ArrayList<>();
                for (Member member : tuple) {
                    if (included.contains(member.getHierarchy())) {
                        conjunction.add(contextMemberPredicate(evaluator, bindingCube, member, strict));
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
        StarPredicate subcube = evaluator.getQuery().getSubcubePredicates(bindingCube, ignored, evaluator);
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
                    alternatives.add(memberPredicate(bindingCube, member, strict));
                }
                predicates.add(alternatives.isEmpty()
                    ? new LiteralStarPredicate(null, false) : new OrPredicate(alternatives));
            }
        }
        return and(predicates);
    }

    private static StarPredicate contextMemberPredicate(
        RolapEvaluator evaluator, RolapCube cube, Member member, boolean strict)
    {
        if (member.isCalculated() && SqlConstraintUtils.isSupportedCalculatedMember(member)) {
            TupleConstraintStruct expanded = new TupleConstraintStruct();
            SqlConstraintUtils.expandSupportedCalculatedMember(member, evaluator, expanded);
            // A unary aggregate is a union. Flattening multi-hierarchy sets
            // would lose their tuple correlations; keep those unsupported.
            if (expanded.getDisjoinedTupleLists().isEmpty()
                && expanded.getMembers().stream().allMatch(value -> !value.isCalculated()
                    && value.getHierarchy().equals(member.getHierarchy())))
            {
                List<StarPredicate> alternatives = expanded.getMembers().stream()
                    .map(value -> memberPredicate(cube, value, strict)).toList();
                return alternatives.isEmpty() ? new LiteralStarPredicate(null, false)
                    : new OrPredicate(alternatives);
            }
        }
        return memberPredicate(cube, member, strict);
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
            alternatives.add(memberPredicate(bindingCube, parent, true));
        }
        addPredicate(query, new OrPredicate(alternatives));
        addPredicate(query, contextPredicate);
    }

    @Override
    public void addConstraint(SqlQuery query, RolapCube baseCube, AggStar aggStar) {
        addPredicate(query, contextPredicate);
    }

    @Override
    public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
        return this;
    }

    @Override
    public Evaluator getEvaluator() {
        // The immutable predicate captures context; fact-based virtual tuple
        // grouping must not select a different physical binding here.
        return null;
    }

    @Override
    public boolean supportsAggTables() {
        return false;
    }

    @Override
    public Object getCacheKey() {
        return cacheKey;
    }
}
