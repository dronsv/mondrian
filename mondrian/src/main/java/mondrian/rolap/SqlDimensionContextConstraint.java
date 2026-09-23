package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import mondrian.calc.Calc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Level;
import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.olap.fun.ParenthesesFunDef;
import mondrian.olap.type.SetType;
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
    /** Length of the canonical predicate text, which grows with the predicate tree: what a kept constraint pins. */
    private final int weight;

    /** Constraints kept per execution before the memo starts over; not final so that a test can shrink it. */
    static int memoCapacity = 1024;
    /**
     * Their total weight before the memo starts over: the count alone does not
     * bound a context that lists thousands of slicer, subselect or role
     * members in every constraint. Not final so that a test can shrink it.
     */
    static int memoWeightCapacity = 4 << 20;

    /**
     * Restricts by every hierarchy of the dimension, including its current
     * members. Navigation from an explicit member of {@code anchored} (null if
     * none): that member, not the evaluator's current member of the same
     * hierarchy, positions the result (e.g.
     * {@code CurrentMember.PrevMember.LastChild}). Slicer sets, subcubes and
     * role limits on {@code anchored} still apply.
     *
     * <p>Built once per execution and distinct context (#97): navigation asks
     * per evaluated tuple, and a build costs far more than the member cache
     * lookup it keys.
     */
    static SqlDimensionContextConstraint of(RolapEvaluator evaluator, Dimension dimension, Hierarchy anchored) {
        MemoKey key = MemoKey.of(evaluator, dimension, anchored);
        if (key == null) {
            return new SqlDimensionContextConstraint(evaluator, dimension, anchored);
        }
        RolapEvaluatorRoot root = evaluator.root;
        SqlDimensionContextConstraint constraint = root.dimensionContextConstraints.get(key);
        if (constraint == null) {
            long subcubeTicks = root.query.getSubcubeContextDependentTicks();
            int expansions = root.dimensionContextPerCellExpansions;
            constraint = new SqlDimensionContextConstraint(evaluator, dimension, anchored);
            // A subselect set, or a calculated slicer member whose own set
            // reads the cell, evaluated on the way makes the constraint a
            // function of the whole cell context, not of the key. Nested
            // builds may have used the memo meanwhile.
            if (subcubeTicks == root.query.getSubcubeContextDependentTicks()
                && expansions == root.dimensionContextPerCellExpansions)
            {
                // Always kept, if need be alone: the heaviest contexts are the costliest to rebuild per tuple.
                if (root.dimensionContextConstraints.size() >= memoCapacity
                    || root.dimensionContextConstraintWeight + constraint.weight > memoWeightCapacity)
                {
                    root.clearDimensionContextConstraints();
                }
                root.dimensionContextConstraints.put(key, constraint);
                root.dimensionContextConstraintWeight += constraint.weight;
            }
        }
        return constraint;
    }

    /** Use {@link #of}; every call here is a real, counted build. */
    private SqlDimensionContextConstraint(RolapEvaluator evaluator, Dimension dimension, Hierarchy anchored) {
        evaluator.root.dimensionContextConstraintBuilds++;
        this.cube = evaluator.getCube();
        Set<Hierarchy> included = new LinkedHashSet<>(Arrays.asList(dimension.getHierarchies()));
        bindingCube = resolveBindingCube(cube, included);
        // An unsupported calculated context member (e.g. "AS 1") is not a
        // member set: leave its hierarchy unrestricted instead of failing.
        contextPredicate = contextPredicate(evaluator, bindingCube, included, anchored, false);
        String canonical = PredicateCanonicalizer.canonicalize(contextPredicate);
        weight = canonical.length();
        cacheKey = List.of(getClass(), cube, bindingCube, dimension, evaluator.getSchemaReader().getRole(), canonical);
    }

    /** Project context onto dimension relations already used by a member query. */
    static void addAvailableContext(SqlQuery query, RolapEvaluator evaluator, boolean strict) {
        RolapCube cube = evaluator.getCube();
        Set<Hierarchy> included = availableHierarchies(cube,
            hierarchy -> isAvailable(query, cube, hierarchy));
        if (!included.isEmpty()) {
            RolapCube bindingCube = resolveBindingCube(cube, included);
            addPredicate(query, contextPredicate(evaluator, bindingCube, included, null, strict));
        }
    }

    private static Set<Hierarchy> availableHierarchies(RolapCube cube,
        java.util.function.Predicate<RolapHierarchy> available)
    {
        Set<Hierarchy> included = new LinkedHashSet<>();
        for (Dimension dimension : cube.getDimensions()) {
            if (dimension.isMeasures()) {
                continue;
            }
            for (Hierarchy hierarchy : dimension.getHierarchies()) {
                if (available.test((RolapHierarchy) hierarchy)) {
                    included.add(hierarchy);
                }
            }
        }
        return included;
    }

    /** Same projection as addAvailableContext, without constructing a SQL query. */
    static boolean isSeparable(RolapEvaluator evaluator, List<Set<MondrianDef.Relation>> groups) {
        Set<MondrianDef.Relation> joint = new LinkedHashSet<>();
        groups.forEach(joint::addAll);
        Set<Hierarchy> included = availableHierarchies(evaluator.getCube(), hierarchy -> {
            Set<MondrianDef.Relation> required = new LinkedHashSet<>();
            return IndependentTargetSplit.collectRelations(hierarchy.getRelation(), required)
                && joint.containsAll(required);
        });
        for (Hierarchy hierarchy : included) {
            Set<MondrianDef.Relation> required = new LinkedHashSet<>();
            IndependentTargetSplit.collectRelations(((RolapHierarchy) hierarchy).getRelation(), required);
            // A hierarchy available in the joint SQL must remain available in
            // one projection. A predicate on only one of its columns is not
            // sufficient: addAvailableContext requires its WHOLE relation.
            if (groups.stream().noneMatch(group -> group.containsAll(required))) {
                return false;
            }
        }
        return isSeparable(contextPredicate(evaluator, evaluator.getCube(), included, null, false), groups);
    }

    static boolean isSeparable(StarPredicate predicate, List<Set<MondrianDef.Relation>> groups) {
        if (predicate instanceof AndPredicate and) {
            return and.getChildren().stream().allMatch(child -> isSeparable(child, groups));
        }
        int owner = -1;
        for (RolapStar.Column column : predicate.getConstrainedColumnList()) {
            int found = -1;
            for (int g = 0; g < groups.size(); g++) {
                if (groups.get(g).contains(column.getTable().getRelation())) {
                    found = g;
                    break;
                }
            }
            if (found < 0 || owner >= 0 && owner != found) {
                return false;
            }
            owner = found;
        }
        return true;
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
        List<Member> expansion = expansionOf(evaluator, member);
        if (expansion != null) {
            List<StarPredicate> alternatives = expansion.stream()
                .map(value -> memberPredicate(cube, value, strict)).toList();
            return alternatives.isEmpty() ? new LiteralStarPredicate(null, false)
                : new OrPredicate(alternatives);
        }
        return memberPredicate(cube, member, strict);
    }

    /**
     * The member set {@code member} stands for, or null when it stands for
     * itself. Only a supported calculated member stands for one, through an
     * expansion in the evaluator.
     *
     * <p>A stable one is expanded once for the execution (#97): the set is
     * then a function of the member, so a constraint built from it is a
     * function of the memo key and never of the cell. An unstable one is
     * expanded again here, as before the memo existed, and counted, which is
     * what keeps its constraint out of the memo in {@link #of}.
     */
    private static List<Member> expansionOf(RolapEvaluator evaluator, Member member) {
        if (!member.isCalculated() || !SqlConstraintUtils.isSupportedCalculatedMember(member)) {
            return null;
        }
        RolapEvaluatorRoot root = evaluator.root;
        if (!isStableContextMember(root, member)) {
            root.dimensionContextPerCellExpansions++;
            return expand(evaluator, member);
        }
        List<Member> expansion = root.dimensionContextExpansions.get(member);
        if (expansion == null && !root.dimensionContextExpansions.containsKey(member)) {
            expansion = expand(evaluator, member);
            root.dimensionContextExpansions.put(member, expansion);
        }
        return expansion;
    }

    /** The members of a supported calculated member's set, or null when it is not one. */
    private static List<Member> expand(RolapEvaluator evaluator, Member member) {
        TupleConstraintStruct expanded = new TupleConstraintStruct();
        SqlConstraintUtils.expandSupportedCalculatedMember(member, evaluator, expanded);
        // A unary aggregate is a union. Flattening multi-hierarchy sets
        // would lose their tuple correlations; keep those unsupported.
        if (!expanded.getDisjoinedTupleLists().isEmpty()
            || !expanded.getMembers().stream().allMatch(value -> !value.isCalculated()
                && value.getHierarchy().equals(member.getHierarchy())))
        {
            return null;
        }
        return List.copyOf(expanded.getMembers());
    }

    /**
     * Whether a calculated context member contributes the same predicate to
     * every cell of this execution. Asked once per navigation call, so the
     * answer is memoized per member.
     */
    private static boolean isStableContextMember(RolapEvaluatorRoot root, Member member) {
        Boolean stable = root.dimensionContextStableMembers.get(member);
        if (stable == null) {
            // An unsupported one stands for no member set and restricts nothing, in every cell alike.
            stable = !SqlConstraintUtils.isSupportedCalculatedMember(member)
                || isContextFree(root, member.getExpression());
            root.dimensionContextStableMembers.put(member, stable);
        }
        return stable;
    }

    /**
     * Whether the member set {@code expression} expands into is the same
     * whatever cell asks for it. Mirrors
     * {@link SqlConstraintUtils#expandExpressions}: everything but an
     * aggregated set is a member reference the parse tree already holds, and
     * an aggregated set is read through the compiled form
     * {@link SqlConstraintUtils#expandSetFromCalculatedMember} evaluates. A
     * compiled set that depends on no hierarchy cannot read the cell - the
     * engine's own test, which reports a dependency on every hierarchy for a
     * user-defined function and on its own dimensions for {@code Existing}.
     */
    private static boolean isContextFree(RolapEvaluatorRoot root, Exp expression) {
        if (!(expression instanceof ResolvedFunCall fun)) {
            return true;
        }
        if (fun.getFunDef() instanceof ParenthesesFunDef) {
            return isContextFree(root, fun.getArg(0));
        }
        if (fun.getFunName().equals("+")) {
            for (Exp arg : fun.getArgs()) {
                if (!isContextFree(root, arg)) {
                    return false;
                }
            }
            return true;
        }
        Exp set = fun.getArg(0);
        if (!(set.getType() instanceof SetType)) {
            // Not a set: leave the expansion to fail the way it always has.
            return false;
        }
        Calc calc = root.getCompiled(set, false, ResultStyle.ITERABLE);
        for (Hierarchy hierarchy : root.cube.getHierarchies()) {
            if (calc.dependsOn(hierarchy)) {
                return false;
            }
        }
        return true;
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

    /**
     * Everything a reused constraint reads from the evaluator besides its
     * root, compared by identity: member {@code equals} goes by unique name,
     * which a role-limited rollup member shares with the plain member it wraps.
     * Of the current member of the anchored hierarchy that is only whether a
     * role limits it, so an axis of that hierarchy shares one constraint. A
     * calculated member is the key of what it stands for only while that is
     * memoized per execution (see {@link #expansionOf}).
     */
    static final class MemoKey {
        private final Object[] parts;
        private final long overriddenSlicerPositions;
        private final int hash;

        private MemoKey(Object[] parts, long overriddenSlicerPositions) {
            this.parts = parts;
            this.overriddenSlicerPositions = overriddenSlicerPositions;
            int h = Long.hashCode(overriddenSlicerPositions);
            for (Object part : parts) {
                h = 31 * h + System.identityHashCode(part);
            }
            hash = h;
        }

        /** Null if this context must not be memoized. Runs per navigation call: keep it free of real work. */
        static MemoKey of(RolapEvaluator evaluator, Dimension dimension, Hierarchy anchored) {
            // Current members are looked up by the hierarchy's ordinal in the evaluator's cube.
            if (!(dimension instanceof RolapCubeDimension cubeDimension)
                || cubeDimension.getCube() != evaluator.getCube())
            {
                return null;
            }
            TupleList slicerTuples = evaluator.getSlicerTuples();
            if (slicerTuples != null && slicerTuples.getArity() > Long.SIZE) {
                return null;
            }
            Hierarchy[] hierarchies = dimension.getHierarchies();
            Object[] parts = new Object[hierarchies.length + 3];
            parts[0] = dimension;
            parts[1] = anchored;
            parts[2] = slicerTuples;
            for (int i = 0; i < hierarchies.length; i++) {
                RolapMember member = evaluator.getContext(hierarchies[i]);
                if (member.getHierarchy().equals(anchored)) {
                    // Navigation replaces this member (see contextPredicate), calculated or not; the role
                    // limits alone still ask what it is. Boolean boxes are canonical, so identity holds.
                    parts[i + 3] = SqlConstraintUtils.isRoleLimited(member);
                    continue;
                }
                if (member.isCalculated() && !(member instanceof RolapResult.CompoundSlicerRolapMember)
                    && !isStableContextMember(evaluator.root, member))
                {
                    // Expanded through the evaluator from a set that may read
                    // the cell: a function of the whole cell context.
                    return null;
                }
                parts[i + 3] = member;
            }
            return new MemoKey(parts, evaluator.getOverriddenSlicerPositions());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MemoKey that) || hash != that.hash
                || overriddenSlicerPositions != that.overriddenSlicerPositions
                || parts.length != that.parts.length)
            {
                return false;
            }
            for (int i = 0; i < parts.length; i++) {
                if (parts[i] != that.parts[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
