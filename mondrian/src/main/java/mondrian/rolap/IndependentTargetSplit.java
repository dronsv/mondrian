package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.ResourceLimitExceededException;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.DrilldownLevelCrossJoinArg;

/** A conservative, immutable partition of fact-less native tuple targets. */
final class IndependentTargetSplit {
    final List<List<Integer>> indexes;
    final List<Set<MondrianDef.Relation>> relations;
    final CrossJoinArg[] args;

    private IndependentTargetSplit(List<List<Integer>> indexes,
        List<Set<MondrianDef.Relation>> relations, CrossJoinArg[] args)
    {
        this.indexes = indexes.stream().map(List::copyOf).toList();
        this.relations = relations.stream().map(Set::copyOf).toList();
        this.args = args.clone();
    }

    static boolean eligible(CrossJoinArg arg) {
        if (arg instanceof DrilldownLevelCrossJoinArg drill) {
            return drill.getDrilledMember() != null
                && drill.getDrilledMember().isAll()
                && eligible(drill.getSqlArg());
        }
        return arg != null && arg.getClass() == DescendantsCrossJoinArg.class
            && arg.getMembers() == null && arg.getLevel() != null
            && !arg.getLevel().isAll() && !arg.getLevel().isParentChild();
    }

    static IndependentTargetSplit plan(CrossJoinArg[] targets, CrossJoinArg[] constraints) {
        List<List<Integer>> indexes = new ArrayList<>();
        List<Set<MondrianDef.Relation>> groups = new ArrayList<>();
        for (int i = 0; i < targets.length; i++) {
            if (!eligible(targets[i])) {
                return null;
            }
            Set<MondrianDef.Relation> relations = new LinkedHashSet<>();
            if (!collectRelations(targets[i].getLevel().getHierarchy().getRelation(), relations)) {
                return null;
            }
            indexes.add(new ArrayList<>(List.of(i)));
            groups.add(relations);
        }
        // A constraint hierarchy can bridge target relations. All v1 argument
        // constraints are no-ops; foreign arguments therefore need no SQL scope.
        for (CrossJoinArg arg : constraints) {
            if (!eligible(arg)) {
                return null;
            }
            Set<MondrianDef.Relation> relations = new LinkedHashSet<>();
            if (!collectRelations(arg.getLevel().getHierarchy().getRelation(), relations)) {
                return null;
            }
            int owner = -1;
            for (int g = 0; g < groups.size(); g++) {
                if (!Collections.disjoint(groups.get(g), relations)) {
                    if (owner < 0) {
                        owner = g;
                        groups.get(g).addAll(relations);
                    } else {
                        groups.get(owner).addAll(groups.remove(g));
                        indexes.get(owner).addAll(indexes.remove(g--));
                    }
                }
            }
        }
        for (int g = 0; g < groups.size(); g++) {
            for (int h = g + 1; h < groups.size(); h++) {
                if (!Collections.disjoint(groups.get(g), groups.get(h))) {
                    groups.get(g).addAll(groups.remove(h));
                    indexes.get(g).addAll(indexes.remove(h));
                    h = g;
                }
            }
        }
        if (groups.size() < 2) {
            return null;
        }
        for (List<Integer> group : indexes) {
            Collections.sort(group);
        }
        // Interleaved groups require the joint SQL order in v1.
        int expected = 0;
        for (List<Integer> group : indexes) {
            for (int index : group) {
                if (index != expected++) {
                    return null;
                }
            }
        }
        return new IndependentTargetSplit(indexes, groups, targets);
    }

    static boolean collectRelations(MondrianDef.RelationOrJoin relation,
        Set<MondrianDef.Relation> result)
    {
        if (relation instanceof MondrianDef.Join join) {
            return collectRelations(join.left, result) && collectRelations(join.right, result);
        }
        if (relation instanceof MondrianDef.Relation table) {
            result.add(table);
            return true;
        }
        return false;
    }

    CrossJoinArg[] groupArgs(int group) {
        return indexes.get(group).stream().map(i -> args[i]).toArray(CrossJoinArg[]::new);
    }

    long checkSize(List<Long> sizes) {
        return checkSize(sizes, null);
    }

    long checkSize(List<Long> sizes, String measure) {
        long count = sizes.contains(0L) ? 0 : 1;
        for (long size : sizes) {
            count = size != 0 && count > Long.MAX_VALUE / size ? Long.MAX_VALUE : count * size;
        }
        return checkCount(count, " groups=" + sizes, measure);
    }

    /**
     * Bounds the joint statement of a shape the split leaves to it while that
     * statement streams, so no separate count re-reads the joint relation.
     * Candidates are counted the way the result is built, which keeps the
     * bound exact: n candidates pass a limit of n and fail one of n - 1.
     */
    JointGuard jointGuard(String measure) {
        return new JointGuard(measure);
    }

    final class JointGuard {
        private final int limit = limit();
        private final String measure;
        /**
         * With a drill, the keys of the All/leaf projections so far, as
         * {@link DrilldownLevelCrossJoinArg#expandTupleList} deduplicates
         * them; without one, every raw row is a candidate and both are null.
         */
        private final Set<String> seen;
        private final TupleList expanded;

        private JointGuard(String measure) {
            this.measure = measure;
            boolean drilled = Arrays.stream(args).anyMatch(DrilldownLevelCrossJoinArg.class::isInstance);
            this.seen = drilled ? new HashSet<>() : null;
            this.expanded = drilled ? TupleCollections.createList(args.length) : null;
        }

        /**
         * Rows the driver may return. Raw rows are the candidates unless a
         * drill expands them; repeated unique names can make a drilled
         * result smaller than its rows, so those rows are not capped.
         */
        int statementMaxRows() {
            return seen == null ? limit + 1 : 0;
        }

        /**
         * With a drill, what {@link DrilldownLevelCrossJoinArg#expandTupleList}
         * builds from the rows read so far, in its order: counting the
         * projections already built them, so they are not expanded twice.
         * Null without a drill.
         */
        TupleList expanded() {
            return expanded;
        }

        /**
         * Accounts for the row whose members the targets hold.
         *
         * @param fetched rows fetched so far, the look-ahead row included
         */
        void afterRow(List<TargetBase> targets, int fetched) {
            if (seen == null) {
                if (fetched > limit) {
                    reject(fetched);
                }
                return;
            }
            Member[] row = new Member[targets.size()];
            for (int i = 0; i < row.length; i++) {
                row[i] = targets.get(i).getCurrMember();
            }
            DrilldownLevelCrossJoinArg.forEachNewExpandedTuple(row, args, seen, tuple -> {
                if (seen.size() > limit) {
                    reject(seen.size());
                }
                expanded.addTuple(tuple.clone());
            });
        }

        private void reject(long atLeast) {
            checkCount(atLeast, " jointCountAtLeast=" + atLeast, measure);
        }
    }

    int limit() {
        MondrianProperties properties = MondrianProperties.instance();
        int cap = properties.CrossJoinFactlessSplitMaxCandidates.get();
        int resultLimit = properties.ResultLimit.get();
        if (cap <= 0 || resultLimit > 0 && resultLimit < cap) {
            cap = resultLimit;
        }
        // Tuple storage is an int-indexed flat list, even without a result cap.
        int allocationLimit = Integer.MAX_VALUE / args.length;
        return cap > 0 ? Math.min(cap, allocationLimit) : allocationLimit;
    }

    private long checkCount(long count, String detail, String measure) {
        int limit = limit();
        if (count > limit) {
            String levels = Arrays.stream(args)
                .map(arg -> arg.getLevel().getUniqueName()).toList().toString();
            String message = MondrianResource.instance().LimitExceededDuringCrossjoin.str(count, limit)
                + ". " + MondrianResource.instance().NativeCrossJoinIndependentGuardAdvice.str(levels)
                + detail + (measure == null ? "" : " measure=" + measure);
            RolapNativeSet.LOGGER.warn(message);
            throw new ResourceLimitExceededException(message);
        }
        return count;
    }
}
