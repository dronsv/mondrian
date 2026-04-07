/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Native-safe DrilldownLevel subset for simple unary sets such as
 * {@code DrilldownLevel({[Hierarchy].[All]})}.
 *
 * <p>The SQL path operates on the drilled child level, while the result tuple
 * list is expanded afterwards back to {@code parent + children} semantics.</p>
 */
public class DrilldownLevelCrossJoinArg implements CrossJoinArg {
    private final CrossJoinArg sqlArg;
    private final RolapMember drilledMember;

    public DrilldownLevelCrossJoinArg(
        CrossJoinArg sqlArg,
        RolapMember drilledMember)
    {
        this.sqlArg = sqlArg;
        this.drilledMember = drilledMember;
    }

    public RolapLevel getLevel() {
        return sqlArg.getLevel();
    }

    public List<RolapMember> getMembers() {
        return null;
    }

    public RolapMember getDrilledMember() {
        return drilledMember;
    }

    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
        sqlArg.addConstraint(sqlQuery, baseCube, aggStar);
    }

    public boolean isPreferInterpreter(boolean joinArg) {
        return false;
    }

    public static TupleList expandTupleList(
        TupleList tupleList,
        CrossJoinArg[] args)
    {
        if (tupleList == null || tupleList.isEmpty() || args == null) {
            return tupleList;
        }
        final TupleList expanded =
            TupleCollections.createList(tupleList.getArity());
        final Set<String> seen = new LinkedHashSet<String>();
        for (List<Member> tuple : tupleList) {
            final Member[] members = tuple.toArray(new Member[tuple.size()]);
            addExpandedTuples(expanded, seen, members, args, 0);
        }
        return expanded;
    }

    private static void addExpandedTuples(
        TupleList expanded,
        Set<String> seen,
        Member[] members,
        CrossJoinArg[] args,
        int index)
    {
        if (index >= args.length) {
            final String key = tupleKey(members);
            if (seen.add(key)) {
                expanded.addTuple(members.clone());
            }
            return;
        }

        final CrossJoinArg arg = args[index];
        if (arg instanceof DrilldownLevelCrossJoinArg) {
            final DrilldownLevelCrossJoinArg drilldownArg =
                (DrilldownLevelCrossJoinArg) arg;
            final Member original = members[index];
            final RolapMember drilledMember = drilldownArg.getDrilledMember();
            if (drilledMember != null
                && (original == null
                    || !drilledMember.getUniqueName()
                        .equals(original.getUniqueName())))
            {
                members[index] = drilledMember;
                addExpandedTuples(expanded, seen, members, args, index + 1);
                members[index] = original;
            }
        }

        addExpandedTuples(expanded, seen, members, args, index + 1);
    }

    private static String tupleKey(Member[] tuple) {
        final StringBuilder key = new StringBuilder();
        for (int i = 0; i < tuple.length; i++) {
            if (i > 0) {
                key.append('\u0001');
            }
            final Member member = tuple[i];
            key.append(member == null ? "" : member.getUniqueName());
        }
        return key.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DrilldownLevelCrossJoinArg)) {
            return false;
        }
        final DrilldownLevelCrossJoinArg that =
            (DrilldownLevelCrossJoinArg) obj;
        if (!equals(this.sqlArg, that.sqlArg)) {
            return false;
        }
        return equals(this.drilledMember, that.drilledMember);
    }

    @Override
    public int hashCode() {
        int c = sqlArg == null ? 1 : sqlArg.hashCode();
        if (drilledMember != null) {
            c = 31 * c + drilledMember.hashCode();
        }
        return c;
    }

    private static boolean equals(Object left, Object right) {
        return left == null ? right == null : left.equals(right);
    }
}

