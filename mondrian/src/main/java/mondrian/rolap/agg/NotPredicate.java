/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.*;

/**
 * A {@link StarPredicate} that negates another predicate.
 * Generates {@code NOT (<inner>)} in SQL.
 *
 * <p>Used by subcube predicate generation when an MDX subselect axis
 * contains a negated set expression such as {@code -{[Member1], [Member2]}}
 * or {@code Except(set1, set2)}.
 */
public class NotPredicate implements StarPredicate {

    private final StarPredicate inner;

    public NotPredicate(StarPredicate inner) {
        assert inner != null;
        this.inner = inner;
    }

    public StarPredicate getInner() {
        return inner;
    }

    public List<RolapStar.Column> getConstrainedColumnList() {
        return inner.getConstrainedColumnList();
    }

    public BitKey getConstrainedColumnBitKey() {
        return inner.getConstrainedColumnBitKey();
    }

    public boolean evaluate(List<Object> valueList) {
        return !inner.evaluate(valueList);
    }

    public boolean equalConstraint(StarPredicate that) {
        if (that instanceof NotPredicate) {
            return inner.equalConstraint(((NotPredicate) that).inner);
        }
        return false;
    }

    public StarPredicate minus(StarPredicate predicate) {
        throw Util.needToImplement(this);
    }

    public StarPredicate or(StarPredicate predicate) {
        List<StarPredicate> list = new ArrayList<StarPredicate>();
        list.add(this);
        list.add(predicate);
        return new OrPredicate(list);
    }

    public StarPredicate and(StarPredicate predicate) {
        List<StarPredicate> list = new ArrayList<StarPredicate>();
        list.add(this);
        list.add(predicate);
        return new AndPredicate(list);
    }

    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        buf.append("NOT (");
        inner.toSql(sqlQuery, buf);
        buf.append(")");
    }

    public void describe(StringBuilder buf) {
        buf.append("not(");
        inner.describe(buf);
        buf.append(")");
    }

    public int hashCode() {
        return ~inner.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof NotPredicate) {
            return inner.equals(((NotPredicate) obj).inner);
        }
        return false;
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder();
        describe(buf);
        return buf.toString();
    }
}

// End NotPredicate.java
