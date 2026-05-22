/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.Collection;

/**
 * Predicate that constrains a column through an SQL subquery.
 *
 * <p>Used for static subcube predicates whose matching key set may be large.
 * The predicate intentionally keeps the filter in SQL instead of resolving
 * keys in Java and emitting a large value list.</p>
 */
public class SqlInSubqueryPredicate extends AbstractColumnPredicate {
    private final String subquerySql;

    public SqlInSubqueryPredicate(
        RolapStar.Column constrainedColumn,
        String subquerySql)
    {
        super(constrainedColumn);
        assert subquerySql != null;
        this.subquerySql = subquerySql;
    }

    public String getSubquerySql() {
        return subquerySql;
    }

    public String toSqlWithColumn(String columnSql) {
        return columnSql + " IN (" + subquerySql + ")";
    }

    @Override
    public void values(Collection<Object> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean evaluate(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equalConstraint(StarPredicate that) {
        return that instanceof SqlInSubqueryPredicate
            && getConstrainedColumnBitKey().equals(
                that.getConstrainedColumnBitKey())
            && subquerySql.equals(
                ((SqlInSubqueryPredicate) that).subquerySql);
    }

    @Override
    public void describe(StringBuilder buf) {
        buf.append("inSubquery(");
        buf.append(subquerySql);
        buf.append(')');
    }

    @Override
    public Overlap intersect(StarColumnPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mightIntersect(StarPredicate other) {
        return true;
    }

    @Override
    public StarColumnPredicate minus(StarPredicate predicate) {
        return new MinusStarPredicate(this, (StarColumnPredicate) predicate);
    }

    @Override
    public StarColumnPredicate cloneWithColumn(RolapStar.Column column) {
        return new SqlInSubqueryPredicate(column, subquerySql);
    }

    @Override
    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        final RolapStar.Column column = getConstrainedColumn();
        buf.append(toSqlWithColumn(column.generateExprString(sqlQuery)));
    }
}

// End SqlInSubqueryPredicate.java
