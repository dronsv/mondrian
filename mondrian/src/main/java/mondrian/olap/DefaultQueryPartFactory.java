/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.parser.MdxParserValidator;
import mondrian.server.Statement;

import java.util.List;

/**
 * Default implementation of {@link MdxParserValidator.QueryPartFactory}.
 * Extracted from the legacy javacup {@code Parser.FactoryImpl} inner class.
 */
public class DefaultQueryPartFactory
    implements MdxParserValidator.QueryPartFactory
{
    public Query makeQuery(
        Statement statement,
        Formula[] formulae,
        QueryAxis[] axes,
        Subcube subcube,
        Exp slicer,
        QueryPart[] cellProps,
        boolean strictValidation)
    {
        final QueryAxis slicerAxis =
            slicer == null
                ? null
                : new QueryAxis(
                    false, slicer, AxisOrdinal.StandardAxisOrdinal.SLICER,
                    QueryAxis.SubtotalVisibility.Undefined, new Id[0]);
        return new Query(
            statement, formulae, axes, subcube, slicerAxis, cellProps,
            strictValidation);
    }

    public DrillThrough makeDrillThrough(
        Query query,
        int maxRowCount,
        int firstRowOrdinal,
        List<Exp> returnList)
    {
        return new DrillThrough(
            query, maxRowCount, firstRowOrdinal, returnList);
    }

    public CalculatedFormula makeCalculatedFormula(
        String cubeName,
        Formula e)
    {
        return new CalculatedFormula(cubeName, e);
    }

    public Explain makeExplain(QueryPart query) {
        return new Explain(query);
    }

    public Refresh makeRefresh(String cubeName) {
        return new Refresh(cubeName);
    }

    public Update makeUpdate(
        String cubeName,
        List<Update.UpdateClause> list)
    {
        return new Update(cubeName, list);
    }

    public DmvQuery makeDmvQuery(
        String tableName,
        List<String> columns,
        Exp whereExpression)
    {
        return new DmvQuery(tableName, columns, whereExpression);
    }

    public TransactionCommand makeTransactionCommand(
        TransactionCommand.Command c)
    {
        return new TransactionCommand(c);
    }
}
