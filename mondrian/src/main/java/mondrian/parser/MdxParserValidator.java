/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara.
* Copyright (c) 2021 Sergei Semenkov
* All rights reserved.
*/

package mondrian.parser;

import mondrian.olap.*;
import mondrian.server.Statement;
import org.olap4j.OlapException;

import java.util.List;

/**
 * Parses and validates an MDX statement.
 *
 * <p>NOTE: API is subject to change. Current implementation is backwards
 * compatible with the old parser based on JavaCUP.
 *
 * @author jhyde
 */
public interface MdxParserValidator {

    /**
      * Parses a string to create a {@link mondrian.olap.Query}.
      * Called only by {@link mondrian.olap.ConnectionBase#parseQuery}.
      */
    QueryPart parseInternal(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable,
        boolean strictValidation) throws OlapException;

    Exp parseExpression(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable) throws OlapException;

    interface QueryPartFactory {

        /**
         * Creates a {@link mondrian.olap.Query} object.
         * Override this function to make your kind of query.
         */
        Query makeQuery(
            Statement statement,
            Formula[] formulae,
            QueryAxis[] axes,
            Subcube subcube,
            Exp slicer,
            QueryPart[] cellProps,
            boolean strictValidation);

        /**
         * Creates a {@link mondrian.olap.DrillThrough} object.
         */
        DrillThrough makeDrillThrough(
            Query query,
            int maxRowCount,
            int firstRowOrdinal,
            List<Exp> returnList);

        CalculatedFormula makeCalculatedFormula(
                String cubeName,
                Formula e);

        /**
         * Creates an {@link mondrian.olap.Explain} object.
         */
        Explain makeExplain(
            QueryPart query);

        Refresh makeRefresh(
                String cubeName);

        Update makeUpdate(
                String cubeName,
                List<Update.UpdateClause> list);

        DmvQuery makeDmvQuery(
                String tableName,
                List<String> columns,
                Exp whereExpression);

        TransactionCommand makeTransactionCommand(
                TransactionCommand.Command c);
    }
}

// End MdxParserValidator.java
