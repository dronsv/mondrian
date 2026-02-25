/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.parser;

import mondrian.olap.*;
import mondrian.server.Statement;
import org.olap4j.OlapException;

import mondrian.xmla.XmlaUtil;

/**
 * Default implementation of {@link MdxParserValidator}, using the
 * <a href="http://java.net/projects/javacc/">JavaCC</a> parser generator.
 *
 * @author jhyde
 */
public class JavaccParserValidatorImpl implements MdxParserValidator {
    private final QueryPartFactory factory;

    public enum QueryLanguage {
        MDX,
        DAX
    }

    /**
     * Creates a JavaccParserValidatorImpl.
     */
    public JavaccParserValidatorImpl() {
        this(new Parser.FactoryImpl());
    }

    /**
     * Creates a JavaccParserValidatorImpl with an explicit factory for parse
     * tree nodes.
     *
     * @param factory Factory for parse tree nodes
     */
    public JavaccParserValidatorImpl(QueryPartFactory factory) {
        this.factory = factory;
    }

    public QueryPart parseInternal(
        Statement statement,
        String queryString,
        boolean debug,
        FunTable funTable,
        boolean strictValidation) throws OlapException
    {
        QueryLanguage language = detectLanguage(queryString);

        try {
            switch (language) {
                case DAX:
                    return XmlaUtil.DaxParserImpl_parseQuery(factory, statement, queryString, debug, funTable);
                case MDX:
                default:
                    final MdxParserImpl mdxParser = new MdxParserImpl(
                            factory, statement, queryString, debug, funTable, strictValidation
                    );
                    return mdxParser.statementEof();
            }
        } catch (ParseException e) {
            throw convertException(queryString, e);
        }
    }

    public Exp parseExpression(
            Statement statement,
            String queryString,
            boolean debug,
            FunTable funTable) throws OlapException
    {
        QueryLanguage language = detectLanguage(queryString);

        try {
            switch (language) {
                case DAX:
                    return XmlaUtil.DaxParserImpl_parseExpression(factory, statement, queryString, debug, funTable);
                case MDX:
                default:
                    final MdxParserImpl mdxParser = new MdxParserImpl(
                            factory, statement, queryString, debug, funTable, false
                    );
                    return mdxParser.expressionEof();
            }
        } catch (ParseException e) {
            throw convertException(queryString, e);
        }
    }

    private QueryLanguage detectLanguage(String queryString) throws OlapException {
        if(XmlaUtil.isDaxQuery(queryString)) {
            return QueryLanguage.DAX;
        }

        // Default to MDX
        return QueryLanguage.MDX;
    }

    /**
     * Converts the exception so that it looks like the exception produced by
     * JavaCUP. (Not that that format is ideal, but it minimizes test output
     * changes during the transition from JavaCUP to JavaCC.)
     *
     * @param queryString MDX query string
     * @param pe JavaCC parse exception
     * @return Wrapped exception
     */
    private RuntimeException convertException(
        String queryString,
        ParseException pe)
    {
        Exception e;
        if (pe.getMessage().startsWith("Encountered ")) {
            final Token errorToken =
                pe.currentToken == null ? null : pe.currentToken.next;
            if (errorToken == null) {
                e = pe;
                return Util.newError(e, "While parsing " + queryString);
            }
            e = new MondrianException(
                "Syntax error at line "
                + errorToken.beginLine
                + ", column "
                + errorToken.beginColumn
                + ", token '"
                + errorToken.image
                + "'");
        } else {
            e = pe;
        }
        return Util.newError(e, "While parsing " + queryString);
    }
}

// End JavaccParserValidatorImpl.java
