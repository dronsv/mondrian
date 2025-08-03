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
import mondrian.xmla.Rowset;
import mondrian.xmla.RowsetDefinition;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;

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
                    try {
                        String className = "emondrian.dax.DaxParserImpl";

                        Class<?> clazz = Class.forName(className,true, mondrian.server.MondrianServerImpl.ModulesLoader);

                        java.lang.reflect.Method method = clazz.getMethod(
                                "parseQuery",
                                QueryPartFactory.class,
                                Statement.class,
                                String.class,
                                boolean.class,
                                FunTable.class);

                        // Call method with argument (null for static)
                        Object result = method.invoke(null, factory, statement, queryString, debug, funTable);

                        QueryPart queryPart =  (QueryPart )result;
                        return queryPart;
                    } catch (ClassNotFoundException e) {
                        throw new OlapException("The emondrian DAX module was not found.");
                    } catch (NoSuchMethodException e) {
                        throw new OlapException("The emondrian DAX DaxParser.parseQuery method was not found.");
                    } catch (Exception e) {
                        throw new OlapException("The emondrian DAX module was not found.", e);
                    }

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
                    try {
                        String className = "emondrian.dax.DaxParserImpl";

                        Class<?> clazz = Class.forName(className,true, mondrian.server.MondrianServerImpl.ModulesLoader);

                        java.lang.reflect.Method method = clazz.getMethod(
                                "parseExpression",
                                QueryPartFactory.class,
                                Statement.class,
                                String.class,
                                boolean.class,
                                FunTable.class);

                        // Call method with argument (null for static)
                        Object result = method.invoke(null, factory, statement, queryString, debug, funTable);

                        Exp exp =  (Exp)result;
                        return exp;
                    } catch (ClassNotFoundException e) {
                        throw new OlapException("The emondrian DAX module was not found.");
                    } catch (NoSuchMethodException e) {
                        throw new OlapException("The emondrian DAX DaxParser.parseQuery method was not found.");
                    } catch (Exception e) {
                        throw new OlapException("The emondrian DAX module was not found.");
                    }
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

    private QueryLanguage detectLanguage(String queryString) {
        String trimmed = queryString.trim().toUpperCase(java.util.Locale.ROOT);

        if (trimmed.startsWith("EVALUATE") ||
                trimmed.startsWith("DEFINE")) {
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
            e = new MondrianException(
                "Syntax error at line "
                + pe.currentToken.next.beginLine
                + ", column "
                + pe.currentToken.next.beginColumn
                + ", token '"
                + pe.currentToken.next.image
                + "'");
        } else {
            e = pe;
        }
        return Util.newError(e, "While parsing " + queryString);
    }
}

// End JavaccParserValidatorImpl.java
