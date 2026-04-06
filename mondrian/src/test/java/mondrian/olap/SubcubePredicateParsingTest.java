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

import junit.framework.TestCase;
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.parser.JavaccParserValidatorImpl;

/**
 * Verifies that real MDX subselect expressions parse into the AST nodes
 * that {@code Query.buildSubcubeAxisPredicate} recognizes for SQL
 * predicate generation.
 *
 * <p>These are parse-level tests (no database, no schema resolution).
 * They confirm the parser produces the correct FunCall tree for each
 * expression pattern handled by our subcube predicate walker.
 */
public class SubcubePredicateParsingTest extends TestCase {

    private static final BuiltinFunTable FUN_TABLE =
        BuiltinFunTable.instance();

    // ---------------------------------------------------------------
    // Literal set — baseline: {[M1], [M2]}
    // ---------------------------------------------------------------
    public void testLiteralSetInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT {[Product].[Dairy], [Product].[Meat]} ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "{}", 2);
    }

    // ---------------------------------------------------------------
    // Negation: -{[M1], [M2]}
    // ---------------------------------------------------------------
    public void testNegatedSetInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT -{[Product].[Dairy], [Product].[Meat]} ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        // Top-level is the unary minus "-"
        FunCall minus = assertFunCall(axis, "-", 1);
        // Inner is the set literal
        assertFunCall(minus.getArg(0), "{}", 2);
    }

    public void testNegatedSingleMemberInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT -{[Product].[Dairy]} ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        FunCall minus = assertFunCall(axis, "-", 1);
        assertFunCall(minus.getArg(0), "{}", 1);
    }

    // ---------------------------------------------------------------
    // Except(set1, set2)
    // ---------------------------------------------------------------
    public void testExceptInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT Except({[Product].[Dairy], [Product].[Meat], "
            + "[Product].[Bakery]}, {[Product].[Meat]}) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        FunCall except = assertFunCall(axis, "Except", 2);
        assertFunCall(except.getArg(0), "{}", 3);
        assertFunCall(except.getArg(1), "{}", 1);
    }

    // ---------------------------------------------------------------
    // Infix set minus: {A, B, C} - {B}
    // The parser resolves this as infix "-" with 2 args.
    // ---------------------------------------------------------------
    public void testInfixSetMinusInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT ({[Product].[Dairy], [Product].[Meat]} "
            + "- {[Product].[Meat]}) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        // Parser wraps parenthesized expression in "()" FunCall.
        // Inside is the infix "-" with 2 set args.
        FunCall paren = assertFunCall(axis, "()", 1);
        FunCall minus = assertFunCall(paren.getArg(0), "-", 2);
        assertFunCall(minus.getArg(0), "{}", 2);
        assertFunCall(minus.getArg(1), "{}", 1);
    }

    // ---------------------------------------------------------------
    // <Hierarchy>.Members
    // ---------------------------------------------------------------
    public void testHierarchyMembersInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT [Product].Members ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "Members", 1);
    }

    // ---------------------------------------------------------------
    // <Member>.Children
    // ---------------------------------------------------------------
    public void testMemberChildrenInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT [Product].[Dairy].Children ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "Children", 1);
    }

    // ---------------------------------------------------------------
    // Descendants(<Member>, <Level>)
    // ---------------------------------------------------------------
    public void testDescendantsInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT Descendants([Product].[All Products], "
            + "[Product].[Brand]) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "Descendants", 2);
    }

    public void testDescendantsWithFlagInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT Descendants([Product].[All Products], "
            + "[Product].[Brand], SELF) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "Descendants", 3);
    }

    // ---------------------------------------------------------------
    // Filter()
    // ---------------------------------------------------------------
    public void testFilterInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT Filter([Product].Members, "
            + "[Measures].[Sales] > 100) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "Filter", 2);
    }

    // ---------------------------------------------------------------
    // TopCount()
    // ---------------------------------------------------------------
    public void testTopCountInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT TopCount([Product].Members, 10, "
            + "[Measures].[Sales]) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "TopCount", 3);
    }

    // ---------------------------------------------------------------
    // NonEmpty()
    // ---------------------------------------------------------------
    public void testNonEmptyFunctionInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT NonEmpty([Product].Members, "
            + "[Measures].[Sales]) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        assertFunCall(axis, "NonEmpty", 2);
    }

    // ---------------------------------------------------------------
    // CrossJoin in subselect
    // ---------------------------------------------------------------
    public void testCrossJoinInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT CrossJoin({[Product].[Dairy]}, "
            + "{[Store].[USA]}) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        FunCall cj = assertFunCall(axis, "CrossJoin", 2);
        assertFunCall(cj.getArg(0), "{}", 1);
        assertFunCall(cj.getArg(1), "{}", 1);
    }

    // ---------------------------------------------------------------
    // Nested subselects
    // ---------------------------------------------------------------
    public void testNestedSubselects() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT {[Store].[USA]} ON 0 FROM ("
            + "SELECT {[Product].[Dairy]} ON 0 "
            + "FROM [Sales]))");
        assertNotNull(sub);
        // Outer subselect axis
        Exp outerAxis = sub.getAxes()[0].getSet();
        assertFunCall(outerAxis, "{}", 1);
        // Inner subselect
        Subcube inner = sub.getSubcube();
        assertNotNull(inner);
        Exp innerAxis = inner.getAxes()[0].getSet();
        assertFunCall(innerAxis, "{}", 1);
    }

    // ---------------------------------------------------------------
    // Negation + CrossJoin combo
    // ---------------------------------------------------------------
    public void testNegatedCrossJoinInSubselect() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT -{[Product].[Dairy]} * {[Store].[USA]} ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        // The parser produces: *( -{[Dairy]}, {[USA]} )
        FunCall star = assertFunCall(axis, "*", 2);
        FunCall neg = assertFunCall(star.getArg(0), "-", 1);
        assertFunCall(neg.getArg(0), "{}", 1);
        assertFunCall(star.getArg(1), "{}", 1);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Parses an MDX query and returns the outermost Subcube (the
     * inner subselect, not the base cube).
     */
    private Subcube parseSubcube(String mdx) throws Exception {
        SubcubeCaptureFactory factory = new SubcubeCaptureFactory();
        new JavaccParserValidatorImpl(factory).parseInternal(
            null, mdx, false, FUN_TABLE, false);
        // The outermost subcube has axes; the innermost just has cubeName.
        Subcube sub = factory.subcube;
        assertNotNull("Expected subcube from MDX: " + mdx, sub);
        return sub;
    }

    /**
     * Asserts that an expression is a FunCall with the expected name
     * and argument count. Returns the FunCall for chaining.
     */
    private FunCall assertFunCall(Exp exp, String expectedName, int expectedArgCount) {
        assertTrue(
            "Expected FunCall('" + expectedName + "') but got "
                + (exp == null ? "null" : exp.getClass().getSimpleName()
                    + ": " + Util.unparse(exp)),
            exp instanceof FunCall);
        FunCall fc = (FunCall) exp;
        assertEquals(
            "FunCall name mismatch for: " + Util.unparse(exp),
            expectedName, fc.getFunName());
        assertEquals(
            "Arg count mismatch for " + expectedName
                + " in: " + Util.unparse(exp),
            expectedArgCount, fc.getArgs().length);
        return fc;
    }

    private static class SubcubeCaptureFactory
        extends DefaultQueryPartFactory
    {
        Subcube subcube;

        @Override
        public Query makeQuery(
            mondrian.server.Statement statement,
            Formula[] formulae,
            QueryAxis[] axes,
            Subcube subcube,
            Exp slicer,
            QueryPart[] cellProps,
            boolean strictValidation)
        {
            this.subcube = subcube;
            return null;
        }
    }
}

// End SubcubePredicateParsingTest.java
