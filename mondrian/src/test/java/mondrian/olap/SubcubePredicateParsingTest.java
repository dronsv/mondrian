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

import mondrian.olap.fun.BuiltinFunTable;
import mondrian.parser.JavaccParserValidatorImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that real MDX subselect expressions parse into the AST nodes
 * that {@code Query.buildSubcubeAxisPredicate} recognizes for SQL
 * predicate generation.
 *
 * <p>These are parse-level tests (no database, no schema resolution).
 * They confirm the parser produces the correct FunCall tree for each
 * expression pattern handled by our subcube predicate walker.
 */
public class SubcubePredicateParsingTest {

    private static final BuiltinFunTable FUN_TABLE =
        BuiltinFunTable.instance();

    // ---------------------------------------------------------------
    // Literal set — baseline: {[M1], [M2]}
    // ---------------------------------------------------------------
    @Test
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
    @Test
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

    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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

    @Test
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
    @Test
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

    /**
     * AST contract for the Excel InStr-on-caption shape using the
     * explicit {@code Properties("MEMBER_CAPTION")} accessor. The
     * InStr static handler in
     * {@link mondrian.olap.Query#tryInStrCaptionFilter} pattern-matches
     * this shape after validation. (#77 perf follow-up)
     */
    @Test
    public void testFilterInStrPropertiesParse() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT Filter([Product].[Product Name].AllMembers, "
            + "InStr(1, [Product].CurrentMember.Properties("
            + "\"MEMBER_CAPTION\"), \"Carrots\") > 0) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        FunCall filter = assertFunCall(axis, "Filter", 2);
        // arg 0: <hier>.<level>.AllMembers
        assertFunCall(filter.getArg(0), "AllMembers", 1);
        // arg 1: BinaryOp(">", InStr(...), 0)
        FunCall gt = assertFunCall(filter.getArg(1), ">", 2);
        FunCall instr = assertFunCall(gt.getArg(0), "InStr", 3);
        // arg 1 of InStr is the caption accessor.
        assertFunCall(instr.getArg(1), "Properties", 2);
    }

    /**
     * AST contract for the bare {@code .member_caption} accessor — a
     * distinct parser shape from {@code Properties("MEMBER_CAPTION")}.
     * Both shapes must be matched by
     * {@link mondrian.olap.Query#tryInStrCaptionFilter}. (#77 perf
     * follow-up)
     */
    @Test
    public void testFilterInStrMemberCaptionParse() throws Exception {
        Subcube sub = parseSubcube(
            "SELECT [Measures].[Sales] ON 0 FROM ("
            + "SELECT Filter([Product].[Product Name].AllMembers, "
            + "InStr(1, [Product].CurrentMember.member_caption, "
            + "\"Carrots\") > 0) ON 0 "
            + "FROM [Sales])");
        assertNotNull(sub);
        Exp axis = sub.getAxes()[0].getSet();
        FunCall filter = assertFunCall(axis, "Filter", 2);
        FunCall gt = assertFunCall(filter.getArg(1), ">", 2);
        FunCall instr = assertFunCall(gt.getArg(0), "InStr", 3);
        // arg 1 of InStr is the bare-identifier property accessor.
        // Parser emits FunCall named "member_caption" with the member
        // as the single arg — distinct from the "Properties" shape
        // above. The InStr handler must check both function names.
        Exp captionAccess = instr.getArg(1);
        assertTrue(
            captionAccess instanceof FunCall,
            "Expected FunCall for .member_caption accessor but got "
                + captionAccess.getClass().getSimpleName());
        FunCall captionFc = (FunCall) captionAccess;
        assertEquals(
            "member_caption",
            captionFc.getFunName().toLowerCase(java.util.Locale.ROOT),
            "Bare .member_caption parses to a FunCall with this name "
                + "(case-insensitive). The validated tree's "
                + "ResolvedFunCall#getFunName() returns the canonical "
                + "form 'Member_Caption'; the parser-level FunCall here "
                + "preserves the source identifier.");
        assertEquals(
            1,
            captionFc.getArgs().length,
            "Bare .member_caption takes only the member as its arg "
                + "(no string literal). The handler distinguishes "
                + "this shape from Properties by arg count.");
    }

    // ---------------------------------------------------------------
    // TopCount()
    // ---------------------------------------------------------------
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
        assertNotNull(sub, "Expected subcube from MDX: " + mdx);
        return sub;
    }

    /**
     * Asserts that an expression is a FunCall with the expected name
     * and argument count. Returns the FunCall for chaining.
     */
    private FunCall assertFunCall(Exp exp, String expectedName, int expectedArgCount) {
        assertTrue(
            exp instanceof FunCall,
            "Expected FunCall('" + expectedName + "') but got "
                + (exp == null ? "null" : exp.getClass().getSimpleName()
                    + ": " + Util.unparse(exp)));
        FunCall fc = (FunCall) exp;
        assertEquals(
            expectedName, fc.getFunName(),
            "FunCall name mismatch for: " + Util.unparse(exp));
        assertEquals(
            expectedArgCount, fc.getArgs().length,
            "Arg count mismatch for " + expectedName
                + " in: " + Util.unparse(exp));
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
