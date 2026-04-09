/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.mdx.MemberExpr;
import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Hierarchy;
import mondrian.olap.Literal;
import mondrian.olap.Member;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PostProcessEvaluator} — Phase D.3 of NativeQueryEngine.
 */
public class PostProcessEvaluatorTest extends TestCase {

    // -----------------------------------------------------------------------
    // RATIO tests
    // -----------------------------------------------------------------------

    /** Simple ratio: 1000 / 50 = 20 */
    public void testSimpleRatio() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[Sales RUB]", 1000.0);
        ctx.put("Identity", "k1", "[Measures].[AKB]", 50.0);

        DependencyResolver.PostProcessPlan plan = makeRatioPlan(
            "[Measures].[Sales RUB]", "[Measures].[AKB]");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[Sales RUB]", "[Measures].[AKB]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNotNull(result);
        assertEquals(20.0, ((Number) result).doubleValue(), 0.001);
    }

    /** Null denominator → null result */
    public void testNullDenominatorReturnsNull() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[Sales RUB]", 1000.0);
        ctx.put("Identity", "k1", "[Measures].[AKB]", null);

        DependencyResolver.PostProcessPlan plan = makeRatioPlan(
            "[Measures].[Sales RUB]", "[Measures].[AKB]");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[Sales RUB]", "[Measures].[AKB]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNull(result);
    }

    /** Zero denominator → null result (avoid division by zero) */
    public void testZeroDenominatorReturnsNull() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[Sales RUB]", 1000.0);
        ctx.put("Identity", "k1", "[Measures].[AKB]", 0.0);

        DependencyResolver.PostProcessPlan plan = makeRatioPlan(
            "[Measures].[Sales RUB]", "[Measures].[AKB]");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[Sales RUB]", "[Measures].[AKB]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNull(result);
    }

    /** Missing key → null result */
    public void testMissingKeyReturnsNull() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        // No data stored for k1

        DependencyResolver.PostProcessPlan plan = makeRatioPlan(
            "[Measures].[A]", "[Measures].[B]");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[A]", "[Measures].[B]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNull(result);
    }

    // -----------------------------------------------------------------------
    // SCALED_RATIO tests
    // -----------------------------------------------------------------------

    /**
     * Scaled ratio: (500 / 10000) * 100 = 5.0
     * (e.g. WD percentage)
     */
    public void testScaledRatio() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[WD Num]", 500.0);
        ctx.put("Identity", "k1", "[Measures].[Sales Total]", 10000.0);

        DependencyResolver.PostProcessPlan plan = makeScaledRatioPlan(
            "[Measures].[WD Num]", "[Measures].[Sales Total]", 100.0);
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[WD Num]", "[Measures].[Sales Total]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNotNull(result);
        assertEquals(5.0, ((Number) result).doubleValue(), 0.001);
    }

    /** Zero denominator in scaled ratio → null */
    public void testScaledRatioZeroDenominatorReturnsNull() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[WD Num]", 500.0);
        ctx.put("Identity", "k1", "[Measures].[Sales Total]", 0.0);

        DependencyResolver.PostProcessPlan plan = makeScaledRatioPlan(
            "[Measures].[WD Num]", "[Measures].[Sales Total]", 100.0);
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[WD Num]", "[Measures].[Sales Total]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNull(result);
    }

    // -----------------------------------------------------------------------
    // ADDITIVE tests
    // -----------------------------------------------------------------------

    /** Addition: 100 + 30 = 130 */
    public void testAdditive() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[A]", 100.0);
        ctx.put("Identity", "k1", "[Measures].[B]", 30.0);

        DependencyResolver.PostProcessPlan plan = makeAdditivePlan(
            "[Measures].[A]", "[Measures].[B]", "+");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[A]", "[Measures].[B]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNotNull(result);
        assertEquals(130.0, ((Number) result).doubleValue(), 0.001);
    }

    /** Subtraction: 100 - 30 = 70 */
    public void testSubtractive() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[A]", 100.0);
        ctx.put("Identity", "k1", "[Measures].[B]", 30.0);

        DependencyResolver.PostProcessPlan plan = makeAdditivePlan(
            "[Measures].[A]", "[Measures].[B]", "-");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[A]", "[Measures].[B]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNotNull(result);
        assertEquals(70.0, ((Number) result).doubleValue(), 0.001);
    }

    // -----------------------------------------------------------------------
    // SINGLE_REF tests
    // -----------------------------------------------------------------------

    /** Single reference: pass-through of leaf value */
    public void testSingleRef() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[X]", 42.0);

        DependencyResolver.PostProcessPlan plan = makeSingleRefPlan(
            "[Measures].[X]");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[X]");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNotNull(result);
        assertEquals(42.0, ((Number) result).doubleValue(), 0.001);
    }

    // -----------------------------------------------------------------------
    // extractScaleFactor tests
    // -----------------------------------------------------------------------

    /** Extracts 100 from (a / b) * 100 */
    public void testExtractScaleFactorFromMultiply() {
        FunCall divide = mockFunCall("/",
            mockMeasureExpr("a"), mockMeasureExpr("b"));
        Literal hundred = Literal.create(BigDecimal.valueOf(100));
        FunCall multiply = mockFunCall("*", divide, hundred);

        FormulaAnalyzer.Result nf = new FormulaAnalyzer.Result(
            multiply,
            java.util.Arrays.<Exp>asList(
                mockMeasureExpr("a"), mockMeasureExpr("b")),
            false,
            null);

        double factor = PostProcessEvaluator.extractScaleFactor(nf);
        assertEquals(100.0, factor, 0.001);
    }

    /** Returns 1.0 when there is no literal in the multiply */
    public void testExtractScaleFactorDefaultsToOne() {
        FunCall divide = mockFunCall("/",
            mockMeasureExpr("a"), mockMeasureExpr("b"));

        FormulaAnalyzer.Result nf = new FormulaAnalyzer.Result(
            divide,
            java.util.Arrays.<Exp>asList(
                mockMeasureExpr("a"), mockMeasureExpr("b")),
            false,
            null);

        double factor = PostProcessEvaluator.extractScaleFactor(nf);
        assertEquals(1.0, factor, 0.001);
    }

    /** Extracts 0.001 from a * 0.001 (SCALED_VALUE) */
    public void testExtractScaleFactorFromScaledValue() {
        MemberExpr a = mockMeasureExpr("a");
        Literal scale = Literal.create(BigDecimal.valueOf(0.001));
        FunCall multiply = mockFunCall("*", a, scale);

        FormulaAnalyzer.Result nf = new FormulaAnalyzer.Result(
            multiply,
            java.util.Arrays.<Exp>asList(a),
            false,
            null);

        double factor = PostProcessEvaluator.extractScaleFactor(nf);
        assertEquals(0.001, factor, 1e-9);
    }

    // -----------------------------------------------------------------------
    // Multiple tuple keys
    // -----------------------------------------------------------------------

    /** Evaluate the same plan for multiple projected keys */
    public void testMultipleTupleKeys() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[Num]", 100.0);
        ctx.put("Identity", "k1", "[Measures].[Den]", 4.0);
        ctx.put("Identity", "k2", "[Measures].[Num]", 200.0);
        ctx.put("Identity", "k2", "[Measures].[Den]", 5.0);

        DependencyResolver.PostProcessPlan plan = makeRatioPlan(
            "[Measures].[Num]", "[Measures].[Den]");
        Map<String, CoordinateClassPlan> classPlans = makeClassPlans(
            "Identity",
            "[Measures].[Num]", "[Measures].[Den]");

        Object r1 = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);
        Object r2 = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k2"), classPlans);

        assertNotNull(r1);
        assertNotNull(r2);
        assertEquals(25.0, ((Number) r1).doubleValue(), 0.001);
        assertEquals(40.0, ((Number) r2).doubleValue(), 0.001);
    }

    /** Missing class in classPlans → null */
    public void testMissingClassIdReturnsNull() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity", "k1", "[Measures].[A]", 100.0);
        ctx.put("Identity", "k1", "[Measures].[B]", 4.0);

        DependencyResolver.PostProcessPlan plan = makeRatioPlan(
            "[Measures].[A]", "[Measures].[B]");
        // Empty classPlans — classId lookup will fail
        Map<String, CoordinateClassPlan> classPlans =
            new LinkedHashMap<String, CoordinateClassPlan>();

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, makeKeyMap("Identity", "k1"), classPlans);

        assertNull(result);
    }

    /**
     * Cross-cube: numerator from "Sales" class (key "Moscow|Kursk|Choc"),
     * denominator from "Geo" class (key "Moscow" only).
     * Verifies per-class key lookup.
     */
    public void testCrossCubePerClassKeys() {
        NativeQueryResultContext ctx = new NativeQueryResultContext();
        ctx.put("Identity@Sales", "Moscow|Kursk|Choc",
            "[Measures].[Revenue]", 5000.0);
        ctx.put("Identity@Geo", "Moscow",
            "[Measures].[StoreCount]", 10.0);

        // Two leaf bindings — numerator from Sales, denominator from Geo
        Map<Integer, PhysicalValueRequest> bindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();
        bindings.put(0, makeStoredRequest("[Measures].[Revenue]"));
        bindings.put(1, makeStoredRequest("[Measures].[StoreCount]"));

        MemberExpr numExpr = mockMeasureExpr("[Measures].[Revenue]");
        MemberExpr denExpr = mockMeasureExpr("[Measures].[StoreCount]");
        FunCall divide = mockFunCall("/", numExpr, denExpr);
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(divide);

        Member measure = mock(Member.class);
        DependencyResolver.PostProcessPlan plan =
            new DependencyResolver.PostProcessPlan(measure, nf, bindings);

        // Two classes — Sales has Revenue, Geo has StoreCount
        Map<String, CoordinateClassPlan> classPlans =
            new LinkedHashMap<String, CoordinateClassPlan>();
        classPlans.put("Identity@Sales", new CoordinateClassPlan(
            "Identity@Sales",
            Collections.singletonList(
                makeStoredRequest("[Measures].[Revenue]"))));
        classPlans.put("Identity@Geo", new CoordinateClassPlan(
            "Identity@Geo",
            Collections.singletonList(
                makeStoredRequest("[Measures].[StoreCount]"))));

        // Per-class keys: Sales uses full key, Geo uses region-only key
        Map<String, String> keyByClassId =
            new LinkedHashMap<String, String>();
        keyByClassId.put("Identity@Sales", "Moscow|Kursk|Choc");
        keyByClassId.put("Identity@Geo", "Moscow");

        Object result = PostProcessEvaluator.evaluate(
            plan, ctx, keyByClassId, classPlans);

        assertNotNull("cross-cube ratio should not be null", result);
        assertEquals(500.0, ((Number) result).doubleValue(), 0.001);
    }

    // -----------------------------------------------------------------------
    // Helper factories
    // -----------------------------------------------------------------------

    private DependencyResolver.PostProcessPlan makeRatioPlan(
        String numId, String denId)
    {
        MemberExpr numExpr = mockMeasureExpr(numId);
        MemberExpr denExpr = mockMeasureExpr(denId);
        FunCall divide = mockFunCall("/", numExpr, denExpr);
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(divide);

        Map<Integer, PhysicalValueRequest> bindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();
        bindings.put(0, makeStoredRequest(numId));
        bindings.put(1, makeStoredRequest(denId));

        Member measure = mock(Member.class);
        return new DependencyResolver.PostProcessPlan(measure, nf, bindings);
    }

    private DependencyResolver.PostProcessPlan makeScaledRatioPlan(
        String numId, String denId, double scale)
    {
        MemberExpr numExpr = mockMeasureExpr(numId);
        MemberExpr denExpr = mockMeasureExpr(denId);
        FunCall divide = mockFunCall("/", numExpr, denExpr);
        Literal scaleLit = Literal.create(BigDecimal.valueOf(scale));
        FunCall multiply = mockFunCall("*", divide, scaleLit);
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(multiply);

        Map<Integer, PhysicalValueRequest> bindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();
        bindings.put(0, makeStoredRequest(numId));
        bindings.put(1, makeStoredRequest(denId));

        Member measure = mock(Member.class);
        return new DependencyResolver.PostProcessPlan(measure, nf, bindings);
    }

    private DependencyResolver.PostProcessPlan makeAdditivePlan(
        String aId, String bId, String op)
    {
        MemberExpr aExpr = mockMeasureExpr(aId);
        MemberExpr bExpr = mockMeasureExpr(bId);
        FunCall fc = mockFunCall(op, aExpr, bExpr);
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(fc);

        Map<Integer, PhysicalValueRequest> bindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();
        bindings.put(0, makeStoredRequest(aId));
        bindings.put(1, makeStoredRequest(bId));

        Member measure = mock(Member.class);
        return new DependencyResolver.PostProcessPlan(measure, nf, bindings);
    }

    private DependencyResolver.PostProcessPlan makeSingleRefPlan(String id) {
        MemberExpr expr = mockMeasureExpr(id);
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(expr);

        Map<Integer, PhysicalValueRequest> bindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();
        bindings.put(0, makeStoredRequest(id));

        Member measure = mock(Member.class);
        return new DependencyResolver.PostProcessPlan(measure, nf, bindings);
    }

    private PhysicalValueRequest makeStoredRequest(String measureId) {
        return new PhysicalValueRequest(
            measureId,
            Collections.<Hierarchy>emptySet(),
            null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);
    }

    /**
     * Creates a single-entry classId -> projectedKey map.
     * Used when all leaves share the same projected key.
     */
    private Map<String, String> makeKeyMap(
        String classId, String projectedKey)
    {
        Map<String, String> map = new LinkedHashMap<String, String>();
        map.put(classId, projectedKey);
        return map;
    }

    private Map<String, CoordinateClassPlan> makeClassPlans(
        String classId, String... measureIds)
    {
        List<PhysicalValueRequest> requests =
            new ArrayList<PhysicalValueRequest>();
        for (String id : measureIds) {
            requests.add(makeStoredRequest(id));
        }
        Map<String, CoordinateClassPlan> map =
            new LinkedHashMap<String, CoordinateClassPlan>();
        map.put(classId, new CoordinateClassPlan(classId, requests));
        return map;
    }

    private MemberExpr mockMeasureExpr(String name) {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(true);
        when(member.getName()).thenReturn(name);

        MemberExpr expr = mock(MemberExpr.class);
        when(expr.getMember()).thenReturn(member);
        return expr;
    }

    private FunCall mockFunCall(String name, Exp... args) {
        FunCall fc = mock(FunCall.class);
        when(fc.getFunName()).thenReturn(name);
        when(fc.getArgs()).thenReturn(args);
        when(fc.getArgCount()).thenReturn(args.length);
        for (int i = 0; i < args.length; i++) {
            when(fc.getArg(i)).thenReturn(args[i]);
        }
        return fc;
    }
}

// End PostProcessEvaluatorTest.java
