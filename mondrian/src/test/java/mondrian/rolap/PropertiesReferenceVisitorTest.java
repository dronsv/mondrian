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

import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Literal;
import mondrian.olap.type.MemberType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PropertiesReferenceVisitor} — the M2 visitor
 * that feeds the V2 RequiredPropertyPlan (dronsv/mondrian#22).
 *
 * <p>Tests target the visitor's public {@code visit(ResolvedFunCall)}
 * surface directly with mocked function calls, since building real
 * resolved MDX ASTs requires a live schema. The visit-traversal
 * (Query → Formula → axes → nested FunCalls) is exercised by the
 * existing engine MDX-resolution path and covered by the M3
 * integration tests that consume this analysis.
 */
public class PropertiesReferenceVisitorTest {

    /**
     * Builds a Properties-shaped call with the given member-typed
     * hierarchy and second-arg expression (either a string literal or
     * an opaque computed expression).
     */
    private ResolvedFunCall propertiesCall(
        Hierarchy targetHierarchy, Exp nameArg)
    {
        FunDef fn = mock(FunDef.class);
        when(fn.getName()).thenReturn("Properties");
        Exp memberArg = mock(Exp.class);
        if (targetHierarchy != null) {
            MemberType mt = mock(MemberType.class);
            when(mt.getHierarchy()).thenReturn(targetHierarchy);
            when(memberArg.getType()).thenReturn(mt);
        }
        ResolvedFunCall call = mock(ResolvedFunCall.class);
        when(call.getFunDef()).thenReturn(fn);
        when(call.getArgs()).thenReturn(new Exp[] { memberArg, nameArg });
        return call;
    }

    private ResolvedFunCall opaqueConstructorCall(String funcName) {
        FunDef fn = mock(FunDef.class);
        when(fn.getName()).thenReturn(funcName);
        ResolvedFunCall call = mock(ResolvedFunCall.class);
        when(call.getFunDef()).thenReturn(fn);
        // args don't matter — the visitor short-circuits on name.
        lenient().when(call.getArgs()).thenReturn(new Exp[0]);
        return call;
    }

    @Test
    public void emptyVisit_noReferencesNoOpaque() {
        PropertiesReferenceVisitor v = new PropertiesReferenceVisitor();
        // Don't visit anything — the empty analysis should be inert.
        // Use reflection-free access: visit a no-op call to drain
        // initial state via the public API.
        PropertiesReferenceVisitor.Analysis a =
            PropertiesReferenceVisitor.analyzeQuery(null);
        assertTrue(a.referencesPerHierarchy().isEmpty());
        assertTrue(a.opaqueHierarchies().isEmpty());
        assertFalse(a.globallyOpaque());
    }

    @Test
    public void literalReference_recordsOnHierarchy() {
        Hierarchy h = mock(Hierarchy.class);
        PropertiesReferenceVisitor v = new PropertiesReferenceVisitor();
        v.visit(propertiesCall(h, Literal.createString("GTIN")));
        // Use the analysis snapshot via toAnalysis (package-private?
        // The class has analyzeQuery as the public entry; for this
        // unit test we exercise the visitor instance and use
        // reflection-free public Analysis from analyzeQuery semantics
        // by repeating the visit.) Build a small Query-less test via
        // a direct method call:
        // (Use the analyzeQuery static method by re-running on a
        // mocked accept callback.)
        // — Simpler approach: replicate the run by constructing a
        // fresh visitor and feeding visit() events, then read state
        // via the toAnalysis-equivalent: re-invoke analyzeQuery with
        // a query that accepts onto a captured second visitor.
        // For now, this assertion ensures no exception was thrown.
        // The richer state shape is tested through analyzeQuery in
        // the integration test in M3.
        // Simulating a "Query" that delegates to our visitor:
        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(
            propertiesCall(h, Literal.createString("GTIN")));
        assertEquals(1, a.referencesPerHierarchy().size());
        assertTrue(a.referencedOn(h).contains("GTIN"));
        assertFalse(a.isOpaqueFor(h));
        assertFalse(a.globallyOpaque());
    }

    @Test
    public void multipleLiterals_groupsByHierarchy() {
        Hierarchy product = mock(Hierarchy.class);
        Hierarchy customer = mock(Hierarchy.class);

        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(
            propertiesCall(product, Literal.createString("GTIN")),
            propertiesCall(product, Literal.createString("Brand")),
            propertiesCall(customer, Literal.createString("Status")));

        assertEquals(2, a.referencesPerHierarchy().size());
        assertEquals(2, a.referencedOn(product).size());
        assertTrue(a.referencedOn(product).contains("GTIN"));
        assertTrue(a.referencedOn(product).contains("Brand"));
        assertEquals(1, a.referencedOn(customer).size());
        assertTrue(a.referencedOn(customer).contains("Status"));
    }

    @Test
    public void computedNameArg_marksHierarchyOpaque() {
        // .Properties(SomeExpr) where SomeExpr is not a string literal
        // → cannot prove which property name will be selected at
        // runtime → that hierarchy must fall back to eager projection.
        Hierarchy h = mock(Hierarchy.class);
        Exp nonLiteralName = mock(Exp.class);

        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(
            propertiesCall(h, nonLiteralName));

        assertTrue(a.opaqueHierarchies().contains(h));
        assertTrue(a.isOpaqueFor(h));
        // Other hierarchies remain non-opaque.
        Hierarchy other = mock(Hierarchy.class);
        assertFalse(a.isOpaqueFor(other));
    }

    @Test
    public void strToMember_marksGloballyOpaque() {
        // StrToMember can build a member from any string at runtime;
        // any subsequent .Properties() call on that result is opaque.
        // Conservative: every hierarchy in the query falls back to
        // eager.
        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(
            opaqueConstructorCall("StrToMember"));

        assertTrue(a.globallyOpaque());
        // isOpaqueFor short-circuits on global flag for ANY hierarchy.
        assertTrue(a.isOpaqueFor(mock(Hierarchy.class)));
    }

    @Test
    public void strToTuple_strToSet_markGloballyOpaque() {
        PropertiesReferenceVisitor.Analysis a1 = analyzeWithCalls(
            opaqueConstructorCall("StrToTuple"));
        assertTrue(a1.globallyOpaque());

        PropertiesReferenceVisitor.Analysis a2 = analyzeWithCalls(
            opaqueConstructorCall("StrToSet"));
        assertTrue(a2.globallyOpaque());
    }

    @Test
    public void unknownHierarchy_literalArg_marksGloballyOpaque() {
        // .Properties on an Exp whose type does not surface a hierarchy
        // (e.g. a UDF result typed as Member-without-hierarchy). The
        // visitor cannot attribute the reference to any level — must
        // assume worst case.
        Exp memberArgUntyped = mock(Exp.class);
        // Type returns null (or no hierarchy).
        // Constructed inline to avoid a typed-Exp factory.
        FunDef fn = mock(FunDef.class);
        when(fn.getName()).thenReturn("Properties");
        ResolvedFunCall call = mock(ResolvedFunCall.class);
        when(call.getFunDef()).thenReturn(fn);
        when(call.getArgs()).thenReturn(
            new Exp[] { memberArgUntyped, Literal.createString("X") });

        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(call);
        assertTrue(a.globallyOpaque());
    }

    @Test
    public void caseInsensitiveFunctionName() {
        // MDX function names are case-insensitive; the visitor must
        // match "Properties", "properties", "PROPERTIES" identically.
        Hierarchy h = mock(Hierarchy.class);
        FunDef fn = mock(FunDef.class);
        when(fn.getName()).thenReturn("PROPERTIES");
        Exp memberArg = mock(Exp.class);
        MemberType mt = mock(MemberType.class);
        when(mt.getHierarchy()).thenReturn(h);
        when(memberArg.getType()).thenReturn(mt);
        ResolvedFunCall call = mock(ResolvedFunCall.class);
        when(call.getFunDef()).thenReturn(fn);
        when(call.getArgs()).thenReturn(
            new Exp[] { memberArg, Literal.createString("GTIN") });

        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(call);
        assertTrue(a.referencedOn(h).contains("GTIN"));
    }

    @Test
    public void mixedReferences_recordAndOpaqueOnSameHierarchy() {
        // First a literal, then an opaque — the hierarchy stays opaque.
        // Planner must treat as eager: cannot trust the literal set
        // because the opaque branch might select something else at
        // runtime.
        Hierarchy h = mock(Hierarchy.class);

        PropertiesReferenceVisitor.Analysis a = analyzeWithCalls(
            propertiesCall(h, Literal.createString("GTIN")),
            propertiesCall(h, mock(Exp.class)));

        // The literal name is still recorded for completeness…
        assertTrue(a.referencedOn(h).contains("GTIN"));
        // …but the hierarchy is opaque, so the planner ignores the
        // reference set.
        assertTrue(a.isOpaqueFor(h));
    }

    /**
     * Runs the visitor over a sequence of mocked calls and returns the
     * snapshot. Since analyzeQuery requires a Query that itself drives
     * accept(), this helper bypasses by invoking visit() directly on
     * the same instance, then snapshotting via the analyzeQuery
     * entry point's semantics (run on a null Query that produces an
     * empty initial analysis, then merging — except mocks can't merge,
     * so we expose the visitor state through a careful reflection of
     * the analyzeQuery contract: the snapshot is what {@link
     * PropertiesReferenceVisitor#toAnalysis} returns, and that method
     * is private. We can still get a usable snapshot by adding an
     * accept callback that re-runs the visitor across all calls.)
     *
     * <p>For test purposes we construct a tiny mock Query that
     * fan-outs into each call. But we don't have a Query mock.
     * Cleanest: extend the visitor with a public hook for tests.
     * Since the production API is analyzeQuery(Query), and we want
     * to keep the test surface minimal, we replicate the public
     * behaviour by re-creating the visitor and feeding it the calls,
     * then snapping via a tiny private mirror.
     */
    private PropertiesReferenceVisitor.Analysis analyzeWithCalls(
        ResolvedFunCall... calls)
    {
        PropertiesReferenceVisitor v = new PropertiesReferenceVisitor();
        for (ResolvedFunCall c : calls) {
            v.visit(c);
        }
        // Build a tiny mock Query whose accept() does nothing extra
        // (we've already fed events directly). Re-running through
        // analyzeQuery would reset state. Instead, expose state via
        // a small protected hook — but for now, leverage analyzeQuery
        // with a fan-out callback. The simpler approach: temporarily
        // make toAnalysis package-private. Already public-API-friendly
        // enough — see PropertiesReferenceVisitor.snapshotForTesting.
        return v.snapshotForTesting();
    }
}

// End PropertiesReferenceVisitorTest.java
