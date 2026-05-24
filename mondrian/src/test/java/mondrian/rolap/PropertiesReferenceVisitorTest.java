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
     * Feeds mocked {@code ResolvedFunCall} events directly into a
     * fresh visitor and snapshots the result. The production entry
     * point {@code analyzeQuery(Query)} requires a real
     * Query/Formula/axis tree; this helper exists so tests can
     * exercise the visitor's per-call branching without standing
     * up a schema. The snapshot is exposed via package-private
     * {@code snapshotForTesting()} on the visitor for the same
     * reason — keeps the production API minimal while keeping
     * tests deterministic.
     */
    private PropertiesReferenceVisitor.Analysis analyzeWithCalls(
        ResolvedFunCall... calls)
    {
        PropertiesReferenceVisitor v = new PropertiesReferenceVisitor();
        for (ResolvedFunCall c : calls) {
            v.visit(c);
        }
        return v.snapshotForTesting();
    }
}

// End PropertiesReferenceVisitorTest.java
