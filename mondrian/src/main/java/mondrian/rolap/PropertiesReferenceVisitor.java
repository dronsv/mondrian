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

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Category;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Literal;
import mondrian.olap.Query;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * M2 of the V2 RequiredPropertyProjection (see dronsv/mondrian#22).
 *
 * <p>Walks an MDX expression tree (typically a resolved {@link Query})
 * and collects, per hierarchy, the literal property names referenced
 * by {@code .Properties("X")} function calls — plus a per-hierarchy
 * "opaque" flag set whenever the call cannot be statically resolved
 * to literal names (computed property names like
 * {@code .Properties(Iif(...))}, dynamic member construction via
 * {@code StrToMember} / {@code StrToTuple} / {@code StrToSet}, or
 * any UDF whose return is used as the property-name argument).
 *
 * <p>Used by {@link mondrian.rolap.RequiredPropertyPlan} to decide
 * which level properties may be safely pruned from tuple/member SQL
 * projection. The contract is fail-safe to eager: if a hierarchy is
 * marked opaque, every level on it falls back to projecting all
 * schema-declared properties for the analysed query (the V1-narrow
 * behaviour). Static analysis only optimises when it can prove the
 * property name is a string literal at compile time.
 *
 * <p>This class is observability-only when used directly — it does
 * not affect SQL generation. The {@code RequiredPropertyPlan} (M3)
 * consumes its output to make projection decisions.
 *
 * <h3>Coverage</h3>
 *
 * <p>The visitor walks the {@link Query} entry point, which already
 * recursively descends into:
 * <ul>
 *   <li>{@link mondrian.olap.QueryAxis} (axes + slicer)</li>
 *   <li>{@link mondrian.olap.Formula} (WITH MEMBER / WITH SET)</li>
 *   <li>nested {@link ResolvedFunCall} arguments</li>
 * </ul>
 *
 * <p>Schema-defined {@code <CalculatedMember>}, {@code <NamedSet>},
 * and {@code <Role>} expressions are <strong>not</strong> walked by
 * this entry point — they live on the schema, not the query. Callers
 * that need full coverage should iterate them separately and visit
 * each expression. The {@code RequiredPropertyPlan} integration does
 * this at the schema-load step.
 */
public final class PropertiesReferenceVisitor extends MdxVisitorImpl {

    /**
     * Per-hierarchy set of literal property names statically referenced
     * via {@code .Properties("name")} in the visited tree. Iteration
     * order is insertion order for stability of any downstream
     * diagnostics.
     */
    private final Map<Hierarchy, Set<String>> referencesPerHierarchy =
        new HashMap<>();

    /**
     * Hierarchies where at least one opaque reference was seen — the
     * planner must fall back to eager projection for every level on
     * them.
     */
    private final Set<Hierarchy> opaqueHierarchies = new HashSet<>();

    /**
     * Set when an opaque construction is encountered whose target
     * hierarchy cannot be statically inferred (e.g. a top-level
     * {@code StrToMember} whose result feeds {@code .Properties}).
     * The planner must fall back to eager projection for every
     * hierarchy in the query.
     */
    private boolean globallyOpaque = false;

    /**
     * Runs the visitor on the given resolved {@link Query} and returns
     * an immutable snapshot of the analysis. The query must be
     * resolved (call after {@link Query#resolve}) so
     * {@link ResolvedFunCall} types are populated.
     */
    public static Analysis analyzeQuery(Query query) {
        PropertiesReferenceVisitor v = new PropertiesReferenceVisitor();
        if (query != null) {
            query.accept(v);
        }
        return v.toAnalysis();
    }

    /**
     * Snapshot of the visitor's findings. The {@code referencesPerHierarchy}
     * map is unmodifiable; {@code opaqueHierarchies} is unmodifiable;
     * {@code globallyOpaque} short-circuits all per-hierarchy decisions.
     */
    public record Analysis(
        Map<Hierarchy, Set<String>> referencesPerHierarchy,
        Set<Hierarchy> opaqueHierarchies,
        boolean globallyOpaque)
    {
        /**
         * Returns true when the planner must treat all properties on
         * {@code hierarchy} as required (cannot safely prune anything).
         * Combines the per-hierarchy opaque flag with the global flag.
         */
        public boolean isOpaqueFor(Hierarchy hierarchy) {
            return globallyOpaque
                || opaqueHierarchies.contains(hierarchy);
        }

        /**
         * Returns the set of literal property names statically referenced
         * on the given hierarchy. Empty if none. Callers must additionally
         * check {@link #isOpaqueFor(Hierarchy)} — when opaque, this set
         * is meaningless because the planner cannot prove the full
         * reference set.
         */
        public Set<String> referencedOn(Hierarchy hierarchy) {
            Set<String> set = referencesPerHierarchy.get(hierarchy);
            return set == null ? Collections.emptySet() : set;
        }
    }

    @Override
    public Object visit(ResolvedFunCall call) {
        if (call == null || call.getFunDef() == null) {
            return null;
        }
        String name = call.getFunDef().getName();
        if (name == null) {
            return null;
        }
        if ("Properties".equalsIgnoreCase(name)) {
            handlePropertiesCall(call);
        } else if (isOpaqueConstructor(name)) {
            // StrToMember / StrToTuple / StrToSet — runtime-resolved
            // member identity. Any subsequent .Properties() on the
            // result would not be statically attributable. Conservative:
            // mark globally opaque so the planner doesn't prune
            // anywhere in this query.
            globallyOpaque = true;
        }
        // Always descend — Properties calls may be nested inside other
        // function calls (Filter, Iif, etc.).
        return null;
    }

    /**
     * Handles a resolved {@code .Properties()} call. Records the
     * literal name when both the member argument has an inferable
     * hierarchy and the name argument is a string literal; marks the
     * relevant hierarchy opaque otherwise.
     */
    private void handlePropertiesCall(ResolvedFunCall call) {
        Exp[] args = call.getArgs();
        if (args == null || args.length < 2) {
            return;
        }
        Hierarchy targetHierarchy = inferHierarchy(args[0]);
        Exp nameArg = args[1];
        if (nameArg instanceof Literal lit
            && lit.category == Category.String
            && lit.getValue() instanceof String s)
        {
            if (targetHierarchy != null) {
                referencesPerHierarchy
                    .computeIfAbsent(
                        targetHierarchy, h -> new HashSet<>())
                    .add(s);
            } else {
                // Can't attribute to a hierarchy → conservative
                // global opacity.
                globallyOpaque = true;
            }
        } else {
            // Computed name (Iif/Case/Concat/UDF/parameter expression).
            if (targetHierarchy != null) {
                opaqueHierarchies.add(targetHierarchy);
            } else {
                globallyOpaque = true;
            }
        }
    }

    /**
     * Best-effort inference of the hierarchy that the member-typed
     * argument refers to. Returns null when the expression type does
     * not surface a hierarchy (e.g. an untyped argument or a tuple
     * type — those are handled conservatively as opaque by the
     * caller).
     */
    private Hierarchy inferHierarchy(Exp memberArg) {
        if (memberArg == null) {
            return null;
        }
        Type t = memberArg.getType();
        if (t instanceof MemberType mt) {
            return mt.getHierarchy();
        }
        // Other member-bearing types (HierarchyType, LevelType) also
        // expose getHierarchy(); try the generic accessor.
        try {
            return t == null ? null : t.getHierarchy();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    /**
     * Identifies functions whose return is a runtime-resolved member
     * (or tuple/set) identity. The planner cannot statically follow a
     * {@code StrToMember(...).Properties("X")} pattern back to a
     * specific hierarchy, so any such call in the query is treated as
     * globally opaque.
     */
    private static boolean isOpaqueConstructor(String funcName) {
        return "StrToMember".equalsIgnoreCase(funcName)
            || "StrToTuple".equalsIgnoreCase(funcName)
            || "StrToSet".equalsIgnoreCase(funcName);
    }

    /**
     * Package-private snapshot for unit tests that feed mocked
     * {@link ResolvedFunCall}s directly into {@link #visit(ResolvedFunCall)}
     * without constructing a full {@link Query} AST. Production code
     * must go through {@link #analyzeQuery(Query)}.
     */
    Analysis snapshotForTesting() {
        return toAnalysis();
    }

    private Analysis toAnalysis() {
        // Defensive copies: callers may hold the Analysis past the
        // visitor's lifetime and the visitor's internal maps are
        // mutable.
        Map<Hierarchy, Set<String>> snapshot =
            new HashMap<>(referencesPerHierarchy.size());
        for (Map.Entry<Hierarchy, Set<String>> e
             : referencesPerHierarchy.entrySet())
        {
            snapshot.put(
                e.getKey(),
                Collections.unmodifiableSet(new HashSet<>(e.getValue())));
        }
        return new Analysis(
            Collections.unmodifiableMap(snapshot),
            Collections.unmodifiableSet(
                new HashSet<>(opaqueHierarchies)),
            globallyOpaque);
    }
}

// End PropertiesReferenceVisitor.java
