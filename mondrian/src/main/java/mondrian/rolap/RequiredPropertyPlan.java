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

import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * M3 of the V2 RequiredPropertyProjection (see dronsv/mondrian#22).
 *
 * <p>Per-query plan that tells {@link SqlTupleReader} and
 * {@link SqlMemberSource} which level properties to project into SQL
 * for the current query. The plan is computed once at query execute
 * from:
 * <ul>
 *   <li>engine-required expressions (key / caption / name / ordinal /
 *       parent — always projected, never skipped);</li>
 *   <li>client-response-required properties from MDX
 *       {@code DIMENSION PROPERTIES} per axis;</li>
 *   <li>query-required literal references found by
 *       {@link PropertiesReferenceVisitor} (M2);</li>
 *   <li>schema-required formatter declarations (parsed at schema load,
 *       carried on {@link RolapLevel}; out of scope for the first M3
 *       commit — falls back to eager for any level whose formatter is
 *       not declared);</li>
 * </ul>
 *
 * <p>Anything <strong>not</strong> in the required set is allowed to
 * be skipped from SQL projection. Properties listed as on-demand in
 * the V1-narrow annotation continue to be skipped on top of this
 * (V2 is strictly more aggressive than V1-narrow).
 *
 * <p>Fail-safe to eager: when the {@link PropertiesReferenceVisitor}
 * analysis marks a hierarchy opaque, every level on that hierarchy
 * falls back to projecting all schema-declared properties. The same
 * fallback applies when the V2 feature flag is off — the plan is
 * never computed and SQL sites continue to use the per-level V1-narrow
 * partition (or eager when V1-narrow flag is also off).
 *
 * <p>Lifecycle: bound to a thread for the duration of one query's
 * tuple/member reads via {@link #use}. SQL sites consult
 * {@link #current()}; null means "no V2 plan active for this thread"
 * and the existing per-level projection logic applies.
 *
 * <p>Feature flag: {@code mondrian.rolap.RequiredPropertyProjection}
 * (default false). When false, {@link #compute} returns null and
 * {@link #current()} stays null even if pushed — so off-path code
 * incurs at most a single property read.
 */
public final class RequiredPropertyPlan {

    private static final ThreadLocal<RequiredPropertyPlan> CURRENT =
        new ThreadLocal<>();

    /**
     * Pre-computed projection plans per {@link RolapLevel}. Identity
     * map because RolapLevel does not (and should not) override
     * {@code equals} for projection-plan purposes — two cube-wrapped
     * copies of the same schema level produce distinct projection
     * plans because their inherited hierarchies differ.
     *
     * <p>A missing entry means "no V2 decision for this level"; callers
     * fall back to the per-level V1-narrow / eager logic via
     * {@link RolapLevel#getProjectionPlan()}.
     */
    private final Map<RolapLevel, RolapProperty[]> projectedByLevel;

    private RequiredPropertyPlan(
        Map<RolapLevel, RolapProperty[]> projectedByLevel)
    {
        this.projectedByLevel =
            Collections.unmodifiableMap(projectedByLevel);
    }

    /**
     * Returns the projected property array for the given level under
     * the active V2 plan, or null if the plan has no entry for this
     * level. Callers fall back to {@link RolapLevel#getProjectionPlan()}
     * when this returns null.
     */
    public RolapProperty[] projectedFor(RolapLevel level) {
        return projectedByLevel.get(level);
    }

    /**
     * Returns the V2 plan active on the current thread, or null if
     * none. SQL sites check this to decide between V2 per-query
     * projection and the pre-V2 per-level path.
     */
    public static RequiredPropertyPlan current() {
        return CURRENT.get();
    }

    /**
     * Pushes {@code plan} as the current thread's active V2 plan for
     * the duration of {@code action}. Restores any previously-set plan
     * on exit. No-op when {@code plan} is null.
     */
    public static <T> T use(
        RequiredPropertyPlan plan, java.util.concurrent.Callable<T> action)
        throws Exception
    {
        if (plan == null) {
            return action.call();
        }
        RequiredPropertyPlan prior = pushCurrent(plan);
        try {
            return action.call();
        } finally {
            popCurrent(prior);
        }
    }

    /**
     * Lower-level push for callers (e.g. constructor bodies) that
     * cannot use a {@link java.util.concurrent.Callable} wrapper.
     * Pairs symmetrically with {@link #popCurrent}: the value
     * returned by {@code pushCurrent} must be passed to
     * {@code popCurrent} in a {@code finally} block to restore the
     * prior thread-local state — including when {@code plan} is null
     * (which simply clears the slot for the dynamic extent).
     */
    public static RequiredPropertyPlan pushCurrent(
        RequiredPropertyPlan plan)
    {
        RequiredPropertyPlan prior = CURRENT.get();
        if (plan == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(plan);
        }
        return prior;
    }

    /** Restores the current thread's plan to {@code prior}. */
    public static void popCurrent(RequiredPropertyPlan prior) {
        if (prior == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(prior);
        }
    }

    /**
     * Computes the V2 plan for the given resolved query, or returns
     * null when the feature flag is off (so callers can skip the
     * thread-local push entirely on the fast path).
     *
     * <p>The plan is built by:
     * <ol>
     *   <li>running {@link PropertiesReferenceVisitor} (M2) to collect
     *       per-hierarchy literal references + opacity flags;</li>
     *   <li>walking each {@link QueryAxis} to collect
     *       {@code DIMENSION PROPERTIES} requests;</li>
     *   <li>for every level visible to the query, computing the
     *       required set = engine-required + DIMENSION-PROPERTIES +
     *       AST-literal + (fallback: all-schema if opaque);</li>
     *   <li>materialising the {@code RolapProperty[]} to project.</li>
     * </ol>
     *
     * <p>Step 3 is partial in this commit: the plan is populated only
     * for levels actually mentioned by an MDX literal reference or a
     * DIMENSION PROPERTIES clause. Levels not mentioned default to
     * "no entry" → SQL sites fall back to the V1-narrow per-level
     * partition. This is a deliberate first cut to keep the diff
     * small while landing the framework. Subsequent commits will
     * widen coverage to all hierarchies on the query's axes.
     */
    public static RequiredPropertyPlan compute(Query query) {
        if (!MondrianProperties.instance()
            .RequiredPropertyProjection.get())
        {
            return null;
        }
        if (query == null) {
            return null;
        }
        PropertiesReferenceVisitor.Analysis analysis =
            PropertiesReferenceVisitor.analyzeQuery(query);

        Map<Hierarchy, Set<String>> dimProps =
            collectDimensionProperties(query);

        Map<RolapLevel, RolapProperty[]> projected =
            new IdentityHashMap<>();

        // Combine per-hierarchy literal refs + DIM PROPS, attribute to
        // each level on the hierarchy, build the projected list.
        Set<Hierarchy> touched = new HashSet<>();
        touched.addAll(analysis.referencesPerHierarchy().keySet());
        touched.addAll(dimProps.keySet());

        for (Hierarchy h : touched) {
            if (analysis.isOpaqueFor(h)) {
                // Fail-safe: every level on this hierarchy must be
                // eagerly projected. Leaving the level absent from
                // `projected` triggers exactly that fallback at the
                // SQL site (it calls level.getProjectionPlan()).
                continue;
            }
            Set<String> required = new HashSet<>();
            required.addAll(analysis.referencedOn(h));
            required.addAll(dimProps.getOrDefault(
                h, Collections.emptySet()));
            for (mondrian.olap.Level lvl : h.getLevels()) {
                if (!(lvl instanceof RolapLevel rl)) {
                    continue;
                }
                if (rl.isAll()) {
                    continue;
                }
                RolapProperty[] all = rl.getProperties();
                if (all == null || all.length == 0) {
                    continue;
                }
                java.util.List<RolapProperty> kept =
                    new java.util.ArrayList<>(all.length);
                for (RolapProperty p : all) {
                    if (p == null) {
                        continue;
                    }
                    // V2 keeps property iff it is required by MDX or
                    // by DIM PROPS. Engine-required expressions
                    // (key/name/caption/ordinal/parent) are projected
                    // by separate code paths in the SQL builders —
                    // this plan only governs the <Property> list.
                    if (required.contains(p.getName())) {
                        kept.add(p);
                    }
                }
                // Canonicalise to the schema RolapLevel: SQL sites
                // may have either a cube-wrapped or schema-side level
                // instance depending on the read path. Store the
                // schema-side as the canonical key; the
                // RolapLevel.getEffectiveProjectedProperties() lookup
                // unwraps RolapCubeLevel on its side, so either
                // direction matches.
                RolapLevel canonical = rl instanceof RolapCubeLevel rcl
                    ? rcl.getRolapLevel()
                    : rl;
                if (canonical == null) {
                    canonical = rl;
                }
                projected.put(
                    canonical,
                    kept.toArray(new RolapProperty[0]));
            }
        }

        return new RequiredPropertyPlan(projected);
    }

    /**
     * Iterates the query's axes (rows/columns/etc. + slicer) and
     * collects per-hierarchy DIMENSION PROPERTIES requests. Best
     * effort: a request id like {@code [Dim].[Hier].[Level].[Prop]}
     * is matched against hierarchies on the query's axes; the last
     * segment is taken as the property name.
     */
    private static Map<Hierarchy, Set<String>>
        collectDimensionProperties(Query query)
    {
        Map<Hierarchy, Set<String>> result = new IdentityHashMap<>();
        QueryAxis[] axes = query.getAxes();
        if (axes == null) {
            return result;
        }
        for (QueryAxis ax : axes) {
            if (ax == null) {
                continue;
            }
            Id[] dps = ax.getDimensionProperties();
            if (dps == null || dps.length == 0) {
                continue;
            }
            for (Id id : dps) {
                addDimensionProperty(query, ax, id, result);
            }
        }
        QueryAxis slicer = query.getSlicerAxis();
        if (slicer != null && slicer.getDimensionProperties() != null) {
            for (Id id : slicer.getDimensionProperties()) {
                addDimensionProperty(query, slicer, id, result);
            }
        }
        return result;
    }

    /**
     * Attributes a DIMENSION PROPERTIES Id to a (hierarchy, property
     * name) pair. The id's last segment is the property name; the
     * preceding segments identify hierarchy/level. Best-effort: when
     * the id cannot be resolved to a known hierarchy, the request is
     * dropped silently (the planner falls back to eager for that
     * level via opacity).
     */
    private static void addDimensionProperty(
        Query query, QueryAxis axis, Id id,
        Map<Hierarchy, Set<String>> result)
    {
        if (id == null) {
            return;
        }
        java.util.List<Id.Segment> segs = id.getSegments();
        if (segs == null || segs.isEmpty()) {
            return;
        }
        Id.Segment last = segs.get(segs.size() - 1);
        if (!(last instanceof Id.NameSegment)) {
            return;
        }
        String propName = ((Id.NameSegment) last).getName();
        // Try to attribute to a hierarchy: walk the id's lead
        // segments as a hierarchy/level reference.
        Hierarchy h = resolveHierarchy(query, segs);
        if (h == null || propName == null) {
            return;
        }
        result.computeIfAbsent(h, x -> new HashSet<>()).add(propName);
    }

    /**
     * Resolves the leading segments of a DIM PROPS id to a hierarchy.
     * Returns null when the id does not start with a recognisable
     * dimension/hierarchy name (in which case the planner skips the
     * request — fail-safe to eager).
     */
    private static Hierarchy resolveHierarchy(
        Query query, java.util.List<Id.Segment> segs)
    {
        if (segs.size() < 2) {
            return null;
        }
        // Strip the trailing property segment.
        java.util.List<Id.Segment> hierSegs =
            segs.subList(0, segs.size() - 1);
        try {
            mondrian.olap.OlapElement el = query.getSchemaReader(true)
                .lookupCompound(
                    query.getCube(),
                    hierSegs,
                    false,
                    mondrian.olap.Category.Unknown);
            if (el == null) {
                return null;
            }
            return el.getHierarchy();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * For tests: builds a plan from a pre-constructed map without
     * needing a real query.
     */
    static RequiredPropertyPlan forTesting(
        Map<RolapLevel, RolapProperty[]> projectedByLevel)
    {
        return new RequiredPropertyPlan(
            new HashMap<>(projectedByLevel));
    }
}

// End RequiredPropertyPlan.java
