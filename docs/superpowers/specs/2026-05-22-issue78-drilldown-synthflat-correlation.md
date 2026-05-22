# Source-hierarchy correlation in `DrilldownMember` cross-hierarchy drill

**Status:** **IMPLEMENTED AND LIVE-VERIFIED · 2026-05-22.** Landed on `mondrian/main` through `eff5c9056` (`fix(#78): vet ancestor-property constraint against drill-side level`) and referenced by the superproject at `3c8e9a7` (`deps: bump mondrian to eff5c9056 (#78 fallback fix)`). Issue #78 is closed. The live Excel 4-deep `DrilldownMember` reproducer now completes in 4.32 s / 2836 tuples on image `issue78c-6977d76`; prior images OOMed at 12 GB heap after growing toward the ~17 B-tuple Cartesian.

This document is now an implementation record. The historical proposal text below is preserved because it explains the path and the rejected alternatives; the current code also includes the post-review safety guards added after the first implementation pass:

- `XmlaHandler.isPropertyInternal` hides all `_synth_src_ancestor_` properties from XMLA metadata.
- `SyntheticFlatHierarchy.buildSyntheticLevel` emits ancestor properties only when the source level is unique and the ancestor key column lives on the same table as the synthetic level key.
- `SyntheticFlatHierarchySupport.filterChildrenBySourcePath` vets each candidate constraint against the drill-side level's emitted properties, so non-unique or snowflaked source hierarchies degrade to the old Cartesian-but-correct behavior instead of filtering everything out.
- Focused local unit coverage lives in `FlatHierarchyTest` and `SyntheticFlatHierarchySupportTest`; live acceptance was run against the FitnessShock VM with the real Excel MDX shape.

## Problem

`DrilldownMemberFunDef.drillDownCrossHierarchy` (`mondrian/src/main/java/mondrian/olap/fun/DrilldownMemberFunDef.java:117-151`) emits one new tuple per child returned by `getMemberChildren(tuple[k])`, where `k` is the position of the drill target hierarchy. The choice of children is unconditional on the other positions of the tuple.

When the drill target and one or more other tuple positions hold members of **sibling `SyntheticFlatHierarchy` instances** (i.e., synthetic-flat hierarchies projecting different levels of the *same* source hierarchy), each drill step Cartesianly multiplies into the full level of the next sibling, because synthetic-flat hierarchies do not carry parent-child structure between themselves.

For Excel's "+ expand" pivot over 5 sibling category levels + product leaves (live-captured 2026-05-22), cumulative cardinality grows: 15 → 930 → ~100 K → ~16 M → ~17 B candidate tuples. Materializes inside `-Xmx5g`, OOMs, container becomes unresponsive.

The pure-`Crossjoin` shape from the issue body is **not** affected — `RolapNativeCrossJoin` intercepts it and emits a single multi-column SELECT whose JOIN against `dim_*_product` restricts results to valid paths. `DrilldownMember` has no equivalent native interception path.

## Scope (this proposal)

- **In scope:** restrict the children emitted by `drillDownCrossHierarchy` so that cross-hierarchy drill between sibling synthetic-flat hierarchies respects the source-hierarchy parent-child structure. Cardinality after the fix is bounded by the source hierarchy's parent-child fan-out, not by the level-N cardinality.
- **Out of scope:** any change in `RolapNativeCrossJoin`, in `CrossJoinFunDef`, in `CrossJoinArgFactory`, or in `Crossjoin` AST semantics. Pure-`Crossjoin` shapes already go native and are correct.
- **Out of scope:** changes to `drillDownObj` (the non-cross-hierarchy drill, line 87). It expands `tuple[k]`'s own children, which is correct for non-cross drills.
- **Out of scope:** changes to `DrilldownLevel` / `DrilldownLevelTopBottom`. Those compile to different `FunDef`s and have a separate code path.
- **Out of scope:** changing `getMemberChildren` semantics for synthetic-flat members in isolation. Outside the cross-hierarchy drill context, returning the full level of children is correct.

## Implementation overview (3 prerequisites + the algorithm)

A code review surfaced four hard blockers in earlier drafts of this spec. The actual implementation requires three prerequisite changes before the drill-down algorithm itself can be written. The order matters — each prerequisite is independently testable and shippable.

1. **Public helper class** in `mondrian.rolap`. The existing primitives in `CrossJoinDependencyPruner` (package `mondrian.rolap.sql`) are `private static` or package-private; none are callable from `DrilldownMemberFunDef` (package `mondrian.olap.fun`). Create `public class SyntheticFlatHierarchySupport` in `mondrian.rolap` with `public static` methods, migrate the private helpers to it, update `CrossJoinDependencyPruner` to delegate. Zero behavior change.
2. **Materialize ancestor source-keys as synthetic-flat member properties.** Today, `SyntheticFlatHierarchy.buildSyntheticLevel` sets `lvl.properties = new MondrianDef.Property[0]` (line 216) — synthetic-flat members **do not** carry their source-hierarchy ancestor identities. The drill-down filter cannot work without this; without it, `member.getPropertyValue("Категория2")` returns null for every Категория3 member, no filter ever fires. Modify `buildSyntheticLevel` (or a sibling method) to emit `MondrianDef.Property` entries for each source-hierarchy ancestor of the synthetic level. Properties get populated at member-load time by the existing `SqlMemberSource` machinery (it adds the property's `column` to the SELECT).
3. **Drill-down filter in `DrilldownMemberFunDef.drillDownCrossHierarchy`.** Uses (1) and (2). Per-child filtering via `child.getPropertyValue(ancestorPropertyName)`, with tolerant key comparison (Integer/Long/String differences for the same numeric ID).

### Primitives the helper class exposes

Current locations in `CrossJoinDependencyPruner.java`; after extraction these become `SyntheticFlatHierarchySupport.<name>`:

| Method | Source line | Role |
|---|---|---|
| `resolveSyntheticFlat(Hierarchy)` | 264 | Unwraps `RolapCubeHierarchy → RolapHierarchy → SyntheticFlatHierarchy`. Returns null otherwise. Handles the wrapper `RolapCubeLevel.getHierarchy()` returns at runtime. |
| `findCommonSourceLink(RolapLevel dep, RolapLevel det)` | 233 | Iterates ALL `SourceLink`s on both sides; returns the dependent-side link if there is a shared source hierarchy where dependent's depth exceeds determinant's depth. Returns null otherwise. |
| (helper) `filterByPropertyKey(List<RolapMember>, String propertyName, Object requiredKey)` | NEW | Per-child filter: keeps each member whose `getPropertyValue(propertyName)` equals `requiredKey` under tolerant comparison (`equalsTolerant` below). Replaces the misuse of `collectPropertyKeys` (which returns a set and cannot do per-child correlation). |
| (helper) `equalsTolerant(Object, Object)` | NEW | Numeric-type-tolerant equality. Integer(42), Long(42L), String("42") all compare equal. Necessary because synthetic-flat keys arrive as the JDBC-driver-decided type (often Integer or Long for numeric IDs), but member properties are read as whatever the property's column declared (sometimes String). Use `Util.equals(Number.toString(...))` or numeric-comparison fallback; existing `Util.equals` in `mondrian.olap.Util` is the place to add the comparator. |

NOT used by this fix:

- `collectAncestorKeys(List<RolapMember>, RolapLevel)` (`CrossJoinDependencyPruner:279`). Walks `getParentMember()` to reach `determinantLevel`. For synthetic-flat children, `getParentMember() == [All]`, so the walk never reaches a source-hierarchy ancestor and returns null. Earlier drafts of this spec proposed using it; that proposal was a silent no-op.
- `collectPropertyKeys(List<RolapMember>, String)` (`CrossJoinDependencyPruner:300`). Returns a *set of all property values across all members* — it cannot do per-child correlation (which child has which key). We need per-child filtering, not bulk set extraction. The existing call site in `CrossJoinDependencyPruner.deriveDeterminantKeys` uses it correctly for a *different* purpose (gathering all determinant keys to constrain the dependent SQL); the drill-down filter case is fundamentally per-child.

### Where ancestor source-keys come from (prerequisite 2)

A `SyntheticFlatHierarchy` for source-level depth N (e.g. `Категория3`) is constructed with a single `SourceLink` to its source level. To carry ancestor identities, the synthetic level must expose **member properties** at construction time, one per source-hierarchy ancestor depth (1..N-1).

Modify `SyntheticFlatHierarchy.buildSyntheticLevel` to also populate `lvl.properties` with one `MondrianDef.Property` per ancestor:

```java
final List<MondrianDef.Property> ancestorProps = new ArrayList<>();
RolapLevel ancestor = sourceLevel.getParentLevel();
while (ancestor != null && !ancestor.isAll()) {
    // The ancestor source level's key column is what we want as
    // a member property on the synthetic flat level.
    final MondrianDef.Expression ancestorKeyExp = ancestor.getKeyExp();
    if (ancestorKeyExp instanceof MondrianDef.Column ancestorCol) {
        MondrianDef.Property p = new MondrianDef.Property();
        p.name = ANCESTOR_PROPERTY_PREFIX + ancestor.getName();
        p.column = ancestorCol.name;
        // Don't set p.type — SqlMemberSource derives it from the column.
        p.dependsOnLevelValue = true; // safe: ancestor is a function of the level value
        ancestorProps.add(p);
    }
    ancestor = ancestor.getParentLevel();
}
lvl.properties = ancestorProps.toArray(new MondrianDef.Property[0]);
```

with a constant `public static final String ANCESTOR_PROPERTY_PREFIX = "_synth_src_ancestor_"` (or similar; the prefix avoids collision with user-defined properties).

At member-load time, the existing `SqlMemberSource.makeChildMemberSql` machinery already projects every level property's column. The new ancestor properties get included in the SELECT for free — no SqlMemberSource changes needed. Each loaded flat member now carries `member.getPropertyValue("_synth_src_ancestor_Категория2")` = the source-level-Категория2 key for that flat member.

### How the drill-down filter uses these

For a candidate Категория3 child being emitted in a tuple where a sibling position is `[Категория2].[Сладости]`:

- `siblingSF = resolveSyntheticFlat(tuple[j].getHierarchy())` → the Категория2 synthetic-flat.
- Iterate `siblingSF.getSourceLinks()`; find `detLink` where `detLink.hierarchy() == drillSF.getSourceHierarchy()`.
- `depLink = drillSF.findLinkForHierarchy(detLink.hierarchy())`. Verify `depLink.depth() > detLink.depth()` (sibling is ancestor in source).
- Property name to read on each candidate child: `ANCESTOR_PROPERTY_PREFIX + detLink.level().getName()`.
- Required key: `tuple[j].getKey()`.
- Per-child filter: keep child iff `equalsTolerant(child.getPropertyValue(propertyName), requiredKey)`.

## Design

### Where the fix lands

A single new code path inside `drillDownCrossHierarchy` (`mondrian/src/main/java/mondrian/olap/fun/DrilldownMemberFunDef.java:117-151`). Existing signature:

```java
protected void drillDownCrossHierarchy(
    Evaluator evaluator,
    Member[] tuple,
    Set<Member> memberSet,
    Hierarchy drillHierarchy,
    TupleList resultList)
```

The existing code:

```java
// existing — emits all children unconditionally
for (int k = 0; k < tuple.length; k++) {
    if (tuple[k].getHierarchy().getUniqueName()
        .equals(drillHierarchy.getUniqueName())) {
        List<Member> children =
            evaluator.getSchemaReader().getMemberChildren(tuple[k]);
        // … emit one tuple per child
    }
}
```

is augmented with a pre-step that, for sibling synthetic-flat hierarchies, computes an "allowed source-hierarchy key set" derived from the OTHER tuple positions, then filters the emitted children to only those whose source-hierarchy ancestor at the appropriate depth is in that set.

### Precise correlation rule

Given:

- `tuple = [m_0, m_1, …, m_{n-1}]` — the tuple being drilled (`n` = tuple arity)
- `k` — the position whose member belongs to `drillHierarchy`
- `m_k` — `tuple[k]`, the member to be expanded

**Step 1 (early exit on non-synthetic-flat drill target).** Let `drillSF = SyntheticFlatHierarchySupport.resolveSyntheticFlat(drillHierarchy)`. If `drillSF == null`, the drill target is not a synthetic-flat → emit all children of `m_k` unchanged (existing behavior).

**Step 2 (scan sibling positions for constraints).** Build `constraints: List<Constraint>` where `Constraint = (String propertyName, Object requiredKey)`. For each tuple position `j ≠ k`:

a. **Skip [All] siblings explicitly.** If `tuple[j].isAll()`, the position imposes no parent-child constraint. Continue to next `j`.

b. **Resolve sibling's synthetic-flat hierarchy.** Let `siblingSF = SyntheticFlatHierarchySupport.resolveSyntheticFlat(tuple[j].getHierarchy())`. If `null` (regular hierarchy, measure, time, etc.), continue.

c. **Find a shared source hierarchy via multi-link iteration.** For each `detLink` in `siblingSF.getSourceLinks()`:

   - `depLink = drillSF.findLinkForHierarchy(detLink.hierarchy())`. If `null`, the drill side has no link to this source hierarchy — try the next `detLink`.
   - If `depLink.depth() <= detLink.depth()`, the sibling does **not** live at an ancestor depth relative to the drill target in this shared source hierarchy — try the next `detLink`. (We need the sibling to be an *ancestor*, not a peer or descendant.)
   - Otherwise: matching link pair found. The property name to look up on candidate children is `ANCESTOR_PROPERTY_PREFIX + detLink.level().getName()` — using `SourceLink.level()` (the record's accessor; there is no `sourceLevel()`).
   - The required key is `tuple[j].getKey()` — a synthetic-flat member's key IS its source-level key by construction.
   - `constraints.add(new Constraint(propertyName, tuple[j].getKey()))`, then `break` out of the per-`detLink` loop (one matching source hierarchy per sibling is enough).

d. After scanning all `j`, if `constraints` is empty, emit all children of `m_k` unchanged (no sibling synthetic-flats present → no correlation to apply).

**Step 3 (filter children).** Otherwise:

- Fetch candidate children: `children = evaluator.getSchemaReader().getMemberChildren(m_k)`. Returns schema-level children — NOT NON-EMPTY filtered. NON EMPTY runs downstream in `tryPruneExpandedDrilldownMember`.
- For each child, evaluate **all** constraints: keep the child only if every `equalsTolerant(child.getPropertyValue(c.propertyName), c.requiredKey)` is true.
- This is per-child, single-pass. Pseudocode:

```java
final List<Member> filtered = new ArrayList<>(children.size());
outer: for (Member child : children) {
    for (Constraint c : constraints) {
        Object actual = child.getPropertyValue(c.propertyName);
        if (!SyntheticFlatHierarchySupport.equalsTolerant(actual, c.requiredKey)) {
            continue outer;
        }
    }
    filtered.add(child);
}
// emit one tuple per filtered member
```

- **Do NOT use `collectPropertyKeys`** (it returns a `Set<Object>` of all values across all members — loses per-child identity and cannot correlate one child to its own ancestor key).
- **Do NOT use `collectAncestorKeys`** (it walks `getParentMember()` and fails for synthetic-flat children whose parent is `[All]`).

**Tolerant key comparison.** Numeric IDs may be Integer in one column and Long in another, depending on how the JDBC driver interprets the dim-table schema vs the flat-level config. `equalsTolerant`:

```java
static boolean equalsTolerant(Object a, Object b) {
    if (a == b) return true;
    if (a == null || b == null) return false;
    if (a.equals(b)) return true;
    // Numeric/string tolerance: compare canonicalized string forms.
    if (a instanceof Number || b instanceof Number) {
        return canonicalNumberString(a).equals(canonicalNumberString(b));
    }
    return false;
}
static String canonicalNumberString(Object o) {
    if (o instanceof Number n) {
        // Strip trailing zeros; use longValue when exact, else toString.
        if (n.doubleValue() == n.longValue()) {
            return Long.toString(n.longValue());
        }
        return n.toString();
    }
    return o.toString();
}
```

The implementation should locate this in `SyntheticFlatHierarchySupport`, not in `Util` — it is a tightly-scoped helper for the synthetic-flat key-matching case, not a general-purpose `equals` replacement.

### Why this rule is correct

Two correctness anchors:

1. **When no sibling synthetic-flat positions exist in the tuple**, the rule produces the same children as today — the `requiredAncestors` map is empty and the unfiltered `children` list is emitted. Backward-compatible.
2. **When all involved synthetic-flat siblings share a common source hierarchy** (the #78 case), each drilled tuple corresponds to a unique source-hierarchy parent-child path. Children whose source-level ancestor doesn't match the sibling's source-level position cannot belong to a valid path and would have been NON-EMPTY-pruned later anyway. The filter is an upfront prune, not a semantic change.

Tuples that today already satisfy the source-path constraint remain in the output. Tuples that today are emitted but pruned later (or, in the OOM scenario, materialized and never pruned due to heap exhaustion) are simply not emitted.

### Cardinality consequence (illustrative)

After the fix, drill output cardinality is bounded by the source hierarchy's actual parent-child fan-out at each step, not by the full sibling level.

The table below illustrates the qualitative scale change with placeholder bounds for a hypothetical FitnessShock-shaped cube. **Actual post-fix numbers will depend on the live source-hierarchy fan-out, not these estimates** — the table is for order-of-magnitude reasoning only. Verification will measure actual counts.

| After step | Pre-fix tuples | Post-fix tuples (bounded by source fan-out) |
|---|---|---|
| Initial Crossjoin | 15 | 15 |
| Drill → Category3 | ~930 | ≤ avg fan-out of Category2 → Category3 per source path |
| Drill → Category4 | ~100 K | ≤ … (source fan-out at Cat3 → Cat4) |
| Drill → Category5 | ~16 M | ≤ … |
| Drill → Product | ~17 B | ≤ source-valid `(cat2..5, product)` paths |

Final outer-axis cardinality after `NON EMPTY` ≤ valid `(cat1, cat2, cat3, cat4, cat5, sku)` paths in the source hierarchy. No 17B intermediate set; no OOM.

### Edge cases

| Case | Handling |
|---|---|
| Drill target is not a synthetic-flat | Step 1 early exit: `resolveSyntheticFlat(drillHierarchy) == null` → emit all children (existing behavior). |
| Drill target is synthetic-flat, no other position is sibling-source synthetic-flat | `constraints` ends empty (step 2 inner loops all `continue`) → step 2d emits all children. |
| Sibling position is at `[All]` | **Explicit `isAll()` guard at step 2a.** The position contributes no constraint. Correct: `[All]` imposes no parent. The earlier draft relied on depth arithmetic across the [All] pseudo-level — the explicit guard is simpler and safer. |
| Tuple contains both real-hierarchy and synthetic-flat positions referring to the same source dimension | `resolveSyntheticFlat` of the real hierarchy returns null → that `j` is skipped. Synthetic-flat siblings still constrain each other. Real-hierarchy correlation is left to the existing native CJ path. |
| Two different source hierarchies (e.g., `[Product.Category]` siblings + `[Store.Geography]` siblings in the same tuple) | Each sibling's source-link iteration only matches `detLink`s for hierarchies the drill side also links to. Independent constraint sets — no cross-contamination. |
| Multi-source-link synthetic-flat (one flat hierarchy links to two source hierarchies) | Step 2c iterates `siblingSF.getSourceLinks()` and breaks on the first match. If multiple links match (rare), the first one wins. The pruner's `findCommonSourceLink` uses the same first-match-wins shape. |
| Drill target's `SourceLink` doesn't include the sibling's source hierarchy | Step 2c's `findLinkForHierarchy` returns null → that `j` contributes no constraint. Other sibling positions still apply. |
| Sibling synthetic-flat is at a *deeper* level than the drill target in the shared source hierarchy (`depLink.depth() <= detLink.depth()`) | Step 2c's ancestor-depth check skips this `detLink` (the sibling is a peer/descendant, not an ancestor). Try the next link; if no link satisfies, this `j` contributes no constraint. |
| `evaluator.getSchemaReader()` returns a restricted reader (role-aware) | The post-filter applies on top of the role-restricted children list — security/visibility preserved. |
| Calculated members in the tuple | `collectPropertyKeys` returns null for any calculated member encountered. Treat as no constraint from that constraint entry — the calculated case falls back to today's (Cartesian-emitting) behavior, preserving existing semantics. |
| `getMemberChildren(m_k)` returns NON-EMPTY-filtered children | **It does not.** The call passes no constraint context; the children list is schema-level. NON-EMPTY pruning happens downstream in `tryPruneExpandedDrilldownMember`. The new filter operates on the schema-level list, which is correct because the source-correlation invariant is a *structural* invariant (parent-child in the source hierarchy), independent of NON-EMPTY measure context. |
| `DrilldownMember` invoked with `RECURSIVE` keyword | `RECURSIVE` only routes through `drillDownObj` (line 102-105 of `DrilldownMemberFunDef.java`), not `drillDownCrossHierarchy` — `drillHierarchy != null` (line 173) is the dispatcher. Cross-hierarchy drill is never recursive by construction; the spec's filter doesn't need a recursive variant. |

### Risks

- **Correctness regression on non-synthetic-flat cubes.** Mitigated by the early `resolveSyntheticFlat == null` exit and by the empty-`requiredAncestors` short-circuit. Cubes without any synthetic-flat hierarchies hit zero new code per drill.
- **Subtle bug in source-projection logic.** The "project to source" step (the new code) must correctly map a synthetic-flat member to its source-level member via key. `SyntheticFlatHierarchy` already exposes this — concrete API: see implementation notes — but the fix must use the existing accessor, not roll a new one.
- **Calculated members.** `collectAncestorKeys` returns null for any calculated member in the input list. The fix must skip the corresponding `j` (treat as no constraint) rather than throwing.
- **Performance of the filter itself.** The filter calls `getMemberChildren(m_k)` once (same as today) and then walks ancestors per child. Source-hierarchy parent walks are O(depth) and cached by `RolapMember.getParentMember()`. Total added cost is bounded by `children.size() × max_depth` — negligible compared to the cartesian today.
- **Empty post-filter result.** If `requiredAncestors` rules out every candidate child for a tuple, that tuple's drill step adds no new tuples (the input tuple itself was already added). Semantically correct.
- **Interaction with `tryPruneExpandedDrilldownMember`** (`DrilldownMemberFunDef:186`). The existing post-pruner runs after the drill; it remains correct because the filter only removes tuples that would not have survived NON-EMPTY pruning anyway.

## Verification

### Unit tests (no DB)

Add to `mondrian/src/test/java/mondrian/olap/fun/DrilldownMemberFunDefTest.java` (or a new test class if simpler):

1. **`testDrillDownCrossHierarchyEmitsAllChildrenWhenNoSyntheticFlat`** — two regular hierarchies, drill target is one of them, no synthetic-flat involvement. Assert the post-fix behavior is identical to pre-fix (count + identity of emitted tuples).

2. **`testDrillDownCrossHierarchySiblingSynthFlatCorrelation`** — fixture with two synthetic-flat hierarchies sharing a source. Construct an input tuple at the [All] of one sibling and a specific source-level member of the other. Drill the [All] target. Assert the emitted children are exactly those whose source-hierarchy ancestor at the sibling's source level equals the sibling's projected member. Use mocked `SyntheticFlatHierarchy` instances; depend on `resolveSyntheticFlat` / `findCommonSourceLink` / `collectAncestorKeys` to drive the assertion.

3. **`testDrillDownCrossHierarchyMultiSourceLink`** — synthetic-flat hierarchy with two source links; drill across one of them. Asserts `findCommonSourceLink` finds the correct link and the filter uses it.

4. **`testDrillDownCrossHierarchyAllMemberSibling`** — sibling position is at [All] of its synthetic-flat. Filter must not over-restrict; all candidate children should pass.

### IT tests (DB required)

Add to `mondrian/src/it/java/mondrian/olap/fun/DrilldownMemberFunDefIT.java`:

5. **`testExcelDrilldownMemberChainOverSyntheticFlatSiblings`** — exercise the real Excel shape (nested `Crossjoin(Hierarchize(DrilldownLevel(...)), Hierarchize(DrilldownMember(DrilldownMember(...))))`) against a fixture schema with at least three synthetic-flat sibling levels of one source hierarchy. Assert the row count equals the count of valid source-hierarchy paths (NOT the Cartesian). Wall-time budget: ≤ 5 s.

6. **`testDrilldownMemberDoesNotRegressNonSyntheticFlatShape`** — same MDX shape against a regular (non-synthetic-flat) hierarchy. Row count and tuple identity must be byte-identical pre- and post-fix. Locks in the no-regression guarantee.

### Live verification (VM)

7. After building a new image (`issue78b-<sha>`), deploy to the FitnessShock VM in place of `issue77d-5503ca7` (preserving prior as `*-prev` for rollback per the #77 deploy pattern). Run:

```mdx
SELECT {[Measures].[Продажи руб]} ON COLUMNS,
       NON EMPTY
         Crossjoin(
           Hierarchize(DrilldownLevel({[Продукт.Категория1].[All Категория1]}, , , INCLUDE_CALC_MEMBERS)),
           Hierarchize(
             DrilldownMember(
               DrilldownMember(
                 DrilldownMember(
                   DrilldownMember(
                     Crossjoin(
                       {[Продукт.Категория2].[All Категория2], [Продукт.Категория2].[Категория2].AllMembers},
                       {([Продукт.Категория3].[All Категория3], [Продукт.Категория4].[All Категория4],
                         [Продукт.Категория5].[All Категория5], [Продукт.Товар].[All Товар])}),
                     [Продукт.Категория2].[Категория2].AllMembers, [Продукт.Категория3]),
                   [Продукт.Категория3].[Категория3].AllMembers, [Продукт.Категория4]),
                 [Продукт.Категория4].[Категория4].AllMembers, [Продукт.Категория5]),
               [Продукт.Категория5].[Категория5].AllMembers, [Продукт.Товар])))
       ON ROWS
FROM [FitnessShock]
```

Target: completes in ≤ 5 s, row count = (valid `(Категория2..Категория5, Товар)` source-hierarchy paths in FS). Pre-fix on `issue77d-5503ca7`: OOM at `-Xmx5g`, container unresponsive. Live evidence already captured 2026-05-22 11:12; the post-fix run should land in the same wall-time band as the pure-`Crossjoin` shape (0.3 s) plus the DrilldownMember-tree walk overhead.

### Regression guard

Run the existing test suites + acceptance packs:

- `mondrian/src/test/java/mondrian/olap/fun/DrilldownMemberFunDefNativeNonEmptyFilterTest.java` — existing IT exercises `DrilldownMember` + `tryPruneExpandedDrilldownMember`. The new filter runs **before** that pruner; the existing test cases must remain green (filter emits a strict subset of what the unfiltered drill would produce, pruner's input is at most as large as today, never larger).
- `scripts/run_issue77_regression.sh` (4 queries, FitnessShock) — must remain green.
- `scripts/run_fitnessshock_preflight.sh` (#71/#72 shapes) — must remain green.
- `scripts/run_excel_product_analysis_pack.sh` (52 queries, Konfet) — must remain green against the dev stack.

## Implementation notes

- **Step order in the implementer's TDD.** Visibility/extraction step first (`SyntheticFlatHierarchySupport` helper or in-place promotion), then the algorithm change in `drillDownCrossHierarchy`. Commit the visibility change before any drill-down logic so reviewers can audit the surface area independently.
- **`tuple[j].getKey()` is the synthetic-flat source-key.** No new accessor is needed on `SyntheticFlatHierarchy`. This was Open Question 1 in an earlier draft; the answer comes from how `SyntheticFlatHierarchy:addSourceLink` constructs flat members — the flat level's key column is the source level's key column.
- **`collectPropertyKeys` not `collectAncestorKeys`.** For synthetic-flat children, `getParentMember()` is `[All]`, not a source-hierarchy ancestor. The ancestor identity at each source-hierarchy level is exposed as a member property named after the source level. `CrossJoinDependencyPruner.deriveDeterminantKeys` (lines 167-196) demonstrates this pattern; the fix mirrors it.
- **`isAll()` guard is explicit, not depth-based.** Step 2a tests `tuple[j].isAll()` directly. The earlier draft relied on `depLink.depth() <= detLink.depth()` arithmetic across the [All] pseudo-level, which is brittle. Explicit guard is simpler and matches reader intuition.
- **`getMemberChildren(m_k)` returns schema-level children.** The call passes no constraint context (`RolapSchemaReader.getMemberChildren(Member)` → `getMemberChildren(member, null)` → `SqlConstraintFactory.getMemberChildrenConstraint(null)` falls back to `DefaultMemberChildrenConstraint`). NON-EMPTY pruning happens downstream in `tryPruneExpandedDrilldownMember`. The fix's filter operates on the schema-level list, which is the correct input for source-path correlation (a structural invariant independent of measure context).
- **Cost.** Each drilled tuple does: O(arity) sibling scan + (if any constraints) one `getMemberChildren` call + O(children.size() × constraints.size()) property lookups + O(survivors) tuple emits. The current code does O(arity) scan + O(children.size()) emits. New cost dominated by the property lookups, which are cached on `RolapMember`. For the FS shape (~1071 children, ≤5 constraints per drill), that is ~5K property reads per drilled tuple — negligible vs the 17B Cartesian we are avoiding.

## Open questions

1. Should the same correlation rule be added to `drillDownObj` (the non-cross drill at line 87) for symmetry? Tentatively: no — `drillDownObj` expands `tuple[k]`'s own children (parent-restricted by construction), so the bug doesn't surface there. The `RECURSIVE` keyword only routes through `drillDownObj`, never through `drillDownCrossHierarchy`. Verify with a unit test before deciding.
2. The post-fix filter runs unconditionally; `tryPruneExpandedDrilldownMember` (`DrilldownMemberFunDef:186`) is gated on `evaluator.isNonEmpty()` and `NativeNonEmptyFilterEnable`. Should the new filter share the same gating? Tentatively: unconditional is correct — the filter never removes a tuple that would survive a correct semantic evaluation; it only avoids materializing structurally invalid tuples. Confirm by running the IT both with and without NON EMPTY.
3. Does adding ancestor properties to synthetic-flat levels (prerequisite 2) break any existing #71-era tests that assume `lvl.properties.length == 0`? Existing test surface: `FlatHierarchyTest`, `NativeQueryEngineEligibilityTest`, `ExplicitRecognizerAliasMatchTest`, `NativeNonEmptyFilterTest`. Run all four after the prerequisite-2 commit; expect green (the test assertions are about the synthetic flat *concept*, not about property arrays being empty).
4. Naming convention for ancestor properties: `_synth_src_ancestor_<LevelName>` is one option; alternatives include `__internal_ancestor_<LevelName>` or borrowing the source level's unique name. The prefix should be unlikely to collide with user-declared properties and should never surface in `XMLA MEMBER_PROPERTIES`. Implementer can pick — the prefix is the only knob; the suffix is determined by `RolapLevel.getName()`.

## Cross-references

- Investigation log: `docs/superpowers/specs/2026-05-22-issue78-flat-crossjoin-fold.md` (REJECTED, do not implement).
- Issue thread: https://github.com/dronsv/emondrian-clickhouse/issues/78
- Referenced primitive: `mondrian/src/main/java/mondrian/rolap/sql/CrossJoinDependencyPruner.java` lines 233 (`findCommonSourceLink`), 264 (`resolveSyntheticFlat`), 279 (`collectAncestorKeys`), 321 (`filterMembersByKey`).
- Target code: `mondrian/src/main/java/mondrian/olap/fun/DrilldownMemberFunDef.java` lines 117-151 (`drillDownCrossHierarchy`).
- Memory: `[[issue71-state]]`, `[[issue45-state]]` for the chronology of synthetic-flat fixes.
