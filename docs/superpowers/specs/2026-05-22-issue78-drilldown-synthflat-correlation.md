# Source-hierarchy correlation in `DrilldownMember` cross-hierarchy drill

**Status:** proposal · 2026-05-22 · addresses emondrian-clickhouse#78 (live-captured reproducer, see investigation log in `2026-05-22-issue78-flat-crossjoin-fold.md`)

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

## Background: building blocks in the codebase

`mondrian/src/main/java/mondrian/rolap/sql/CrossJoinDependencyPruner.java` has the right shape of helpers but they are **not directly callable** — every method we need is either `private static` or package-private inside `mondrian.rolap.sql`, and the fix lives in `mondrian.olap.fun`. **Step 1 of the implementation is a visibility / extraction step before any drill-down logic is written.**

Two equivalent options for the visibility step (pick one in implementation; functional contract is identical):

- **Option A (preferred): extract a shared utility class.** Create `mondrian.rolap.SyntheticFlatHierarchySupport` (package `mondrian.rolap`, package-private — callable from `mondrian.olap.fun` via an explicit `public` API) that exposes the five primitives below. Migrate `CrossJoinDependencyPruner`'s private helpers to delegate to it. Net: zero behavior change at the pruner; one new utility class.
- **Option B (smaller change): promote visibility in-place.** Change the four `private static` methods in `CrossJoinDependencyPruner` to `public static` (or move them to a sibling class in the same package and re-export). Acceptable but couples `mondrian.olap.fun` to `mondrian.rolap.sql`, which the project currently avoids.

The shared primitives the fix needs (current locations in `CrossJoinDependencyPruner.java`):

1. `resolveSyntheticFlat(Hierarchy)` (line 264) — unwraps `RolapCubeHierarchy → RolapHierarchy → SyntheticFlatHierarchy`. Returns null otherwise. Handles the wrapper `RolapCubeLevel.getHierarchy()` returns.
2. `findCommonSourceLink(RolapLevel dependentLevel, RolapLevel determinantLevel)` (line 233) — iterates **all** `SourceLink`s on both sides and looks for a pair `(detLink, depLink)` such that they reference the same source hierarchy and `depLink.depth() > detLink.depth()`. Returns the dependent-side `SourceLink` or null.
3. `collectPropertyKeys(List<RolapMember>, String propertyName)` (line 300) — for each member, reads the named property and accumulates non-null values into a set. **This is the right primitive for synthetic-flat children**, because synthetic-flat members store their source-hierarchy ancestor identities as member properties named after the source levels — they cannot be walked via `getParentMember()` (their parent is `[All]`, not a source-hierarchy ancestor).
4. `filterMembersByKey(List<RolapMember>, Set<Object>)` (line 321) — filters by an allowed key set.

Not used by this fix (despite being adjacent in `CrossJoinDependencyPruner`):

- `collectAncestorKeys(List<RolapMember>, RolapLevel)` (line 279). This primitive walks `getParentMember()` until it reaches `determinantLevel`. For **synthetic-flat children, `getParentMember()` returns the `[All]` pseudo-member, not a source-hierarchy ancestor.** The method returns `null` for every flat child, which the spec's earlier draft would have treated as "no constraint" — collapsing the fix to a no-op. `CrossJoinDependencyPruner.deriveDeterminantKeys` (lines 167-196) already documents this: it uses `collectPropertyKeys` (not `collectAncestorKeys`) for synthetic-flat-determinant cases.

## How synthetic-flat members project to the source hierarchy

A `SyntheticFlatHierarchy` level has a 1:1 mapping to its source-level column. **A flat member's own `getKey()` IS the source-level key** — no separate `projectToSource(Member)` accessor is needed (the earlier draft's Open Question 1 was a phantom).

For a flat member at the *sibling* tuple position, the value used to constrain candidate children is `tuple[j].getKey()` directly.

For a *candidate child* at the drill-output position, the value to *check against* the sibling key is the child's **member property named after the sibling's source level** (e.g. `category_l2_id`). The synthetic-flat construction at `SyntheticFlatHierarchy:addSourceLink` arranges for these properties to be exposed; reading them is what `collectPropertyKeys` does.

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

**Step 1 (early exit on non-synthetic-flat drill target).** Let `drillSF = resolveSyntheticFlat(drillHierarchy)`. If `drillSF == null`, the drill target is not a synthetic-flat → existing behavior unchanged, emit all children of `m_k` and return.

**Step 2 (scan sibling positions for constraints).** Iterate `constraints: Map<String /* sibling sourceLevelName */, Object /* sibling key */>`. For each tuple position `j ≠ k`:

a. **Skip [All] siblings explicitly.** If `tuple[j].isAll()`, the position imposes no parent-child constraint (the [All] member covers every child by definition). Continue to next `j`.

b. **Resolve sibling's synthetic-flat hierarchy.** Let `siblingSF = resolveSyntheticFlat(tuple[j].getHierarchy())`. If `null` (regular hierarchy, measure, time, etc.), continue to next `j`.

c. **Find a shared source hierarchy via multi-link iteration.** For each `detLink` in `siblingSF.getSourceLinks()`:

   - `depLink = drillSF.findLinkForHierarchy(detLink.hierarchy())`. If `null`, the drill side has no link to this source hierarchy — try the next `detLink`.
   - If `depLink.depth() <= detLink.depth()`, the sibling does **not** live at an ancestor depth relative to the drill target in this shared source hierarchy — try the next `detLink`. (We need the sibling to be an *ancestor* of the drill, not a peer or descendant.)
   - Otherwise we have a valid (`detLink`, `depLink`) pair. Record a constraint: `constraints.put(detLink.sourceLevel().getName(), tuple[j].getKey())`.
     - **No `projectToSource()` call.** A synthetic-flat member's `getKey()` IS its source-level key by construction (the flat level's column directly maps the source column). No new accessor is needed on `SyntheticFlatHierarchy`.
   - `break` out of the per-`detLink` loop (one matching source hierarchy per sibling is enough).

d. After scanning all `j`, if `constraints` is empty, emit all children of `m_k` unchanged (no synthetic-flat siblings present → no correlation to apply).

**Step 3 (filter children).** Otherwise, fetch the candidate children: `children = evaluator.getSchemaReader().getMemberChildren(m_k)`. Returns schema-level children of `m_k` (NOT NON-EMPTY filtered — `tryPruneExpandedDrilldownMember` applies NON EMPTY downstream). For synthetic-flat `m_k`, this is the full member list of the drill hierarchy's leaf level.

For each constraint entry `(sourceLevelName, requiredKey)`:

- Use `collectPropertyKeys(children, sourceLevelName)` — same primitive `CrossJoinDependencyPruner.deriveDeterminantKeys` uses for the synthetic-flat case. This reads each child's member property named after the source level (e.g. `client_category_l2_id`) — the source-hierarchy ancestor identity for that depth, exposed by `SyntheticFlatHierarchy:addSourceLink` at member-construction time.
- A child survives the constraint iff its property value at `sourceLevelName` equals `requiredKey`.

Apply all constraints conjunctively (a child must satisfy every constraint to be emitted). The pattern matches `CrossJoinDependencyPruner.filterMembersByKey` extended to a per-property check; the implementation can either build successive allowed-key sets (one per constraint) and intersect, or check each child against the full constraint map in a single pass — both are O(`children.size() × constraints.size()`).

Emit one tuple per surviving child.

**Note on `collectAncestorKeys` (line 279 of CrossJoinDependencyPruner).** Do **not** use it here. It walks `getParentMember()` chains to find a member at a specified `RolapLevel`. For synthetic-flat members, `getParentMember()` returns the `[All]` pseudo-member — not a source-hierarchy ancestor. The primitive returns null for every flat child, which would silently disable the fix (zero constraints applied, OOM continues). This was confirmed by code review and verified by reading the primitive's implementation.

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

1. Should the same correlation rule be added to `drillDownObj` (the non-cross drill at line 87) for symmetry? Tentatively: no — `drillDownObj` expands `tuple[k]`'s own children (parent-restricted by construction), so the bug doesn't surface there. The `RECURSIVE` keyword only routes through `drillDownObj`, never through `drillDownCrossHierarchy` (see edge-case row). Verify with a unit test before deciding.
2. The post-fix filter runs unconditionally; `tryPruneExpandedDrilldownMember` (`DrilldownMemberFunDef:186`) is gated on `evaluator.isNonEmpty()` and `NativeNonEmptyFilterEnable`. Should the new filter share the same gating? Tentatively: unconditional is correct — the filter never removes a tuple that would survive a correct semantic evaluation; it only avoids materializing tuples that are structurally invalid in the source hierarchy. But confirm by running the IT both with and without NON EMPTY.
3. Option A vs Option B for the visibility step (extract helper vs in-place promote): implementer's call. The spec doesn't mandate one. Code review at PR time can push back if the chosen option crosses a boundary the project tries to keep clean.

## Cross-references

- Investigation log: `docs/superpowers/specs/2026-05-22-issue78-flat-crossjoin-fold.md` (REJECTED, do not implement).
- Issue thread: https://github.com/dronsv/emondrian-clickhouse/issues/78
- Referenced primitive: `mondrian/src/main/java/mondrian/rolap/sql/CrossJoinDependencyPruner.java` lines 233 (`findCommonSourceLink`), 264 (`resolveSyntheticFlat`), 279 (`collectAncestorKeys`), 321 (`filterMembersByKey`).
- Target code: `mondrian/src/main/java/mondrian/olap/fun/DrilldownMemberFunDef.java` lines 117-151 (`drillDownCrossHierarchy`).
- Memory: `[[issue71-state]]`, `[[issue45-state]]` for the chronology of synthetic-flat fixes.
