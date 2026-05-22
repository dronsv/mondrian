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

## Background: building blocks already in the codebase

`mondrian/src/main/java/mondrian/rolap/sql/CrossJoinDependencyPruner.java` already implements every primitive this fix needs. The fix MUST reuse them, not re-implement.

1. `resolveSyntheticFlat(Hierarchy)` (line 264) — unwraps `RolapCubeHierarchy → RolapHierarchy → SyntheticFlatHierarchy`, returns null otherwise. Handles the wrapper that `RolapCubeLevel.getHierarchy()` returns at runtime.

2. `findCommonSourceLink(RolapLevel dependentLevel, RolapLevel determinantLevel)` (line 233) — iterates **all** `SourceLink`s on both sides and looks for a pair `(detLink, depLink)` such that they reference the same source hierarchy and `depLink.depth() > detLink.depth()`. Returns the dependent-side `SourceLink` (so the caller knows at what depth the dependent member lives in the common source hierarchy). Returns null if there is no common dependency. This is the right "are these levels siblings of a shared source hierarchy" test, NOT a `getSourceHierarchy() ==` comparison.

3. `collectAncestorKeys(List<RolapMember>, RolapLevel)` (line 279) — for each `dependentMember`, walks `getParentMember()` up the source hierarchy until reaching the given `determinantLevel`, returns the set of source-level keys at that depth. Used to obtain "the source-level-N keys these dependent members descend from".

4. `filterMembersByKey(List<RolapMember>, Set<Object>)` (line 321) — filters a member list to those whose key is in the allowed set.

These primitives were introduced for `CrossJoinDependencyPruner`'s cross-arg pruning; they apply unchanged to the drill-down case.

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

Let `drillSF = resolveSyntheticFlat(drillHierarchy)`. If `drillSF == null`, the drill target is not a synthetic-flat → existing behavior unchanged, emit all children of `m_k`.

For each other tuple position `j ≠ k`:

a. Let `siblingSF = resolveSyntheticFlat(tuple[j].getHierarchy())`. Skip if null.

b. Let `siblingLevel = ((RolapCubeLevel) tuple[j].getLevel()).getRolapLevel()` (or equivalent unwrap). Skip if not `RolapLevel`.

c. Let `drillLevelChild = (the level the drill target's children live at)`. For an [All]-member drill, this is the synthetic-flat hierarchy's leaf level. For a non-[All] drill, it is the child level of `m_k`'s level.

d. Compute `depLink = drillSF.findLinkForHierarchy(siblingSF.getSourceHierarchy())`. If `depLink == null`, the drill hierarchy has no `SourceLink` to `siblingSF`'s source — skip (no correlation possible).

e. Compute `detLink = siblingSF.findLinkForHierarchy(siblingSF.getSourceHierarchy())`. If `detLink == null` or `depLink.depth() <= detLink.depth()`, the sibling does not live at an *ancestor* depth in the shared source hierarchy → no parent-child constraint to apply for this `j`; skip.

f. Project `tuple[j]` to a source-level member: `sourceMember_j = siblingSF.projectToSource(tuple[j])`. (See implementation notes for how to derive this from the member's key; `SyntheticFlatHierarchy` exposes a 1:1 key↔source-key map.)

g. Add `sourceMember_j` to the set `requiredAncestors[depLink.hierarchy()][detLink.depth()]` — a map keyed by source hierarchy and ancestor level.

After scanning all `j ≠ k`:

- If `requiredAncestors` is empty, no sibling synthetic-flat siblings → emit all children of `m_k` unchanged.
- Otherwise, fetch the candidate children list: `children = evaluator.getSchemaReader().getMemberChildren(m_k)`.
- Project each `child` to its source-hierarchy member chain, walk ancestors using `collectAncestorKeys(...)` at each `(sourceHierarchy, ancestorLevel)` entry in `requiredAncestors`, and keep `child` only if every projected ancestor key matches the corresponding `sourceMember_j` key.
- Emit a tuple per surviving child.

### Why this rule is correct

Two correctness anchors:

1. **When no sibling synthetic-flat positions exist in the tuple**, the rule produces the same children as today — the `requiredAncestors` map is empty and the unfiltered `children` list is emitted. Backward-compatible.
2. **When all involved synthetic-flat siblings share a common source hierarchy** (the #78 case), each drilled tuple corresponds to a unique source-hierarchy parent-child path. Children whose source-level ancestor doesn't match the sibling's source-level position cannot belong to a valid path and would have been NON-EMPTY-pruned later anyway. The filter is an upfront prune, not a semantic change.

Tuples that today already satisfy the source-path constraint remain in the output. Tuples that today are emitted but pruned later (or, in the OOM scenario, materialized and never pruned due to heap exhaustion) are simply not emitted.

### Cardinality consequence

After the fix, drill output cardinality is bounded by the source hierarchy's actual parent-child fan-out at each step, not by the full sibling level. For the FitnessShock live capture:

| After step | Pre-fix tuples | Post-fix tuples (bounded by source fan-out) |
|---|---|---|
| Initial Crossjoin | 15 | 15 |
| Drill → Category3 | ~930 | ≤ 76 (≤ avg fan-out of Category2→Category3 per source path) |
| Drill → Category4 | ~100 K | ≤ 184 |
| Drill → Category5 | ~16 M | ≤ 277 |
| Drill → Product | ~17 B | ≤ 1248 |

Final outer-axis cardinality after `NON EMPTY` ≤ valid `(cat1, cat2, cat3, cat4, cat5, sku)` paths in the source hierarchy. No 17B intermediate set; no OOM.

### Edge cases

| Case | Handling |
|---|---|
| Drill target is not a synthetic-flat | `resolveSyntheticFlat(drillHierarchy) == null` → emit all children (existing behavior). |
| Drill target is synthetic-flat, no other position is sibling-source synthetic-flat | `requiredAncestors` ends empty → emit all children. |
| Drill target is synthetic-flat, sibling positions are at `[All]` | `tuple[j]` is the [All] of a sibling synthetic-flat. `m_j.getKey()` is null or "all". The "project to source" step yields the source [All] member. Walking source ancestors of any candidate child up to that level always succeeds — effectively no constraint added. Children emitted unchanged. Correct: `[All]` imposes no parent. |
| Tuple contains both real-hierarchy and synthetic-flat positions referring to the same source dimension | `resolveSyntheticFlat` of the real hierarchy returns null → not added to `requiredAncestors`. Synthetic-flat siblings still constrain each other. Real-hierarchy correlation is left to the existing native CJ path (which already handles it). |
| Two different source hierarchies (e.g., `[Product.Category]` synthetic-flat siblings + `[Store.Geography]` synthetic-flat siblings in the same tuple) | `findLinkForHierarchy(sourceA)` from a `sourceB` synthetic-flat returns null → no entry added for that hierarchy pair. Each source hierarchy's siblings constrain independently. |
| Multi-source-link synthetic-flat (one flat hierarchy links to two source hierarchies) | `findCommonSourceLink` already iterates all source links. The fix uses the same primitive. |
| Drill target is a synthetic-flat but its `SourceLink` to the sibling's source hierarchy doesn't exist | No correlation possible → that `j` is skipped. Other sibling positions may still apply. |
| Sibling synthetic-flat is at a *deeper* level than the drill target in the shared source hierarchy (`depLink.depth() <= detLink.depth()`) | The "sibling" is not an *ancestor* in the source hierarchy — it can't constrain the drill. Skip. |
| `evaluator.getSchemaReader()` returns a restricted reader (role-aware) | The post-filter applies on top of the role-restricted children list — security/visibility preserved. |
| Calculated members in the tuple | `collectAncestorKeys` returns null for calculated members (existing behavior). Treat as no constraint from that `j`. |

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

Run the existing acceptance packs against the new image:

- `scripts/run_issue77_regression.sh` (4 queries, FitnessShock) — must remain green.
- `scripts/run_fitnessshock_preflight.sh` (#71/#72 shapes) — must remain green.
- `scripts/run_excel_product_analysis_pack.sh` (52 queries, Konfet) — must remain green against the dev stack.

## Implementation notes

- **Source projection API on `SyntheticFlatHierarchy`.** The implementer must locate the correct accessor. Candidates: `SyntheticFlatHierarchy.findLinkForHierarchy(...)` returns a `SourceLink`; the link should expose source-level + source-key-from-flat-member mapping. If no public accessor exists, add a package-private one — do not duplicate the lookup logic inline.
- **Level resolution.** `tuple[k].getLevel()` may return a `RolapCubeLevel`; downstream calls (`collectAncestorKeys`) expect `RolapLevel`. Use the unwrap pattern from `CrossJoinDependencyPruner.resolveSyntheticFlat`.
- **`evaluator.getSchemaReader()` cost.** Existing call is once per tuple. The fix adds one more call only if `requiredAncestors` is non-empty AND the cached children list isn't already filterable.
- **The existing `getMemberChildren(tuple[k])` already returns post-NON-EMPTY children when called from the right evaluator context.** Verify by reading `RolapSchemaReader.getMemberChildren` semantics before assuming children are or aren't measure-aware. The fix doesn't change that.

## Open questions

1. Does `SyntheticFlatHierarchy` expose a public source-key projection accessor today, or does the fix need to add one? If yes — where (which file/line)?
2. Should the same correlation rule be added to `drillDownObj` (the non-cross drill at line 87) for symmetry? Tentatively: no — `drillDownObj` expands `tuple[k]`'s own children (parent-restricted by construction), so the bug doesn't surface there. Verify with a unit test before deciding.
3. The post-fix `tryPruneExpandedDrilldownMember` (`DrilldownMemberFunDef:186`) currently bails when `evaluator.isNonEmpty()` is false or `NativeNonEmptyFilterEnable` is off. The new filter runs unconditionally. Is that correct, or should it also gate on `NativeNonEmptyFilterEnable`? Tentatively: unconditional is correct — the filter never removes a tuple that would survive a correct semantic evaluation; it only avoids materializing tuples that are *invalid* in the source-hierarchy sense.

## Cross-references

- Investigation log: `docs/superpowers/specs/2026-05-22-issue78-flat-crossjoin-fold.md` (REJECTED, do not implement).
- Issue thread: https://github.com/dronsv/emondrian-clickhouse/issues/78
- Referenced primitive: `mondrian/src/main/java/mondrian/rolap/sql/CrossJoinDependencyPruner.java` lines 233 (`findCommonSourceLink`), 264 (`resolveSyntheticFlat`), 279 (`collectAncestorKeys`), 321 (`filterMembersByKey`).
- Target code: `mondrian/src/main/java/mondrian/olap/fun/DrilldownMemberFunDef.java` lines 117-151 (`drillDownCrossHierarchy`).
- Memory: `[[issue71-state]]`, `[[issue45-state]]` for the chronology of synthetic-flat fixes.
