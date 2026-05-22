# Fold CrossJoin of sibling synthetic-flat levels back to their source hierarchy

**Status (2026-05-22, updated):** **TARGETED WRONG CODE PATH — superseded by live investigation.** This spec proposed an `areLevelsConnected` change to `RolapNativeCrossJoin`. Two rounds of evidence:

1. The issue body's pure-`Crossjoin` MDX **does not reproduce the bug** — that shape is intercepted by `RolapNativeCrossJoin` and resolved by a single multi-column SQL (0.30 s for 160 rows on FitnessShock). See [#78 comment 4518056303](https://github.com/dronsv/emondrian-clickhouse/issues/78#issuecomment-4518056303).
2. Real Excel emits a `Crossjoin(Hierarchize(...), Hierarchize(DrilldownMember(DrilldownMember(...))))` chain, not a flat `Crossjoin`. That shape **does** OOM on the same VM/image — live capture 2026-05-22 11:12-11:14, JVM heap pegged at 5.1 GB, heap dump at `/tmp/java_pid1.hprof`. The actual code path is `DrilldownMemberFunDef.drillDownCrossHierarchy` (`mondrian/src/main/java/mondrian/olap/fun/DrilldownMemberFunDef.java:117-151`), which emits cartesian cross-hierarchy children with no source-hierarchy correlation. See [#78 comment 4518253349](https://github.com/dronsv/emondrian-clickhouse/issues/78#issuecomment-4518253349).

This spec is kept as an investigation log of the path explored but **must not be implemented** — `areLevelsConnected` is not the offending check. A new spec targeted at `DrilldownMemberFunDef.drillDownCrossHierarchy` (source-hierarchy-constrained cross-hierarchy drill) is the right next step.

---

**Original status:** proposal · 2026-05-22 · follow-up to #71, addresses emondrian-clickhouse#78

## Problem

`SyntheticFlatHierarchy` (introduced for the #71 family) exposes individual levels of a real drill hierarchy as separate top-level fields, so Excel can show e.g. `[Product.Category1]` … `[Product.Category5]` as five independent pivot fields even though they're all aliases of levels in the source hierarchy `[Product.Category]`.

Excel reliably emits a nested `CrossJoin` across these flat fields:

```mdx
NON EMPTY
  Crossjoin(Crossjoin(Crossjoin(Crossjoin(
    [Product.Category1].[Category1].Members,
    [Product.Category2].[Category2].Members),
    [Product.Category3].[Category3].Members),
    [Product.Category4].[Category4].Members),
    [Product.Category5].[Category5].Members)
ON ROWS
```

Mondrian treats the five `SyntheticFlatHierarchy` instances as independent hierarchies and expands the full Cartesian product. On the cited cube (4 · 14 · 62 · 108 · 160 cardinalities, 160 valid source-hierarchy paths), the synthetic-flat shape took **115.7 s** vs **1.5 s** for the equivalent `[Product.Category].[Category5].Members` query against the source hierarchy. Under sustained Excel use this also caused `java.lang.OutOfMemoryError: Java heap space`.

### Root cause (verified by investigation)

`RolapNativeCrossJoin.areLevelsConnected` at `mondrian/src/main/java/mondrian/rolap/RolapNativeCrossJoin.java:664`:

```java
if (left.getHierarchy() != null
    && right.getHierarchy() != null
    && left.getHierarchy().equals(right.getHierarchy()))
{
    return true;
}
return hasValidatedRule(context, left, right) || ...;
```

Each `[Product.CategoryN]` is a distinct `SyntheticFlatHierarchy` instance — `equals` is identity-based, so five different hierarchies are never equal. No `DependencyRegistry` rule connects them. `hasDependencyConnectivity` returns false → `maybeFailFastIndependentCrossJoin` either:

1. Throws `ResourceLimitExceededException` (when `ResultLimit > 0`), or
2. Silently declines native, so `CrossJoinFunDef.CrossJoinIterCalc.evaluateIterable` falls through to a Java-side cascade. The innermost pair may evaluate natively → produces a `TupleList`; the next outer `CrossJoinIterCalc` sees that materialized list as a non-native operand, expands it via `MemberListCrossJoinArg`, multiplies into the next level, and cascades. The result is hundreds of millions of intermediate tuples materialized in Java heap before `NON EMPTY` prunes to 160.

The infrastructure to fold these back to the source hierarchy already exists:

- `SyntheticFlatHierarchy.getSourceHierarchy()` / `getSourceLevel()` (`mondrian/src/main/java/mondrian/rolap/SyntheticFlatHierarchy.java:107-129`).
- Agg-table matching already understands synthetic-flat → source-level forwarding (`ExplicitRecognizer.java:404-497`, `RolapStar.java:803-815`).
- `NativeNonEmptyFilter` already uses the source-level unwrap pattern (`NativeNonEmptyFilter.java:459`).

Nothing in the CrossJoin path consults this mapping.

## Scope (this proposal)

**In scope:** detect at native-CrossJoin time that ≥2 `CrossJoinArg` operands' levels belong to `SyntheticFlatHierarchy` instances sharing the same `getSourceHierarchy()`, treat them as connected, and let the existing native CJ SQL builder emit a single multi-column SELECT against the source dim table.

**Phased, two-step delivery:**

- **Phase 1 (this proposal): minimal "connectivity recognition" fix.** Teach `areLevelsConnected` that same-source-hierarchy synthetic-flat levels are connected. This alone is sufficient to unblock the native path for the issue body's MDX — native CJ then fires with five `DescendantsCrossJoinArg` entries, the existing SQL builder emits one SELECT with five column references, ClickHouse returns ≤160 rows, and the result is correct and fast.
- **Phase 2 (follow-up, separate spec): aggressive fold.** `CrossJoinArgFactory.checkCrossJoin` post-flatten step that collapses multiple same-source-hierarchy `DescendantsCrossJoinArg` entries into a single source-hierarchy multi-level arg. This makes the fix robust when native CJ is unavailable for unrelated reasons (e.g., NON EMPTY off, dependency rule disabled, mixed flat + real members in the same CrossJoin). Out of scope here.

**Out of scope:**

- Folding for `Tuple` axes (the issue body's MDX uses `CrossJoin`; `Tuple` axes have a separate code path).
- Mixed CrossJoin of synthetic-flat levels from *different* source hierarchies (correctly remains a Cartesian).
- Mixed CrossJoin of synthetic-flat + real-hierarchy levels (Phase 2 will cover; Phase 1 leaves them as-is).
- Changes to `TupleType.checkHierarchies` identity comparison (line 203 of `TupleType.java`). The current `==` identity check is correct *for the type system*; the fix is purely about runtime evaluation strategy.

## Design

### Where the fix lands

A single new check in `RolapNativeCrossJoin.areLevelsConnected` (`mondrian/src/main/java/mondrian/rolap/RolapNativeCrossJoin.java`, around line 664):

```java
private boolean areLevelsConnected(
    DependencyContext context,
    RolapLevel left,
    RolapLevel right)
{
    if (left == null || right == null) {
        return false;
    }
    if (left.getHierarchy() != null
        && right.getHierarchy() != null
        && left.getHierarchy().equals(right.getHierarchy()))
    {
        return true;
    }
    // (NEW) Two synthetic-flat hierarchies projecting levels of the
    // same real hierarchy are connected by construction — they share
    // the same dim table and are correlated by the source hierarchy's
    // parent-child structure. Without this check, the independent-
    // CrossJoin guard treats them as independent and either fails
    // fast (ResultLimit) or pushes the evaluator into the slow
    // Java-side cartesian. See dronsv/emondrian-clickhouse#78.
    if (left.getHierarchy() instanceof SyntheticFlatHierarchy leftSf
        && right.getHierarchy() instanceof SyntheticFlatHierarchy rightSf
        && leftSf.getSourceHierarchy() != null
        && leftSf.getSourceHierarchy() == rightSf.getSourceHierarchy())
    {
        return true;
    }
    return hasValidatedRule(context, left, right)
        || /* … existing terms unchanged … */;
}
```

That is the entire engine change for Phase 1. Three lines of actual logic; the rest is comment.

### Why this is sufficient (and bounded)

`areLevelsConnected` only affects the independent-guard decision. Once two operands are reported as connected, the native CJ SQL builder proceeds with the existing flow:

1. `CrossJoinArgFactory.checkCrossJoin` already flattens the nested binary tree into a `CrossJoinArg[]` of 5 `DescendantsCrossJoinArg` entries.
2. The SQL generator emits a single SELECT with 5 column expressions and applies any `NON EMPTY` constraint via the fact join.
3. Each level's column resolution goes through `RolapStar.lookupColumn(... synthetic-flat aware lookup at line 803-815 ...)` which already forwards to the source-level star column.
4. Agg-table matching via `ExplicitRecognizer` already iterates source links for synthetic-flat levels (lines 404-497), so a covering agg table is still found if present.
5. Result rows are projected back into tuples whose member references are the *synthetic-flat* members — matching what the AST asked for, so XMLA `MEMBER_UNIQUE_NAME` / `MEMBER_CAPTION` contracts established in #71 are preserved.

The fix does not change anything about the type system, the AST shape, the cell-loading pipeline, or aggregate matching. It only stops a guard from incorrectly disabling an already-working path.

### Cardinality bound

After the fix, `estimateUpperBoundCardinality` (`RolapNativeCrossJoin:586` per the investigation) still computes the product of level cardinalities, which for the cited cube is ~60M. The independent guard would still fail-fast at `ResultLimit` if treated as independent — but now treated as connected, the guard skips the multiplication and uses the source-hierarchy's leaf cardinality as the bound. Since `SyntheticFlatHierarchy.getSourceHierarchy()` is the same object across the five levels, the leaf cardinality is the source hierarchy's leaf count (160 in the issue) — well within any sane `ResultLimit`.

This is correct: the actual number of distinct tuples is bounded by the source hierarchy's deepest-level cardinality, not by the Cartesian product.

### Verification approach

Two layers:

1. **Unit test on `areLevelsConnected`** in `RolapNativeCrossJoinGuardEstimateTest` — mock two `SyntheticFlatHierarchy` instances sharing the same `getSourceHierarchy()` return value, assert `hasDependencyConnectivity` returns true. Also assert that two synthetic-flat hierarchies from *different* source hierarchies remain reported as independent (negative case).

2. **IT against H2 FoodMart** — there is no FoodMart schema with `flatName` levels today, so the IT either (a) builds a tiny ad-hoc schema with two `flatName` levels from one hierarchy and exercises a 2-level CrossJoin, or (b) lives in a new dedicated IT class with its own schema fixture. Approach (a) using `TestContext.legacy().create(...)` is the established pattern in this repo (see `mondrian/src/it/java/mondrian/test/SchemaModifiers.java` and `mondrian/src/it/java/mondrian/test/loader/...`).

3. **Live verification on the FitnessShock VM** — the deployed catalog `FitnessShock.generated.xml` already declares `[Продукт.Категория1]`, `[Продукт.Категория2]` as synthetic-flat siblings of `[Продукт.Категория]`. A small acceptance MDX:

   ```mdx
   SELECT {[Measures].[Продажи руб]} ON 0,
          NON EMPTY Crossjoin(
            [Продукт.Категория1].[Категория1].Members,
            [Продукт.Категория2].[Категория2].Members
          ) ON 1
   FROM [FitnessShock]
   ```

   Pre-fix on the live `issue77d-5503ca7` image (against prod CH): expected to be slow / OOM-prone under `ResultLimit > 0`. Post-fix on `issue78-<sha>` image: should match the rows of `Crossjoin([Продукт.Категория].[Категория1].Members, [Продукт.Категория].[Категория2].Members)` from the real hierarchy (i.e., parent-child path count, not 4·14 = 56 cartesian).

### Edge cases

| Case | Behavior |
|---|---|
| Two synthetic-flat from same source hierarchy | Connected → native CJ fires → single SQL → correct, fast |
| Two synthetic-flat from *different* source hierarchies | Not connected (the new check requires equal `getSourceHierarchy()`) → existing logic unchanged → cartesian as today |
| Synthetic-flat + real-hierarchy level from same dimension | Not connected by the new check (one side is not `SyntheticFlatHierarchy`) → unchanged. Phase 2 will handle. |
| Synthetic-flat + measure / time / unrelated hierarchy | Not connected → unchanged |
| `SyntheticFlatHierarchy.getSourceHierarchy()` returns `null` (degenerate case) | Falls through to the existing checks → unchanged |
| Levels with different `RolapCubeHierarchy` wrapping but underlying same `SyntheticFlatHierarchy` | The `instanceof SyntheticFlatHierarchy` check covers the direct case. If the level's hierarchy is a `RolapCubeHierarchy` wrapping a `SyntheticFlatHierarchy`, we may need to unwrap. The investigation noted `NativeNonEmptyFilter.java:459` already does this unwrap pattern. **Implementation must verify**: walk `RolapCubeHierarchy.getRolapHierarchy()` (or equivalent) before the `instanceof` check, mirroring the existing unwrap. |
| Subselect-restricted synthetic-flat axes | The subcube predicate machinery (recently fixed for #77) is downstream of the CJ tuple expansion. The fix preserves all subcube semantics — same `DescendantsCrossJoinArg` entries flow through, the only change is the connectivity-guard decision. |

### Risks

- **Regression in non-flat scenarios.** The new check is additive and gated on `instanceof SyntheticFlatHierarchy` + equal source hierarchy. Any path that didn't have synthetic-flat levels in the first place is untouched.
- **Aggregate matching.** Preserved by construction (see "Why this is sufficient", bullet 4). `ExplicitRecognizerAliasMatchTest` should still pass.
- **`NativeQueryEngineEligibilityTest` axis guard.** That guard explicitly *blocks* NQE for any axis containing `SyntheticFlatHierarchy` (`NativeQueryEngine.java:126-136 / 164-196`). This fix is in the native CJ path, not NQE. The two guards are independent. Verify both regression tests still pass after the change.
- **`RolapCubeHierarchy` wrapping.** Documented in the edge-case table; the implementer must unwrap before the `instanceof` check. If the unwrap is missed, the fix degrades to a no-op (the slow path is taken as today) — fail-closed.
- **`hasValidatedRule` interaction.** The new check fires *before* `hasValidatedRule`, so it cannot regress any explicit dependency rules already configured for the cube.
- **Cardinality estimate.** With the levels reported as connected, `estimateUpperBoundCardinality` skips the multiplication step. This must be confirmed by reading the estimator code; if the estimator still multiplies for connected hierarchies (unlikely but possible), the guard threshold should be reconsidered.

## Phased follow-ups (separate proposals)

1. **`CrossJoinArgFactory.checkCrossJoin` post-flatten fold.** When the flattened arg list contains multiple `DescendantsCrossJoinArg` entries whose levels are synthetic-flat siblings of the same source hierarchy, collapse them to a single source-level multi-column arg. Necessary if native CJ is declined for unrelated reasons (NON EMPTY off, etc.). Larger surface area; deferred.

2. **Tuple-axis (non-CrossJoin) support.** `Tuple(([Cat1].m1, [Cat2].m2, …))` has a different parse-tree shape. If Excel ever emits this, a parallel fold is needed in the tuple evaluator.

3. **Mixed synthetic-flat + real-hierarchy CrossJoin in same dimension.** When Excel mixes `[Product.Category1].Members` with `[Product.Category].[Category2].Members` (the real hierarchy alongside one synthetic alias), the fold must detect that both reference the same source hierarchy and treat them as connected.

4. **Connectivity reporting / EXPLAIN output.** A debug log line when the new check fires would help future diagnosis of slow CJ shapes. Optional; not blocking.

## Open questions

1. Is `SyntheticFlatHierarchy.getSourceHierarchy()` guaranteed to return the *same Java instance* for two synthetic-flat levels declared in the same `<Hierarchy>` parent? The fix relies on `==` identity. Verify against `SyntheticFlatHierarchy.addSourceLink` to confirm the source-hierarchy reference is shared. If it's not, the check should use `equals()` on hierarchy unique-name instead. Implementer must confirm before writing the test.

2. Should the fix also be exposed as a feature flag (e.g., `mondrian.rolap.crossjoin.foldSyntheticFlatSiblings.enable`, default `true`)? My read: no — the change is strictly additive (only relaxes a guard) and fails closed if the unwrap misses. Adding a flag costs test-matrix overhead with no real safety benefit.

3. Does the existing `DependencyRegistry` already have a rule type that could express "these N hierarchies are siblings of the same source"? If so, a more declarative fix would be to add such a rule automatically when `SyntheticFlatHierarchy` instances are constructed (`addSourceLink` site). Worth a 30-minute read of `DependencyRegistry` before committing to the inline `areLevelsConnected` check; the declarative variant may be cleaner.

## Verification matrix (before merge of Phase 1)

- Unit: `mondrian.rolap.RolapNativeCrossJoinGuardEstimateTest` — +2 cases (positive same-source, negative different-source). Existing tests still pass.
- Unit: `mondrian.rolap.NativeQueryEngineEligibilityTest` — unchanged, still 100% pass (axis guard is independent).
- Unit: `mondrian.rolap.FlatHierarchyTest` — unchanged.
- Unit: `mondrian.rolap.aggmatcher.ExplicitRecognizerAliasMatchTest` — unchanged.
- IT: new IT in `mondrian/src/it/java/mondrian/rolap/` exercising a 2-level synthetic-flat CJ shape against a fixture schema; assert row count matches the source-hierarchy path count, not the Cartesian.
- Live: build a new image (`issue78-<sha>`), deploy to the FitnessShock VM in place of `issue77d-5503ca7`, run the existing FitnessShock preflight pack + a new q03 covering the synthetic-flat-CJ shape. Target: synthetic-flat-CJ wall time should drop from observed-slow / OOM to a value comparable to the source-hierarchy query.

## Tag for the resulting image (post-implementation)

`issue78-<short-outer-sha>` (after the outer submodule bump + push). Replaces `issue77d-5503ca7` on the FitnessShock VM, preserving the prior image as `*-prev` for rollback as we did for #77.
