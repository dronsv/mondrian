# Issue #78 investigation log — REJECTED HYPOTHESIS: `RolapNativeCrossJoin.areLevelsConnected`

**Status:** **REJECTED · do not implement.** This document records an investigation path that was disproven by live evidence. The original hypothesis — that `RolapNativeCrossJoin.areLevelsConnected` failing to recognize same-source-hierarchy synthetic-flat levels was the root cause of #78 — turned out to be wrong: neither the guard nor the connectivity check is on the failing code path. The actual fix target was `DrilldownMemberFunDef.drillDownCrossHierarchy`; it is implemented and live-verified in the companion spec `docs/superpowers/specs/2026-05-22-issue78-drilldown-synthflat-correlation.md`.

This file is kept as an investigation record (what was tried, why, and why it was rejected) so future work on `SyntheticFlatHierarchy` performance doesn't re-walk the same path.

---

## Originally proposed (rejected)

The hypothesis: when Excel issues a 5-level nested `Crossjoin([Product.Category1].Members, [Product.Category2].Members, …, [Product.Category5].Members)` over five sibling `SyntheticFlatHierarchy` instances projecting levels of one source hierarchy, `RolapNativeCrossJoin.areLevelsConnected` (`mondrian/src/main/java/mondrian/rolap/RolapNativeCrossJoin.java:664`) returns `false` because each synthetic-flat hierarchy is a distinct Java instance and `getHierarchy().equals(...)` is identity-based. The independent-CrossJoin guard `maybeFailFastIndependentCrossJoin` then either throws `ResourceLimitExceededException` (when `mondrian.result.limit > 0`) or pushes the evaluator into a slow Java-side cascading cartesian.

The proposed fix was to extend `areLevelsConnected` with a check: if both levels' hierarchies are `SyntheticFlatHierarchy` instances sharing the same `getSourceHierarchy()`, treat them as connected.

## Why it was rejected

Two rounds of evidence:

### Code review (2026-05-22)

A reviewer raised five findings against the original spec:

1. **`areLevelsConnected` is only consulted by the `maybeFailFastIndependentCrossJoin` guard.** It does not enable or disable native CrossJoin dispatch and does not change generated SQL. If the guard does not throw, the proposed change is a no-op. If the guard does throw, the change converts the throw into an unstudied "proceed" — without verified evidence of what the proceed actually does at scale on the offending query.
2. **The `instanceof SyntheticFlatHierarchy` check would no-op for cube queries.** Synthetic-flat hierarchies are wrapped by `RolapCubeHierarchy`; `RolapCubeLevel.getHierarchy()` returns the wrapper. Unwrap is required and the spec relegated it to an edge case.
3. **The cardinality-bound claim was factually wrong.** `estimateUpperBoundCardinality` multiplies argument cardinalities *before* consulting connectivity; connectivity is used for throw-vs-proceed, not for bound calculation.
4. **`getSourceHierarchy()` covers only the primary source link.** `SyntheticFlatHierarchy` supports multiple `SourceLink`s; the existing `CrossJoinDependencyPruner.findCommonSourceLink` iterates all of them via `findLinkForHierarchy`. The spec's `==` comparison on `getSourceHierarchy()` would miss aliased levels.
5. **The proposed live-verification oracle (a two-`Members` Crossjoin over the same source hierarchy) is rejected by `TupleType.checkHierarchies`** (`mondrian/src/main/java/mondrian/olap/type/TupleType.java:203`) — same-hierarchy duplicate. A deepest-level `[Product.Category].[Category5].Members` reference query is the right oracle.

### Live empirical evidence (2026-05-22)

Ran the exact reproducer from the issue body (5-level pure nested `Crossjoin`) against the FitnessShock VM with three different mondrian builds spanning weeks of commits (`issue77d-5503ca7`, `issue77-a30294a`, `main-0d3b5d`):

- **0.30–2.4 s wall time** (cold cache slower, warm-cache fast)
- **160 rows** in every case — the source-hierarchy leaf cardinality
- Container `mondrian-sql.log` shows a **single** `SqlTupleReader.readTuples [[Category1], [Category2], [Category3], [Category4], [Category5]]:` call → one multi-column SELECT against `agg_fitnessshock_month_category_region` → one `Segment.load`
- No `maybeFailFastIndependentCrossJoin` trigger, no exception, no cascading expansion

`CrossJoinArgFactory.checkCrossJoin` already flattens the nested binary `Crossjoin` tree into a 5-arg `CrossJoinArg[]` and the native SQL builder emits one SELECT. The same `dim_fitnessshock_product`/`agg_*` JOIN that resolves member existence inherently restricts the result to **valid source-hierarchy paths only**. The proposed fix to `areLevelsConnected` would have changed nothing observable on this shape.

### What the bug actually is

Live capture during a real Excel session against the same VM (2026-05-22 11:12) reproduced the OOM with a different MDX:

```mdx
NON EMPTY
  Crossjoin(
    Hierarchize(DrilldownLevel({[Product.Category1].[All]}, , , INCLUDE_CALC_MEMBERS)),
    Hierarchize(
      DrilldownMember(   -- step 4
        DrilldownMember( -- step 3
          DrilldownMember( -- step 2
            DrilldownMember( -- step 1
              Crossjoin(
                {[Product.Category2].[All], [Product.Category2].AllMembers},
                {([Product.Category3].[All], [Product.Category4].[All],
                  [Product.Category5].[All], [Product.Товар].[All])}),
              {-{[Product.Category2].[<one>]}}, [Product.Category3]),
            [Product.Category3].AllMembers, [Product.Category4]),
          [Product.Category4].AllMembers, [Product.Category5]),
        [Product.Category5].AllMembers, [Product.Товар])))
```

This is Excel's "+ expand" pivot mechanic — a chain of `DrilldownMember` over sibling synthetic-flat hierarchies, not a flat `Crossjoin` of `.Members` sets. `DrilldownMemberFunDef.drillDownCrossHierarchy` does cross-hierarchy drill by emitting one tuple per child returned by `getMemberChildren(tuple[k])`, with no correlation to the rest of the tuple. With sibling synthetic-flat hierarchies that have no parent-child relationship, this produces a full Cartesian.

Cumulative cardinality on FS: ~15 → 930 → 100K → 16M → ~17B candidate tuples. JVM heap pegs at `-Xmx5g`, heap dump at `/tmp/java_pid1.hprof` (1.8 GB), VM unresponsive until container restart.

The fix must live in `DrilldownMemberFunDef.drillDownCrossHierarchy`, not in `RolapNativeCrossJoin.areLevelsConnected`. See the new spec.

## Issue thread

- [#78 comment 4518056303](https://github.com/dronsv/emondrian-clickhouse/issues/78#issuecomment-4518056303) — initial "cannot reproduce" against the issue body's shape.
- [#78 comment 4518253349](https://github.com/dronsv/emondrian-clickhouse/issues/78#issuecomment-4518253349) — corrected diagnosis after live capture.

## Building blocks that ARE reusable (for the new spec)

The investigation surfaced existing primitives in `mondrian/src/main/java/mondrian/rolap/sql/CrossJoinDependencyPruner.java` that the corrected fix will reuse:

- `resolveSyntheticFlat(Hierarchy)` (line 264) — unwraps `RolapCubeHierarchy` → `SyntheticFlatHierarchy` or returns null. Addresses Finding 2.
- `findCommonSourceLink(RolapLevel, RolapLevel)` (line 233) — iterates all `SourceLink`s on both sides to find a common ancestor relationship. Addresses Finding 4.
- `collectAncestorKeys(List<RolapMember>, RolapLevel)` (line 279) — walks parents in the source hierarchy up to a given level. Addresses the source-path correlation requirement.
- `filterMembersByKey(...)` (line 321) — filters members by an allowed key set.

The corrected fix will use these directly rather than re-implementing.
