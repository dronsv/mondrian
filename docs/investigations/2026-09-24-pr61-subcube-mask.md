# NECJ expression-cache subcube reset key

An explicit All-member tuple can change subcube visibility while the current
member remains All. The expression-result key must therefore include the
evaluator's immutable `ignoredSubcubeHierarchies` snapshot. PR61's NECJ memo
otherwise reuses the first context's tuple list in the second context.

## Reproduction

`mondrian/src/test/java/mondrian/rolap/NonEmptyCrossJoinSubcubeMemoTest.java`
contains the complete synthetic H2 fixture, schema and connection setup. Every
test invocation uses a fresh database, schema, connection and native tuple cache.
The fact rows are:

- 2025, A, S1, quantity 10.
- 2026, B, S1, quantity 20.
- 2026, C, S2, quantity 30.

The two calculated measures share the same compiled NECJ expression:

```mdx
WITH MEMBER [Measures].[Pair Count] AS
  Count(NonEmptyCrossJoin([Product].[Name].Members, [Store].[Name].Members))
MEMBER [Measures].[Reset Pair Count] AS
  ([Measures].[Pair Count], [Calendar].[All Years])
SELECT {[Measures].[Pair Count], [Measures].[Reset Pair Count]} ON 0
FROM (SELECT {[Calendar].[2026]} ON 0 FROM [Sales])
```

The literal result is `[2, 3]`. Reversing the columns must produce `[3, 2]`.
Selecting each measure alone must produce `[2]` and `[3]`, respectively.

Run the fixture from each worktree through the shared Docker Java 25 build lock:

```sh
flock /tmp/emondrian-correctness-build.lock ./scripts/test.sh NonEmptyCrossJoinSubcubeMemoTest
```

The shipping tests disable native enumeration and set
`mondrian.expCache.enable=false`. This disables Count's separate, automatically
inserted `Cache(...)` wrapper, isolating the unconditional NECJ memo introduced
by PR61. It does not disable that memo. With the property left at its default
true, the older Count cache already exhibits the same missing-mask collision.

With the Count cache disabled:

- Pre-PR61 `0cd94cab48b97d1c2ad0c15683cb22a8fe586cc2` returns the literal
  `[2, 3]` and `[3, 2]`; both isolated counts pass.
- PR61 `8eb54969a8715314d0b37b1f228101222ba984d5` returns `[2, 2]` and `[3, 3]`;
  both isolated counts still pass.

The fix adds only the existing immutable reset-mask snapshot to both expression
key paths. Cell-reader dirty/miss validity rules and the NECJ memo remain in
place. The unchanged `NonEmptyCrossJoinJudgeCostTest` checks native memo savings,
including the two judging passes for one batch-load pass plus the loaded answer.

## Separate native candidate restriction

The same fixture also exposes a preexisting native-enumeration issue. To run
that probe, change the two explicit `false` arguments in `values(...)`:

```java
properties.EnableNativeNonEmpty.set(true);
registry.setEnabled(true);
```

Leave the four literal expectations unchanged and run the same test command.
The normal-only count passes, but the reset-only count is `[2]` instead of
`[3]`, and both column orders return `[2, 2]`. The native candidate SQL retains
`calendar.year = 2026` while the reset context's cell aggregation SQL correctly
omits that restriction. The 2025-only `(A, S1)` pair never reaches judging.

This was reproduced on:

- Pre-PR61 `0cd94cab48b97d1c2ad0c15683cb22a8fe586cc2`, with Count's cache both
  enabled and disabled.
- PR61 `8eb54969a8715314d0b37b1f228101222ba984d5`, with Count's cache both
  enabled and disabled.
- Corrected integration `c367590b8ddaf1f083f5ec303fd466b4c5a7e2a0` plus this
  mask-key fix, with Count's cache enabled. To reproduce that exact configuration,
  also change the fixture's `mondrian.expCache.enable` setting to `true`.

The native candidate issue is not fixed by this expression-cache change. Its
failing probe is documented here separately from the enabled regression suite;
the expected values have not been weakened to match the incorrect result.
