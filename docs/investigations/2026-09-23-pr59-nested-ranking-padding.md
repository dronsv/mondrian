# PR 59 nested ranking padding investigation

Compared base `3d5d2cc9c6b283e35ecd7be3090f1f1131f7bc51` and PR head
`fda8af662ed12525f8a5af0092d47891ae14702a` using fresh H2 fixtures and
Java 25 through `scripts/test.sh`, serialized with
`flock /tmp/emondrian-correctness-build.lock`. No production queries were run.

## Confirmed regression retained as a test

`OpeningClosingPeriodContextTest` has A (Red, Quantity 10), B (Blue,
Quantity 20), and C (Red, no sales). This query must retain A and C,
whose displayed cells both equal 1:

```mdx
WITH MEMBER [Measures].[One] AS 1
SELECT {[Measures].[One]} ON 0,
       NON EMPTY [Product].[Name].Members ON 1
FROM (SELECT TopCount([Product].[Name].Members, 2, [Measures].[Quantity])
      ON 0 FROM (SELECT {[Product.Manufacturer].[Red]}
                 ON 0 FROM [Navigation]))
```

With NQE disabled, base returns `[A, C]` with native sets both disabled
and enabled. PR head returns `[A, C]` with native sets disabled but `[A]`
with them enabled. The ranking SQL retains `manufacturer = 'Red'` and
selects A. Its padding SQL only excludes A, selects B, and drops C.
The final member query intersects `{A, B}` with Red and returns only A.

The regression test is `nestedTopCountSubselectPaddingRetainsInnerManufacturer`.
It asserts literal member names and values for both native-set modes.
Existing `NqeSubselectAggregateTest.rankedSubselectAxisKeepsItsRanking`
continues to require FULL_RESULT for the simple sole-axis TopCount and
Head(Order(...)) shapes.

The correction permits the padding exemption only while resolving the
query's sole complete raw subcube axis expression. Expression identity
is intentional: the raw AST node passed into dynamic evaluation must
be the whole axis. Nested axes, sibling axes, and compound expressions
keep the existing veto. Re-entry leaves the active expression unchanged;
the owning evaluation clears it in `finally`, before reading the final
miss count, including on exceptions.

## Pre-existing failures retained as investigation evidence

These diagnostic probes are not committed as failing regression tests,
and their expectations were not changed to accept the observed results.
Here, "pre-existing" means present in the combined release baseline
`3d5d2cc9c6b283e35ecd7be3090f1f1131f7bc51`; presence on engine main
has not been established.

1. In `NqeSubselectAggregateTest`, use the existing 16-product fixture
   and either ranked set below in this query:

   ```mdx
   SELECT {[Measures].[Quantity]} ON COLUMNS,
          Existing [Product].[Name].Members ON ROWS
   FROM (SELECT <ranked-set> ON COLUMNS
         FROM (SELECT {[Product].[P007], [Product].[P011]}
               ON COLUMNS FROM [Navigation]))
   ```

   Ranked sets: `TopCount([Product].[Name].Members, 2, [Measures].[Quantity])`
   and `Head(Order([Product].[Name].Members, [Measures].[Quantity], BDESC), 2)`.
   P007 and P011 both have no fact rows. Expected rows were `[P007, P011]`;
   both revisions returned `[]` for both ranking spellings, with NQE off
   and on. Each revision ran 2 parameterized cases and reported 2 failures,
   0 errors. The native predicate combined `{P001, P002}` with `{P007, P011}`.
   This is not a PR 59 regression.

2. Substitute `Head(Order([Product].[Name].Members, [Measures].[Quantity],
   BDESC), 2)` into the Red-manufacturer query above. Both revisions return
   `[A]`, with native sets off and on (NQE off). The literal expected rows
   were `[A, C]`. This behavior is pre-existing and remains outside this fix.

The combined Red differential had 4 cases (TopCount/Head times native-set
off/on): base had 2 failures (both Head cases); PR head had 3 failures
(the same Head cases plus native TopCount). Fresh connections were used
for every case. Raw local logs were `/tmp/pr59-red-padding-base-20260923.log`,
`/tmp/pr59-red-padding-head-20260923.log`,
`/tmp/pr59-nested-padding-base-20260923.log`, and
`/tmp/pr59-nested-padding-probe-20260923.log`.

Member-limit omissions above 5,000 members, broad nested-subselect
semantics, and the pre-existing Head/all-null cases were not fixed or
expanded into the regression scope. ClickHouse and XMLA acceptance of
this correction remain release checks.


## Validation of the correction

The focused Java 25/H2 run completed with 182 tests, 0 failures, 0 errors,
and 0 skipped:

```sh
flock /tmp/emondrian-correctness-build.lock ./scripts/test.sh 'SubcubePredicateParsingTest,SqlConstraintUtilsSubcubePredicateTest,RolapNativeTopCount*,NqeSubselectAggregateTest,OpeningClosingPeriodContextTest'
```

The complete unit run completed with 1,596 tests, 0 failures, 0 errors,
and 0 skipped:

```sh
flock /tmp/emondrian-correctness-build.lock ./scripts/test.sh
```

The new test checks literal `[A, C]` rows with `[1, 1]` cells in both
native-set modes. Existing tests continue to require FULL_RESULT for
simple sole-axis ranked subselects. The exception/re-entry reset was also
reviewed in source: re-entry returns before assigning the active expression,
and the owner clears it as the first statement in `finally`, before
miss accounting. No exception-injection test was added.

Local validation logs: `/tmp/pr59-scoped-padding-focused-20260923.log`
and `/tmp/pr59-scoped-padding-unit-20260923.log`. These unit results do not
establish ClickHouse/XMLA acceptance or resolve the out-of-scope cases above.
