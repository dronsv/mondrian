# Issue #103 — shape 2 (tuple with a navigated/literal member) — investigator report

reproduced: True · root cause confidence: proven-by-experiment

## Minimal repro

Reproduced on unmodified main 608f823b1 (H2, JUnit 5, own worktree, now removed). The test, raw output and experiment patch are in /home/andrey/work/emodrian_changes/artifacts/issue103-shape2-investigation-20260922/ (gitignored): Issue103Shape2TmpTest.java, issue103-s2-modes-output.txt, experiment-instrumentation.patch.

Fixture: the probe fixture (cube [Navigation], Calendar Year>Month>Week, Product, Store, stored Quantity) plus one row `INSERT INTO fact VALUES (2,2,1,1000)`. P002 then sells 1000 in week 35 and 2 in week 36, so the all-weeks value differs from the week-36 value. Without that row several variants look correct only because each product sells in one week.

Smallest MDX. No navigation, slicer measure or subselect is needed; a literal tuple member is enough:

`WITH MEMBER [Measures].[M] AS ([Measures].[Quantity], [Calendar].[2026].[8].[36]) SELECT {[Measures].[M]} ON COLUMNS, NON EMPTY CrossJoin([Store].[Name].Members,[Product].[Name].Members) ON ROWS FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Navigation])`

- NQE off: (S1,P002)=2.0.
- NQE on: (S1,P002)=1002, `NQE prefetch: hits=4 misses=0`.

The issue's exact MDX, (Quantity, [Calendar].[2026].[9].PrevMember.LastChild) in the slicer:
- NQE off: 4 positions, P002=2.0, P006=6, P010=10, P014=14.
- NQE on: 8 positions. (S1,P002)=1002 plus the week-35 sellers (S1,P008)=0, (S1,P016)=16, (S2,P004)=4, (S2,P012)=12.

Minimization matrix (NQE off vs on, main):

| Variant | Result with NQE on |
|---|---|
| literal member | WRONG |
| PrevMember.LastChild | WRONG |
| literal.NextMember | WRONG |
| ClosingPeriod(Week, literal month) | WRONG |
| CurrentMember.PrevMember under slicer week 36 | WRONG: a union of week-36 prefetch hits (P002=2) and week-35 rows that missed the prefetch |
| CurrentMember.PrevMember under slicer week 37 | correct, only because the prefetch was empty ("empty context, no attachment") |
| without NON EMPTY | WRONG: 48 cells, (S1,P008)=0 and (S2,P004)=4 instead of null |
| measure on axis | WRONG |
| without subselect | WRONG: 12 positions instead of 4 |
| IIF(x=0,NULL,x) guarded form | WRONG: 7 positions |
| literal.LastChild, .Parent, .FirstChild, CurrentMember.Parent | WRONG through a different path (defect 2, see rootCause) |

The axis phase is not affected. The axis before NON EMPTY stripping has the same size with NQE off and on (8 tuples for the issue MDX), and the native crossjoin SQL is identical. The extra positions come from RolapConnection.NonEmptyResult (RolapConnection.java:1155/1186), which keeps a position when its cell is non-null. With NQE on, the cells of the week-35 sellers are non-null and wrong.

## Root cause

The issue title is misleading. The Calendar member of the tuple is not dropped by MeasureClassifier, tryInlineToDirectPush or tuple flattening. The evaluator does apply it. The wrong value originates in the NQE prefetch lookup, whose key ignores it. This is defect 1, the issue's MDX, and it also explains issue shape 1.

Chain on main:

1. Util.java:1091 `q.addMeasuresMembers(olapElement)` adds every measure referenced anywhere in the MDX text to query.getMeasuresMembers(). That includes [Quantity] inside the WITH formula.
2. MeasureClassifier.classify (MeasureClassifier.java:154-190) gives Quantity=DIRECT_PUSH_STORED and M=EVALUATOR ("unsafe function: PrevMember" or "coordinate-changing tuple").
3. NativeQueryEngine.tryCreate (NativeQueryEngine.java:116) creates the engine because `ownable` is non-empty. classifyExecutionMode (:1744-1765) returns PREFETCH_ONLY. The log reads `eligible, measures=[Quantity=DIRECT_PUSH_STORED, M=EVALUATOR]`, `NQE: mode=PREFETCH_ONLY`.
4. executePrefetchOnly (:418-505) runs the stored plan under the slicer evaluator context. The generated SQL is: `SELECT store."name" AS k0, product."name" AS k1, sum(f.qty) AS v0 FROM fact f JOIN "store" store ON f."store_id" = store."id" JOIN "product" product ON f."product_id" = product."id" WHERE product."manufacturer" = 'Red' GROUP BY store."name", product."name"`. It has no Calendar predicate, so it returns all weeks. It attaches the context at :494 ("context attached (8 entries)").
5. In executeBody the evaluator evaluates the tuple: it sets Calendar to week 36 and the measure to Quantity, then calls FastBatchingCellReader.get (:433). lookupFromPrefetch (FastBatchingCellReader.java:293-403) builds the key only from the plan's projected hierarchies, Store and Product (:352-369). It never compares the evaluator's other coordinates with the context the SQL was generated under, and the `request` parameter is unused. It returns the all-weeks value before the correct week-36 segment is consulted. That segment is already in the cache, loaded during the axis phase.

Trace from my instrumentation on main: `PREFETCH-KEY-HIT measure=[Measures].[Quantity] key=[S1, P002] value=1002 evaluatorNonAll=[[Calendar].[2026].[8].[36] [Product].[P002] [Store].[S1]] nonProjectedMatchesBase=false`, and `NQE prefetch: hits=8 misses=0`.

Proof by experiment. All runs use NQE on, across 35 shapes, including issue shape 1 (Sum over stores of ClosingQty = 3000 vs 1000):
- **(a) Prefetch lookup skipped ("nolookup"):** every defect-1 shape becomes correct.
- **(b) Candidate fix, serving a prefetched value only when all non-projected, non-reset evaluator members equal a snapshot of the slicer evaluator members:** correct, and plain cells still hit the prefetch (hits=16 kept).
- **(c) Bisect:** at 8da2508a5 (parent of f89f101e7) all defect-1 shapes are correct ("fallback reason=UNSUPPORTED_MEASURE_PATTERN"). At f89f101e7 they are wrong.

So this is a regression exposed by f89f101e7, "evaluator calc members no longer poison prefetch eligibility". Its claim that PREFETCH_ONLY partial coverage is "safe by construction" is false: a hit is served at any coordinate.

Defect 2 is a separate, older path. It was already wrong at 8da2508a5.
- FormulaAnalyzer.isTupleWithNonMeasureDimension (FormulaAnalyzer.java:287-304) flags a tuple only when an argument is a non-measure MemberExpr (:296).
- A tuple whose member comes from a function that is not in UNSAFE_FUNCTIONS (:59-83) passes as safe. LastChild, FirstChild, Parent, CurrentMember and DefaultMember are all absent from that list.
- The measure is then classified POST_PROCESS_CANDIDATE and the mode is FULL_RESULT: `measures=[M=POST_PROCESS_CANDIDATE, Quantity=DIRECT_PUSH_STORED]`, `mode=FULL_RESULT`.
- ContextBackedCellReader.get (ContextBackedCellReader.java:63-95) serves the leaf at the cell's fixed projected key.
- Example: `[Measures].[Quantity] / ([Measures].[Quantity], [Product].CurrentMember.Parent)` returns Infinity (correct 0.94). (Q, [2026].[8].LastChild) returns 1002 (correct 2).
- The issue's own formula does not take this path, because PrevMember is in the list.

## Proposed fix

**Fix at the source for defect 1.** About 30 lines. I tested it.
- In NativeQueryEngine.executePrefetchOnly (NativeQueryEngine.java:494), pass a snapshot `evaluator.getMembers().clone()` through RolapResult.attachPrefetchContext into FastBatchingCellReader.
- In lookupFromPrefetch, before the containsKey check (FastBatchingCellReader.java:380), walk every hierarchy ordinal from 1 up. If the hierarchy is neither projected nor reset and `!members[i].equals(baseMembers[i])`, `continue`. That is a miss, which falls through to the segment cache and is always correct.
- Result on the fixture: all 28 defect-1 variants plus issue shape 1 are correct.
- The #97 16-shape probe then has 0 differences between NQE off and on. Only shapes 04 and 09 changed versus main-cells.txt; compound slicer, virtual cube and calculated slicer member are unchanged.
- An Excel-style helper query (Intersect(...).Count next to a stored measure) still gets hits=8/8, so the #84 win is kept.
- 182 tests in 16 NQE-related classes passed with the fix on by default. That run was tail-truncated to 10 visible class lines, so NqeAggIntegrationTest and OpeningClosingPeriodContextTest were not confirmed individually.

**Fix for defect 2.** I tested it.
- In FormulaAnalyzer.isTupleWithNonMeasureDimension, treat any tuple argument that is not a measure MemberExpr as coordinate-changing. The pin-tuple recognizer still takes precedence.
- This cures literal.LastChild, the share-of-parent form, .Parent and .FirstChild. They become EVALUATOR, and the defect-1 fix then serves them correctly.
- It is not complete. `Stddev({w35,w36}, Q)` still returns 0.0 (correct 705.7), because the block-list lacks Stddev and the set literal. The durable fix is a type-based allow-list that rejects any sub-expression typed Set, Tuple, non-measure Member, Level or Hierarchy. I did not implement that.

**Conservative guard alternatives in NativeQueryEngine.tryCreate.** Both tested.
- Broad guard: return null when `classification.evaluatorOnly` is non-empty, which is the behaviour before f89f101e7. It cures all defect-1 shapes, including the leaf-key and non-unique-slicer collisions in PREFETCH_ONLY queries. It gives up the #84 prefetch for every Excel query that carries __XLRelated/__XLPath; the helper query on the fixture lost its prefetch entirely.
- Narrow guard: decline only when an EVALUATOR candidate has `normalizedFormula == null` or non-empty leafRefs. It gives the same cures and keeps the Excel-helper prefetch (hits=8). For production it should also treat StrToMember, StrToTuple, StrToSet and LookupCube as "reads measures"; the experiment did not include that.
- Both guards miss two things. One is defect 2. The other is a pin tuple evaluated in PREFETCH_ONLY that was forced by a nativeSql measure rather than by an EVALUATOR measure.

**Trade-off.** The source fix is small and local. Its only possible failure is an extra miss, never a wrong hit. It keeps the performance win. I recommend the lookup fix plus the FormulaAnalyzer tuple strictness, and optionally the narrow guard as belt-and-braces for the canary.

**On fixing before the canary: yes.** This is a regression that is already in the prefetch images. It corrupts ordinary "stored measure next to a share or average measure" reports, and the instant kill-switch costs all of NQE.

**Test coverage.** NqeCoexistenceTest, NqeExecutionModeTest, FormulaAnalyzerTest, MeasureClassifierTest, NativeQueryEngineEligibilityTest, NativeQueryEngineCellMeasureTest, OpeningClosingPeriodContextTest and PostProcessEvaluatorTest cover the area, but none of them asserts coordinate safety of prefetch hits. The fix should add the shapes from Issue103Shape2TmpTest as an H2 regression test with the NQE-off result as the oracle, and add both issue MDX to the NQE regression pack.

## Production exposure

Production runs with queryEngine.enable=true and native.sql.enable=true. I checked lemana.xml by reading the formula text and ran analog shapes on the H2 fixture. Nothing was run against ClickHouse or the real catalog.

Defect 1 triggers when two things hold. First, the query also contains an ownable measure that prefetches the stored measure being read: the stored measure itself, or a ratio measure that depends on it. "Продажи руб" next to "Доля продаж руб %" is the typical report. Second, the formula reads that stored measure at a coordinate that differs from the slicer context on a hierarchy that is not on an axis. Schema-defined formulas alone do not trigger it. The analogs with only the calculated measure in the query were correct, because the engine was not created.

Affected shapes:

1. **`Кол-во ТТ (ср.мес.)` = Avg(Existing [Период.Календарь].[Месяц].Members, [АКБ]).**
   - Ran the analog Avg(Existing weeks, Quantity) next to Quantity under a month slicer.
   - NQE off 501, NQE on 1002: every iteration returns the slicer-context value.

2. **All share measures built as Sum(Existing months, stored-or-tuple).**
   - They are `Доля продаж руб %`, `Доля продаж шт %`, `Доля продаж, руб СКЮ`, `Доля продаж, шт СКЮ`, `Доля подкатегории в категории %` and `Доля региона в ФО %`.
   - Ran the week-level share analog under a month slicer: 94.17% becomes 188.35%, which is ×2, the number of weeks.
   - Ran the month-level share analog with no calendar slicer: 94.17% becomes 470.86%, which is ×5, the number of months.
   - Any production query whose period context spans more than one month and does not have Month on an axis gets the numerator multiplied by the month count.
   - A slicer on Год is safe only by accident: the MULTILEVEL_FIRST_LEVEL_ON_AXIS guard declines NQE. I ran that case and it was correct.

3. **Pin tuples.**
   - They are `ОКБ MDX` (used as the fallback inside `Анти-АКБ`, `Нум. дистрибуция %` and `Нумерическая дистрибуция, %`), `Доля продаж, руб/шт Производитель` and `Доля оффтейка руб/шт`.
   - Ran the analog (Quantity, [Product].[All Products]) under WHERE [Product].[P002] in PREFETCH_ONLY. It returns 1002, the slicer value, instead of 1048.
   - It is wrong only when a pinned hierarchy is constrained in the slicer. When the hierarchy is on an axis, the arity-safe keys produce a miss.

4. **A stored-measure closing-stock form**, IIF((Q,ClosingPeriod(..))=0,NULL,(Q,ClosingPeriod(..))). Ran a schema-defined analog: P002 gives 1002 instead of 2.

Not affected: `Остаток на конец недели, шт`, a tuple of nativeSql members with ClosingPeriod.
- Ran an H2 analog with a templated nativeSql measure, both next to a stored measure and next to the native measure.
- It was identical with NQE off and on, and P002 = 2.0, the week-36 value.
- The tuple is classified EVALUATOR and the native member is evaluated by NativeSqlCalc at the navigated coordinate. It never goes through FastBatchingCellReader.
- The ratio measures (Средняя цена, Маржа %, Оффтейк and so on) change no coordinates and are not affected.

Defect 2 (FULL_RESULT): no lemana.xml formula has that shape while nativeSql is enabled. `ОКБ` uses .DefaultMember tuples but is classified DIRECT_PUSH_NATIVE first. The exposure is ad-hoc client MDX, for example share-of-parent through CurrentMember.Parent.

## Other findings

- Issue #103 shape 1 (Sum over stores of ClosingQty, ×3) has the same root cause as shape 2. Ran it: 3000 vs 1000 on main. It is correct with the lookup skipped, with the candidate fix, and with either guard. It is also correct at 8da2508a5, so it too is a regression from f89f101e7. Trace: Quantity lookups at Store=S1/S2/S3 all hit the key [P002], which carries the all-stores value.
- A third defect, found by experiment (shape L1): executePrefetchOnly builds NativeQuerySqlGenerator without projectedLevelByHierarchy (NativeQueryEngine.java:475), so the prefetch groups by the leaf level. With [Calendar].[Month].Members on the axis the SQL is `SELECT calendar."week" AS k0, sum(f.qty) ... GROUP BY calendar."week"`. lookupFromPrefetch keys by rm.getKey() of whatever level the evaluator member is at. A plain stored cell at [2026].[8] returned week 8's 77777 instead of 1092. The members-equality fix does not cure this, because the hierarchy is projected; both guards do. Possible production collisions are the all-Integer key chains Категория1/2/3 id versus master_sku_key, and numeric-string brand_id versus sku key, in any PREFETCH_ONLY query with a non-leaf level on an axis. I did not check this against data.
- A fourth defect (shapes N1/N2): NativeQuerySqlGenerator.buildMemberPredicate (:1540-1554) emits only the member's own level key. A slicer on [2026].[8] becomes `WHERE calendar."month" = 8` with no year. With uniqueMembers=false and month 8 also present in 2025, a plain stored measure returned 5004 instead of 4, even in FULL_RESULT mode with no calculated measure. It predates f89f101e7. lemana.xml has uniqueMembers=true on every level, so it is not exposed there; other catalogs were not checked.
- The FormulaAnalyzer block-list has other holes besides tuples. `Stddev({set literal}, measure)` is classified POST_PROCESS_CANDIDATE and returns 0.0 (correct 705.7). Neither 'Stddev' nor the set constructor is in UNSAFE_FUNCTIONS, and this stays wrong even with the tuple fix. An allow-list or type-based eligibility check is the durable design.
- Several variants look correct only because of the data, which is why probe shapes 01/03 and the literal measure-on-axis case passed. Without the extra overlap fact row every product sells in a single week, so the all-weeks value equals the week value. The native NonEmpty axis SQL also absorbs a literal tuple member when the measure is on an axis. The regression pack needs fixtures in which the prefetch-context value differs from the navigated-coordinate value.
- The Integer versus Double difference from the issue was confirmed: prefetch hits return the JDBC Integer (2) while the evaluator path returns Double (2.0). It is cosmetic, but it shows at a glance which cells were served from the prefetch.
- Worktree hygiene: Docker Maven leaves root-owned target/ directories, so `git worktree remove --force` fails with 'Permission denied'. I cleaned them up through the maven container, then rmdir and `git worktree prune`. All three of my worktrees are gone, the main checkout and the other pipelines' worktrees were not touched, and nothing was committed or pushed.

## Not run

- Nothing was run against ClickHouse or the real lemana catalog. Production exposure comes from reading the formula text plus analog shapes on the H2 fixture.
- The full Mondrian unit suite was not run with the candidate fix. Only 16 NQE-related classes (182 tests, 0 failures; the tail-truncated output showed 10 class lines, so NqeAggIntegrationTest and OpeningClosingPeriodContextTest were not confirmed individually) and the #97 16-shape probe were run. The Excel regression pack and dual-run hash comparison were not run.
- The performance impact of the candidate fix was not measured. The only evidence is prefetch hit counters on the fixture: plain cells still hit, and the Excel-helper query keeps 8/8 hits.
- No fix was designed or tested for the leaf-level prefetch key collision (shape L1) or for the non-unique slicer-level predicate (shapes N1/N2). Only their existence on main was demonstrated.
- A type-based allow-list for FormulaAnalyzer (the Stddev case) was not implemented or tested. The narrow guard was tested without StrToMember/StrToTuple/StrToSet/LookupCube handling.
- The (Q, Store.DefaultMember) variant under a store slicer is inconclusive. NQE off and on gave the same result because products sold in S2 sell only in S2. It was classified POST_PROCESS_CANDIDATE, so it is presumed affected by defect 2.
- The candidate fix was not exercised with aggregate tables or distinct-count/HLL state plans (STATE_AGGREGATE). Reset-hierarchy pin plans ran only in the pin-tuple shape, and multi-plan virtual-cube queries ran only through probe shape 13.
- The nativeSql tuple check used a simple H2 template (presence subquery with ${factJoins} and ${whereClause}), not the production template or ClickHouse. The native SQL text was not captured by RolapUtil.setHook. That it executed is inferred from the value 2.0, which is neither the -999 formula fallback nor 1002.
