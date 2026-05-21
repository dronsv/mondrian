# Subselect Filter(InStr(member_caption)) — direct SQL pushdown

**Status:** proposal · 2026-05-20 · narrow follow-up to #77 perf regression

## Problem

The #77 engine fix (commits `73d8a3259`, `038b69afb`, `183cfd422`, `16e3dabe2`) correctly restricts the outer axis for `FROM (SELECT Filter(<level>.AllMembers, InStr(<level>.CurrentMember.member_caption, "X") > 0) …)` — the Excel-style label filter.

End-to-end against prod ClickHouse with image `issue77c-bccc7c4` + the current `FitnessShock.generated.xml` catalog, the q02 shape from the issue body returns 28 correct cells in **104.8 s**. Compared to the previous build (`issue77-a30294a` — different fix shape, less correct on `NonEmpty(set, null measure)` but faster on q02) which returned 28 cells in **3.5 s**, that is a ~30× regression on the exact MDX users send from Excel.

### Why it's slow

`evalFallbackDisjunction` compiles the MDX `Filter(...)` and evaluates it in Java. The resulting 27 matching members are converted into 27 single-key `MemberColumnPredicate` instances, wrapped in `OrPredicate`. Container SQL log for q02 shows:

```
SqlTupleReader [Товар] → master_sku_key = 408073  (1 row, 218 ms)
SqlTupleReader [Товар] → master_sku_key = 408399  (1 row, 214 ms)
SqlTupleReader [Товар] → master_sku_key = 408074  (1 row,  42 ms)
… 24 more …
```

Two factors stack:

1. Evaluating `Filter(<level>.AllMembers, …)` over a 1071-row dimension in Java loads the entire `[Товар]` level first (~330 ms × 3 redundant loads visible in the log: `SqlTupleReader` + `SqlMemberSource.getMemberChildren` + repeated `SqlTupleReader`), then iterates each member's caption to apply InStr.
2. The resulting `OrPredicate` of 27 single-column equalities is not folded to `IN (…)`. Each branch becomes its own SqlTupleReader probe through the SSH tunnel.

### What we want

Issue **one** SQL against the dimension table:

```sql
SELECT master_sku_key
FROM dim_fitnessshock_product
WHERE positionUTF8(sku_name, 'CHIKA') > 0
```

— get the 27 matching keys back, and produce a single `ListColumnPredicate(master_sku_key, IN (k1…k27))`. The rest of the query then loads cells with one batched `… WHERE master_sku_key IN (…)` query.

Estimated wall time: O(1) round-trip on the dim table (~50 ms — the dim table is small, no JOIN, indexed) plus normal cell-load.

## Scope (this proposal)

**In scope:** exactly the Excel-style AST pattern below.

```
Filter(
  <hierarchy-or-level-expr>.AllMembers,    // or .Members
  ?op (
    InStr( 1 , <hierarchy-expr>.CurrentMember.Properties("MEMBER_CAPTION") , <string-literal> ) ,
    <integer-literal>
  )
)
```

where:

- `?op` is `>` (with RHS `0`) or `>=` (with RHS `1`). Both encode "contains". The two are semantically equivalent because InStr never returns a negative value, so a single SQL form `positionUTF8(col, lit) > 0` covers them.
- The InStr `start` argument is the literal `1` (Excel's exact emission).
- The property lookup is one of two distinct parse-tree shapes — Mondrian does **not** normalize them:
  - `member.Properties("MEMBER_CAPTION")` parses to a `ResolvedFunCall` whose `getFunName()` = `"Properties"`, with the string literal `"MEMBER_CAPTION"` as the second arg.
  - `member.member_caption` parses to a `ResolvedFunCall` whose `getFunName()` = `"Member_Caption"` (canonical mixed-case from `BuiltinFunTable`), with just the member as the single arg. No string-literal arg.

  The handler MUST detect both names explicitly (case-insensitive compare on the function name; for `Properties`, additionally check the string literal arg).
- The hierarchy in the InStr argument matches the level in the Filter source.

Anything that does not match the pattern continues to fall through to the current `evalFallbackDisjunction`. **No behavior change for other Filter shapes.**

**Out of scope (this proposal):**

- The `<>` ("does not contain") operator. The return type of `expandSubcubePredicateDisjunction` (`List<List<StarPredicate>>`) cannot carry a `NOT` wrapper — `NotPredicate` is applied at the `buildSubcubeAxisPredicate` level (alongside `-{set}` and `Except`) and the current static handlers do not flow through that layer. Supporting `<>` would require adding a new `Filter`-with-NE intercept in `buildSubcubeAxisPredicate` to wrap the result in `NotPredicate`. Excel's primary "contains" filter always emits `> 0`; the "does not contain" variant is uncommon. Deferred — falls through to the existing slow evaluator path with correct results.
- Other position functions (`Position`, `Find`, `Like`, `Mid`, regex)
- Compound conditions (`AND` / `OR` of InStr)
- Non-literal substrings (parameters, calculated members)
- Anything in a deeper expression than the immediate Filter argument
- General `Or(And(col=k)…) → IN(k…)` collapse (see Phased follow-ups)

Phased follow-ups will widen scope as needed; the broader `Or → IN` collapse is a known improvement we are deliberately not bundling.

## Design

### Where it slots in

A new branch inside `Query.expandSubcubePredicateDisjunction` (`mondrian/src/main/java/mondrian/olap/Query.java`), placed alongside the Level-1 static handlers (`Members`, `Children`, `Descendants`):

```java
// Level-1 (new): Filter(level.AllMembers, InStr(... member_caption, "X") op N)
// Resolves to one SQL on the dim table → single ListColumnPredicate.
if ("Filter".equalsIgnoreCase(funName)
    && funCall.getArgs().length == 2)
{
    final List<List<StarPredicate>> instrMatch =
        tryInStrCaptionFilter(baseCube, funCall, ignoredHierarchies);
    if (instrMatch != null) {
        return instrMatch;
    }
    // fall through to dynamic fallback
}
```

If the pattern doesn't match exactly, `tryInStrCaptionFilter` returns `null` and the existing fallback handles it (current behavior preserved).

**Validation prerequisite.** Subcube axes arrive at `expandSubcubePredicateDisjunction` as `UnresolvedFunCall` trees — the parent function (`expandSubcubePredicateDisjunction` → `evalFallbackDisjunction`) explicitly validates via `exp.accept(validator)` before compiling. Pattern-matching on `ResolvedFunCall` shapes (canonical `getFunName()` values like `"Member_Caption"`, resolved `Hierarchy` references) requires the same upfront validation.

The handler MUST validate the Filter expression before AST inspection:

```java
private List<List<StarPredicate>> tryInStrCaptionFilter(
    RolapCube baseCube,
    FunCall filterCall,
    Set<Hierarchy> ignoredHierarchies)
{
    // Validate so getFunName() returns canonical names and hierarchy
    // expressions resolve to actual Hierarchy objects. This costs one
    // accept(validator) call but matches what evalFallbackDisjunction
    // does, and the cost is recovered many times over by skipping the
    // per-member fanout the unoptimized fallback produces.
    final Validator validator = createValidator();
    final Exp resolved;
    try {
        resolved = ((Exp) filterCall).accept(validator);
    } catch (Exception e) {
        return null; // unresolvable — let the fallback try
    }
    if (!(resolved instanceof ResolvedFunCall)) {
        return null;
    }
    final ResolvedFunCall validated = (ResolvedFunCall) resolved;
    if (!"Filter".equalsIgnoreCase(validated.getFunName())
        || validated.getArgs().length != 2)
    {
        return null;
    }

    final Exp setArg = validated.getArg(0);
    final Level level = extractLevelFromAllMembers(setArg);
    if (level == null) return null;
    // Non-unique levels need parent-level constraints to identify
    // members; the key-only predicate this handler emits would
    // under-restrict. Punt to the fallback (which walks parents via
    // expandMemberPredicateConjunction). For the prod FitnessShock
    // schema [Продукт].[Товар] is uniqueMembers="true", so q02 is
    // covered. Phased follow-up extends to non-unique by widening the
    // SELECT to include parent key columns.
    if (!level.isUnique()) return null;

    // arg 1: Binary(op, InStr(1, hier.CurrentMember.Properties("MEMBER_CAPTION"), literal), N)
    final Exp condArg = validated.getArg(1);
    final InStrConditionMatch cond = matchInStrCondition(condArg);
    if (cond == null) return null;

    // hierarchy in the InStr arg must equal the level's hierarchy
    if (cond.hierarchy != level.getHierarchy()) return null;

    // resolve the dim table + name column for the level (single
    // <Table>, not <Join> — see "Multi-table levels" in Risks)
    final NameColumnInfo nameInfo = resolveNameColumn(baseCube, level);
    if (nameInfo == null) return null;

    // run the SQL, get matching keys, wrap as ListColumnPredicate
    return resolveInStrFilterToPredicate(
        baseCube, level, nameInfo, cond, ignoredHierarchies);
}
```

### Pattern detection

The high-level `tryInStrCaptionFilter` shape is shown above in "Where it slots in" (it includes the mandatory `accept(validator)` step and the unique-level guard). The helper signatures:

`extractLevelFromAllMembers` handles `<HierarchyExpr>.AllMembers`, `<DimensionExpr>.AllMembers` (default hierarchy), `<HierarchyExpr>.Members`, and `<LevelExpr>.Members` — same shapes the existing `expandMemberEnumerationFunCall` covers. Operates on the validated tree (`HierarchyExpr` / `LevelExpr` are post-validation node types).

`matchInStrCondition` walks the binary-op tree expected from Excel. Implementation is purely AST inspection (no compile/eval):

```java
private static final class InStrConditionMatch {
    Hierarchy hierarchy;
    String substring;
    // op is always GT-or-GE; both reduce to "found at any position" since
    // InStr returns 0 when not found and ≥1 when found. No NE/<> here —
    // see "Out of scope" above for why.
}
```

We accept `>` (with RHS `0`) and `>=` (with RHS `1`). Both are normalized to a single SQL form `positionUTF8(col, lit) > 0`.

The caption-extraction step recognizes the two distinct parser shapes (Mondrian does not normalize them — the two forms produce different `getFunName()` values):

- `member.Properties("MEMBER_CAPTION")` → `ResolvedFunCall` with `getFunName()` = `"Properties"`, args = `[<MemberExpr>, StringLiteral("MEMBER_CAPTION")]`. Match: function name `"Properties"` **and** second arg is a string literal equal to `"MEMBER_CAPTION"` (case-insensitive). The member expression is the first arg.
- `member.member_caption` → `ResolvedFunCall` with `getFunName()` = `"Member_Caption"` (canonical, mixed-case via `BuiltinFunTable`), args = `[<MemberExpr>]`. Match: function name `"Member_Caption"` (case-insensitive). The member expression is the single arg.

The hierarchy is then extracted from the matched member-expression argument and compared against the Filter source's level's hierarchy. The two AST shapes are NOT currently tested in `SubcubePredicateParsingTest`; this proposal adds parser-level tests for both (see Tests section).

### Name column resolution

`Level` exposes `nameExp` / `keyExp`. For a level declared

```xml
<Hierarchy ...>
  <Table name="dim_fitnessshock_product"/>
  <Level column="master_sku_key" nameColumn="sku_name" ... />
```

we need:

- the **key column** (`master_sku_key`) — for the resulting predicate's column reference
- the **name column** (`sku_name`) — for the LIKE predicate
- the **dim table** (`dim_fitnessshock_product`) — for the FROM

```java
private static final class NameColumnInfo {
    RolapStar.Column keyColumn;     // for the StarPredicate
    MondrianDef.Expression keyExpr; // for the SELECT
    MondrianDef.Expression nameExpr;// for the WHERE
    MondrianDef.RelationOrJoin from;// for the FROM
}
```

If the level has no `nameColumn`, we can't run the LIKE pushdown — return null and let the fallback handle it (every member's caption equals its key under MDX defaults, which is a separate edge case).

### SQL generation

Use the cube's `RolapStar` / `SqlQuery` infrastructure (the same path `SqlMemberSource` already uses for `getMemberChildren`). Build:

```sql
SELECT <keyExpr> AS k
FROM <dim-table>
WHERE <dialect-specific-position-fn>(<nameExpr>, '<substring>') <op-mapped> <rhs>
```

where `<dialect-specific-position-fn>` is dialected:

| Dialect | Function |
|---|---|
| ClickHouse | `positionUTF8(<col>, '<lit>')` — 1-based, returns 0 when not found |
| MySQL | `LOCATE('<lit>', <col>)` — same semantics |
| H2 | `INSTR(<col>, '<lit>')` — 1-based |
| Generic / Other | `POSITION('<lit>' IN <col>)` — SQL-standard |

Dialect is read from `RolapStar.getSqlQueryDialect()` (already used elsewhere in the predicate-rendering path). If the dialect isn't recognized we return null and fall back.

The string literal is dialect-quoted via `SqlQuery.getDialect().quoteStringLiteral(buf, substring)` (or the dialect's equivalent — the existing `Dialect` interface exposes string-literal quoting). This handles both single-quote escaping (`'` → `''`) and dialect-specific concerns like MySQL's non-strict-mode backslash interpretation that `Util.singleQuoteString` (the more general helper at `Util.java:1322`) does not address. The substring originates from an MDX literal in the user query, so injection through a malicious request body is the threat model; backslash-misinterpretation is the semantic-correctness concern.

### Resulting StarPredicate

```java
// Execute the query through RolapStar's connection
List<Object> keys = executeKeyList(baseCube.getStar(), sql);

if (keys.isEmpty()) {
    // empty match — feed the existing emptySetDisjunction() (LiteralStarPredicate.FALSE)
    return emptySetDisjunction();
}
if (keys.size() > EVAL_MEMBER_LIMIT) {
    // refuse to inline a huge IN list. Return null (NOT
    // noConstraintDisjunction) so expandSubcubePredicateDisjunction
    // actually falls through to evalFallbackDisjunction. Returning
    // noConstraintDisjunction here would emit an empty conjunction
    // that buildSubcubeAxisPredicate treats as "no restriction" —
    // producing the full unrestricted axis (the original #77 bug).
    return null;
}

final List<StarColumnPredicate> branches = new ArrayList<>(keys.size());
for (Object key : keys) {
    branches.add(new ValueColumnPredicate(nameInfo.keyColumn, key));
}
final StarPredicate inList = new ListColumnPredicate(nameInfo.keyColumn, branches);

// Wrap in disjunction-of-one to match the return shape.
final List<StarPredicate> conjunction = Collections.singletonList(inList);
return Collections.singletonList(conjunction);
```

Downstream consumers see one `ListColumnPredicate` over one column → `SqlTupleReader` emits one batched `master_sku_key IN (…)`.

### Edge cases

| Case | Handling |
|---|---|
| Pattern doesn't match | Return null, fall through to evalFallbackDisjunction (current behavior) |
| Filter expression fails validation | Return null — fallback |
| Level is non-unique (`uniqueMembers="false"`) | Return null — fallback (current `expandMemberPredicateConjunction` walks parent levels to disambiguate) |
| Level is on a snowflake `<Join>`, not flat `<Table>` | Return null — fallback (this proposal's SQL emitter assumes single-table) |
| No matching keys | `emptySetDisjunction()` — already correct per the recent FALSE-predicate fix |
| > EVAL_MEMBER_LIMIT (5000) keys | Return **null** — fall through to evalFallbackDisjunction. Do NOT return `noConstraintDisjunction()` (that emits an empty conjunction which `buildSubcubeAxisPredicate` treats as "no restriction" → returns the full axis, the original #77 bug). |
| Level has no nameColumn | Pattern doesn't match (no LIKE column to push to) — return null |
| Dialect doesn't have a position fn | Pattern doesn't match — return null |
| `<>` operator | Out of scope. Falls through to evalFallbackDisjunction. See "Out of scope" above. |
| Multiple Filter conditions (`AND` / `OR` of InStr) | Out of scope this proposal — falls back |
| Filter source uses a measure (e.g., `Filter([X].Members, [Measures].[Sales] > 100)`) | Doesn't match the InStr pattern — fallback (and that case actually NEEDS the live-evaluator path) |
| Negation: `-Filter(...)` | The Filter arg is unwrapped first by `buildSubcubeAxisPredicate`'s `-` handler, which recurses. Our handler matches the inner Filter; the outer `-` wraps the result in `NotPredicate`. Free correctness. |

### Tests

**Parser-level (no DB)** — add to `mondrian/src/test/java/mondrian/olap/SubcubePredicateParsingTest.java`:

1. **`testFilterInStrPropertiesParse`** — asserts the prod MDX shape `Filter([X].AllMembers, InStr(1, [X].CurrentMember.Properties("MEMBER_CAPTION"), "lit") > 0)` parses to the expected FunCall tree (top `Filter`, inner `>`, inner-inner `Properties` with `MEMBER_CAPTION` literal, etc.). Lock in the AST contract the handler relies on.

2. **`testFilterInStrMemberCaptionParse`** — same shape but using the bare `.member_caption` accessor. Asserts `getFunName()` = `"Member_Caption"` (canonical-cased), confirming the two AST shapes are distinct and the handler must match both names explicitly.

**Behavior + SQL-emission (DB required)** — add to `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`:

3. **`testInStrSubselectIsBatchedSql`** — same MDX as the existing `testSubselectFilterInStrRestrictsOuterAxisWithNqe`, but install a SQL hook via `mondrian.rolap.RolapUtil.setHook` (precedent: `mondrian/src/it/java/mondrian/rolap/DataSourceChangeListenerTest`) to capture every JDBC statement that fires during execution. After execute: assert (a) exactly one captured statement matches the dialect's position-function pattern (`INSTR(sku_name, 'Carrots')` on H2 / `positionUTF8` on ClickHouse), (b) zero statements match the per-member bare-equality pattern `master_sku_key = \d+`. This is the test that distinguishes the optimized path from the old slow path — cell counts alone don't, since both produce 27/28 cells.

4. **`testInStrSubselectFallsBackOnUnsupportedShape`** — `Filter([X].Members, InStr(...member_caption...) > 0 AND [Measures].[Unit Sales] > 100)` (compound condition). The InStr handler must return null; the existing fallback must produce a correct (smaller) result. SQL hook asserts no `INSTR`-style query was emitted by the handler.

5. **`testInStrSubselectEmptySubstringReturnsEmptyAxis`** — `InStr(...) > 0` with a substring that exists in zero members. Asserts the `emptySetDisjunction()` → 0 outer-axis members (already covered for the dynamic fallback by `testSubselectFilterWithNoMatchesReturnsEmptyAxis`; this is the static-handler equivalent — assert via SQL hook that the static handler ran, not the fallback).

6. **`testInStrSubselectOversizedFallsBack`** — synthetic test that drives `tryInStrCaptionFilter` past `EVAL_MEMBER_LIMIT` (use a substring matching every product, e.g. empty string or a common substring on a level with > 5000 members). SQL hook asserts the InStr-style query ran once, returned > limit keys, then the handler returned null and the legacy evalFallbackDisjunction ran. Cell count must match what `testSubselectFilterInStrRestrictsOuterAxisWithoutNqe`-style fallback produces — proves we don't accidentally return the full unrestricted axis.

7. **`testInStrSubselectOnNonUniqueLevelFallsBack`** — pick a FoodMart level that has `uniqueMembers="false"` (or omit the attribute), apply the InStr filter, assert the static handler returned null and the fallback produced the correct restricted set. Guards against accidentally enabling the fast path on a non-unique level and silently dropping the parent-level constraints.

Live verification continues against the prod CH stack via `scripts/run_issue77_regression.sh` — expected q02 wall time drops from ~105 s to single-digit seconds.

### What this proposal does NOT change

- `evalFallbackDisjunction`, `expandSubcubePredicateDisjunction` other handlers, `RolapEvaluator.getSubcubePredicate` — unchanged.
- The `LiteralStarPredicate.FALSE` empty-set handling — preserved.
- The live-evaluator threading for `NonEmpty` / `TopCount` — preserved (those still need it).
- Existing tests — all 7 in `SubcubeFilterPushdownIT` should keep passing. The new q01-q04 acceptance pack against prod CH should pass with q02 in single-digit seconds.

## Caching — out of scope this proposal

A `subcubePredicateCache: Map<SubcubePredicateCacheKey, StarPredicate>` was prototyped on the `Query` instance in the working tree (uncommitted, see `git -C mondrian diff HEAD -- mondrian/src/main/java/mondrian/olap/Query.java`). Memoizes `getSubcubePredicates(baseCube, ignoredHierarchies, fallbackEvaluator)` by `(baseCubeName, ignoredHierarchyNames)`.

**That cache is unsafe and must be reverted as part of this proposal.** Two distinct correctness gaps:

1. **Stale across mutations.** `Query.setParameter` (line 986) and `Query.setSlicerAxis` (line 892) can mutate state that flows into Filter / NonEmpty / TopCount evaluation. The cache has no invalidation hooks. Already a footgun in `evalCache` (no `setParameter` clear there either) — we shouldn't add a second.
2. **Stale within one execution.** Even with Execution-scoping, the key `(baseCubeName, ignoredHierarchyNames)` doesn't include the evaluator state. `getSubcubePredicates` now accepts a `fallbackEvaluator` and uses `fallbackEvaluator.push()` to evaluate context-dependent subcube expressions:
   - `Filter([X].Members, [Measures].[Sales] > 100)` — depends on slicer / axis evaluator state at the call site.
   - `NonEmpty(set, measure)` — depends on the CellReader and live evaluator state.
   - `TopCount(set, n, measure)` — depends on evaluator state.

   Two `evaluator.getSubcubePredicate()` calls during the same Execution can land on different evaluator states; caching by base-cube key would return the first call's predicate on the second call, silently producing a wrong subset. The InStr-only path is context-independent (purely AST + dim-table metadata), but the cache is currently shared with the dynamic fallback that is *not* context-independent.

A correct general cache requires either:

- restricting the cache to **context-independent predicate builders** (static handlers: Members / Children / Descendants / this new InStr handler) and explicitly bypassing it for `evalFallbackDisjunction`, or
- a **dependency signature** in the key that captures the evaluator-context inputs the underlying predicate evaluation actually reads (current member of each hierarchy, current measure, slicer tuple). Constructing that signature correctly is a non-trivial design — likely larger than this whole proposal.

Either way, that work is out of scope here.

### Action for this proposal

1. **Revert the working-tree cache prototype.** Remove the `subcubePredicateCache` field on `Query`, the `getSubcubePredicates` wrapper, and the `SubcubePredicateCacheKey` inner class. Keep the rest of the engine fix.
2. The InStr-pushdown handler ships uncached. The dim-table SQL it issues (~50 ms per call, 5-10 calls per MDX execution = ≤ 500 ms total) is still negligible vs. the 105 s pre-fix baseline.
3. Caching becomes phased follow-up #6 with a specific safer first target (see below).

## Phased follow-ups

If this proposal lands and we want to widen later:

1. **Generalize `Or(And(col=k))…` folding** in `buildSubcubeAxisPredicate` — collapses any disjunction of single-column-equality predicates into a `ListColumnPredicate`. Helps cases where the subcube produces many single-key predicates by other means (explicit member set, named set, etc.).
2. **Other position functions** — `Position`, `Like`-style wildcards, `Mid`. Same handler shape, different AST keyword set.
3. **Compound conditions** — `AND` / `OR` over multiple InStr or InStr + measure threshold. Each leaf produces a sub-predicate; combine via existing `AndPredicate` / `OrPredicate`.
4. **Non-unique levels** — widen the SELECT to include parent-level key columns and emit a compound predicate so the fast path covers non-unique levels.
5. **Snowflake `<Join>` levels** — emit FROM + JOIN clauses; reuse `SqlMemberSource`'s join builder.
6. **Execution-scoped cache, narrowest-first target.** Start with the **InStr handler's keys** only, not the general `getSubcubePredicates` output. Key by `(baseCube, level, dialect, position-fn, substring literal, operator, rhs)` — every input the handler reads to produce its keys. This is context-independent by construction (the handler never touches the evaluator). Live on `Execution`. Once that lands and bakes, separately consider adding context-independent caching for the other static handlers (Members / Children / Descendants). Only after that, if benchmarks show it's worth the cost, design a dependency-signature-keyed cache for `evalFallbackDisjunction` results — and even then, the signature work is substantial and likely a separate proposal.

Each follow-up is independent and can ship without revisiting this proposal's contract.

## Risks

- **SQL injection / backslash semantics.** The substring literal must be dialect-quoted. Use `SqlQuery.getDialect().quoteStringLiteral(...)` (or the `Dialect`-exposed equivalent), never raw concat and never `Util.singleQuoteString` (which only handles `'` → `''` and ignores backslash, which MySQL non-strict mode treats as an escape character). Add a unit test with `'` and `\` in the substring to confirm proper quoting.
- **Dialect coverage.** ClickHouse / MySQL / H2 / Postgres / Generic covered (functions `positionUTF8` / `LOCATE` / `INSTR` / `POSITION` respectively). Rare dialects fall back. Acceptable.
- **Multi-table levels (snowflake).** If the level's dimension is defined via `<Join>` instead of `<Table>`, the SQL needs FROM + JOIN, not a single `FROM <dim-table>`. **Scope decision: snowflake levels are out of scope for this proposal.** The handler returns null (falls back) when the level's `<Hierarchy>` content is anything other than a `<Table>` element. The faulting prod schema (`dim_fitnessshock_product`) is a flat table, so q02 is unaffected. Snowflake support is a phased follow-up.
- **Caption vs. uniqueName.** Mondrian's `MEMBER_CAPTION` is the level's `nameColumn`. The `MEMBER_UNIQUE_NAME` is a different property that includes parent levels — if Excel ever emits a filter on UNIQUE_NAME instead, the pattern won't match and we'll fall back (correct behavior). No surprise.
- **Sort stability.** The SQL doesn't specify ORDER BY; the resulting key set is unordered. The downstream subcube predicate is a SET — ordering doesn't affect semantics. Acceptable.
- **Re-entry / caching.** Each call to `evaluator.getSubcubePredicate()` will re-run the dim-table SQL. For a single MDX execution this can be 5-10 calls. At ~50 ms each (well under the previous 105 s baseline) this is acceptable; phased follow-up #6 eliminates it via the narrowest-first Execution-scoped cache for the InStr handler's output specifically. The new handler does **not** need to use the `inEvalFallback` guard because it never compiles or evaluates an MDX expression — it issues a single JDBC statement via the star's connection. Re-entry through native evaluators (the original `inEvalFallback` trigger) cannot reach this path.
- **JDBC connection acquisition.** Use the same machinery `SqlMemberSource.getMemberChildren` uses — acquire a connection via `RolapStar.getJdbcConnection()` (or the existing `executeSqlQuery` helper, depending on which is the public API in this branch). Do not bypass into a fresh `DataSource`-level call.

## Open questions

1. Should we add a feature flag (`mondrian.rolap.subcube.inStrPushdown.enable`, default `true`)? My read: no — it's strictly additive, fails closed to the existing fallback, and adding flags multiplies test matrix. But if any reviewer is worried about a rollout-pause path, the flag is cheap.
2. The `start` argument of InStr is currently restricted to literal `1`. Excel always emits `1`. We mirror Excel's exact emission; any other constant falls through to the fallback. Lock this in via the parser-level test.
3. The `ValueColumnPredicate` constructor variants vary across the codebase. Implementation must reference the constructor that matches `expandMemberPredicateConjunction`'s `MemberColumnPredicate(column, rolapCubeMember)` pattern (extends `ValueColumnPredicate`) so the resulting predicate behaves identically to a hand-coded member set under the existing `SqlTupleReader` IN-list rendering. Closing-task confirmation, not blocking.
4. Caching (phased follow-up #6) — should we file an issue immediately so the per-call repeated dim-table SQL is at least tracked? Cheap, prevents drift. My recommendation: yes, file alongside the merge.

## Verification matrix (before merge)

- `./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT` — must be 12/12 (existing 7 + 5 new IT tests with SQL-hook assertions: batched-SQL, fallback-on-compound, empty-substring, oversized-overflow-fallback, non-unique-level-fallback)
- `./scripts/test.sh SubcubePredicateParsingTest` — must be 17/17 (existing 15 + 2 new parser-level AST tests for the InStr/`.member_caption` vs `.Properties("MEMBER_CAPTION")` shapes)
- `./scripts/test.sh` — no new failures vs main
- Live against prod CH via `scripts/run_issue77_regression.sh`:
  - q01 direct-axis Filter — ≤ 2 s
  - q02 subselect Filter — ≤ 5 s (target; was 105 s on issue77c-bccc7c4)
  - q03 TopCount subselect — ≤ 3 s (unchanged path)
  - q04 NonEmpty(Filter) subselect — ≤ 3 s (unchanged path)
- Container SQL log for q02 contains exactly one `positionUTF8` (or dialect equivalent) statement and one `IN (…)` cell-load statement; no per-member `master_sku_key = X` SqlTupleReader probes.

## Tag for the resulting image

`issue77d-<short-sha>` after the change lands on dronsv/emondrian-clickhouse main + a fresh outer submodule bump.
