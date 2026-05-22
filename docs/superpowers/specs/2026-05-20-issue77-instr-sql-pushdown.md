# Subselect caption label filters — direct SQL pushdown

**Status:** implemented · broadened 2026-05-22 for Excel label-filter variants. The original narrow `InStr(...MEMBER_CAPTION...) > 0` handler has been generalized to `Query.tryCaptionFilter`: contains / does-not-contain, equals / does-not-equal, begins-with / does-not-begin-with, ends-with / does-not-end-with, lexical range predicates, and between / not-between compositions over `CurrentMember.Name`, `CurrentMember.Caption`, `CurrentMember.Member_Caption`, and `Properties("MEMBER_CAPTION"|"MEMBER_NAME")`. `InStr` predicates honor Mondrian's `CaseSensitiveMdxInstr` setting. Unsupported evaluator-dependent filters still fall back.

**Implementation update:** the shipped design keeps the label predicate in SQL as `SqlInSubqueryPredicate`: the fact or aggregate key is constrained with `key IN (SELECT DISTINCT dim_key FROM dim WHERE caption_predicate)`. It does **not** execute the dimension SQL in Java and does **not** materialize a key list for label filters.

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
SELECT DISTINCT master_sku_key
FROM dim_fitnessshock_product
WHERE positionUTF8(sku_name, 'CHIKA') > 0
```

and keep it embedded in the measure/member SQL:

```sql
... WHERE fact.master_sku_key IN (
  SELECT DISTINCT master_sku_key
  FROM dim_fitnessshock_product
  WHERE positionUTF8(sku_name, 'CHIKA') > 0
)
```

This avoids both the Java-side level scan and huge generated `IN (k1, k2, ...)` lists.

Estimated wall time: O(1) round-trip on the dim table (~50 ms — the dim table is small, no JOIN, indexed) plus normal cell-load.

## Scope

**In scope:** Excel-style subselect label filters over a unique, single-table level:

```
Filter(
  <hierarchy-or-level-expr>.AllMembers,    // or .Members
  <caption-or-name-predicate>
)
```

Supported predicate shapes:

- Contains: `InStr(1, caption, "x") > 0`, `>= 1`, or `<> 0`.
- Does not contain: `InStr(1, caption, "x") = 0`, `<= 0`, or `< 1`.
- Begins with: `InStr(1, caption, "x") = 1` or `Left(caption, Len("x")) = "x"`.
- Does not begin with: `InStr(1, caption, "x") <> 1` or `Left(caption, Len("x")) <> "x"`.
- Ends with / does not end with: `Right(caption, Len("x")) = "x"` / `<> "x"`.
- Equals / does not equal: `caption = "x"` / `<> "x"`.
- Lexical range: caption comparisons (`>`, `>=`, `<`, `<=`) and same-hierarchy `AND` / `OR` combinations, including Excel "between" and "not between" forms.

Accepted caption/name accessors:

- `member.Properties("MEMBER_CAPTION")`
- `member.member_caption` / `member.Caption`
- `member.Properties("MEMBER_NAME")`
- `member.Name`

The hierarchy in the predicate must match the Filter source level's hierarchy. Anything else continues to fall through to the current `evalFallbackDisjunction`.

`InStr` SQL lower-cases both sides when `CaseSensitiveMdxInstr=false` (Mondrian default), matching the dynamic VBA implementation instead of relying on database collation.

**Out of scope:**

- Arbitrary evaluator-dependent predicates, especially measure filters such as `Filter([X].Members, [Measures].[Sales] > 100)`.
- Case-transform wrappers (`LCase`, `UCase`) and regex / wildcard operators (`Matches`, `Like`).
- Non-literal substrings (parameters, calculated members)
- General `Or(And(col=k)…) → IN(k…)` collapse (see Phased follow-ups)

Phased follow-ups will widen scope as needed; the broader `Or → IN` collapse is a known improvement we are deliberately not bundling.

## Design

### Where it slots in

A new branch inside `Query.expandSubcubePredicateDisjunction` (`mondrian/src/main/java/mondrian/olap/Query.java`), placed alongside the Level-1 static handlers (`Members`, `Children`, `Descendants`):

```java
// Level-1: Filter(level.AllMembers, <caption predicate>)
// Resolves to one SQL subquery predicate on the dim table.
if ("Filter".equalsIgnoreCase(funName)
    && funCall.getArgs().length == 2)
{
    final List<List<StarPredicate>> captionMatch =
        tryCaptionFilter(baseCube, funCall, ignoredHierarchies);
    if (captionMatch != null) {
        return captionMatch;
    }
    // fall through to dynamic fallback
}
```

If the pattern doesn't match safely, `tryCaptionFilter` returns `null` and the existing fallback handles it (current behavior preserved).

**Validation prerequisite.** Subcube axes arrive at `expandSubcubePredicateDisjunction` as `UnresolvedFunCall` trees — the parent function (`expandSubcubePredicateDisjunction` → `evalFallbackDisjunction`) explicitly validates via `exp.accept(validator)` before compiling. Pattern-matching on `ResolvedFunCall` shapes (canonical `getFunName()` values like `"Member_Caption"`, resolved `Hierarchy` references) requires the same upfront validation.

The handler MUST validate the Filter expression before AST inspection:

```java
private List<List<StarPredicate>> tryCaptionFilter(
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

    // arg 1: supported caption/name predicate
    final Exp condArg = validated.getArg(1);
    final CaptionPredicateNode cond = matchCaptionFilterCondition(condArg);
    if (cond == null) return null;

    // hierarchy in the predicate must equal the level's hierarchy
    if (cond.getHierarchy() != level.getHierarchy()) return null;

    // Build SELECT DISTINCT <dim-key> FROM <dim-table>
    // WHERE <caption predicate>, then keep it as a StarPredicate.
    return buildSqlInSubqueryPredicate(baseCube, level, cond);
}
```

### Pattern detection

The high-level `tryCaptionFilter` shape is shown above in "Where it slots in" (it includes the mandatory `accept(validator)` step and the unique-level guard). The helper signatures:

`extractLevelFromAllMembers` handles `<HierarchyExpr>.AllMembers`, `<DimensionExpr>.AllMembers` (default hierarchy), `<HierarchyExpr>.Members`, and `<LevelExpr>.Members` — same shapes the existing `expandMemberEnumerationFunCall` covers. Operates on the validated tree (`HierarchyExpr` / `LevelExpr` are post-validation node types).

`matchCaptionFilterCondition` walks the binary-op tree expected from Excel. Implementation is purely AST inspection (no compile/eval):

```java
private interface CaptionPredicateNode {
    Hierarchy getHierarchy();
    CaptionValueKind getValueKind(); // CAPTION or NAME
    CaptionPredicateNode invert();
    CaptionPredicateNode positiveComplement();
    String toSql(String nameSql, String positionFn, Dialect dialect);
}
```

`InStr` contains variants are normalized to `positionUTF8(col, lit) > 0` (or dialect equivalent). Simple negative variants (`not contains`, `not equals`, `not begins`, `not ends`) are resolved by querying the positive complement and wrapping the resulting SQL subquery predicate in `NotPredicate`.

The caption-extraction step recognizes the two distinct parser shapes (Mondrian does not normalize them — the two forms produce different `getFunName()` values):

- `member.Properties("MEMBER_CAPTION")` → `ResolvedFunCall` with `getFunName()` = `"Properties"`, args = `[<MemberExpr>, StringLiteral("MEMBER_CAPTION")]`. Match: function name `"Properties"` **and** second arg is a string literal equal to `"MEMBER_CAPTION"` (case-insensitive). The member expression is the first arg.
- `member.member_caption` → `ResolvedFunCall` with `getFunName()` = `"Member_Caption"` (canonical, mixed-case via `BuiltinFunTable`), args = `[<MemberExpr>]`. Match: function name `"Member_Caption"` (case-insensitive). The member expression is the single arg.

The hierarchy is then extracted from the matched member-expression argument and compared against the Filter source's level's hierarchy. `SubcubePredicateParsingTest` locks the parser-level AST contract for both `Properties("MEMBER_CAPTION")` and bare `.member_caption` shapes.

### Key and caption expression resolution

`Level` exposes `nameExp` / `keyExp`. For a level declared

```xml
<Hierarchy ...>
  <Table name="dim_fitnessshock_product"/>
  <Level column="master_sku_key" nameColumn="sku_name" ... />
```

we need:

- the **key column** (`master_sku_key`) — for the resulting predicate's column reference
- the **name column** (`sku_name`) — for the caption predicate
- the **dim table** (`dim_fitnessshock_product`) — for the FROM

The predicate column is the level's base star key column. The subquery select expression is the level key expression. The caption/name expression is chosen as:

1. `captionExp` for `MEMBER_CAPTION` / `Caption`, if declared.
2. `nameExp`, if declared.
3. `keyExp` fallback.

The key fallback is intentional: if a schema uses a human-readable key, MDX caption/name semantics fall back to that key string and the SQL predicate remains equivalent.

### SQL generation

Use the cube's `RolapStar` / `SqlQuery` infrastructure (the same path `SqlMemberSource` already uses for `getMemberChildren`). Build:

```sql
SELECT DISTINCT <keyExpr> AS k
FROM <dim-table>
WHERE <caption predicate over nameExpr>
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
final String subquerySql = buildCaptionKeySubquery(...);
StarPredicate predicate =
    new SqlInSubqueryPredicate(starKeyColumn, subquerySql);
if (conditionWasConvertedToPositiveComplement) {
    predicate = new NotPredicate(predicate);
}
return Collections.singletonList(
    Collections.<StarPredicate>singletonList(predicate));
```

Downstream consumers see one `SqlInSubqueryPredicate` over one column. The legacy SQL path renders it through `StarPredicate.toSql`; the native query engine renders it through `NativeQuerySqlGenerator`, resolving the predicate column through `ResolvedTable` so aggregate tables can use denormalized key columns without a dimension JOIN.

### Edge cases

| Case | Handling |
|---|---|
| Pattern doesn't match | Return null, fall through to evalFallbackDisjunction (current behavior) |
| Filter expression fails validation | Return null — fallback |
| Level is non-unique (`uniqueMembers="false"`) | Return null — fallback (current `expandMemberPredicateConjunction` walks parent levels to disambiguate) |
| Level is on a snowflake `<Join>`, not flat `<Table>` | Return null — fallback (this proposal's SQL emitter assumes single-table) |
| No matching keys | The SQL subquery returns zero rows; `IN (empty result)` naturally produces an empty axis |
| Very large matching key set | Still one subquery predicate. No Java-side key materialization and no generated huge `IN (...)` list |
| Level has no nameColumn | Use key expression as the caption/name fallback; SQL failure still falls back |
| Dialect doesn't have a position fn | Pattern doesn't match — return null |
| `<>` operator | Supported for contains/equality/begins/ends forms. Simple negative forms are optimized by querying the positive complement and wrapping the subquery predicate in `NotPredicate`, avoiding huge negative predicates. |
| Multiple caption conditions (`AND` / `OR`) | Supported when every leaf constrains the same hierarchy and same caption/name accessor kind. |
| Filter source uses a measure (e.g., `Filter([X].Members, [Measures].[Sales] > 100)`) | Doesn't match the caption predicate pattern — fallback (and that case actually NEEDS the live-evaluator path) |
| Negation: `-Filter(...)` | The Filter arg is unwrapped first by `buildSubcubeAxisPredicate`'s `-` handler, which recurses. Our handler matches the inner Filter; the outer `-` wraps the result in `NotPredicate`. Free correctness. |

### Tests

**Parser-level (no DB)** — implemented in `mondrian/src/test/java/mondrian/olap/SubcubePredicateParsingTest.java`:

1. **`testFilterInStrPropertiesParse`** — asserts the prod MDX shape `Filter([X].AllMembers, InStr(1, [X].CurrentMember.Properties("MEMBER_CAPTION"), "lit") > 0)` parses to the expected FunCall tree (top `Filter`, inner `>`, inner-inner `Properties` with `MEMBER_CAPTION` literal, etc.). Lock in the AST contract the handler relies on.

2. **`testFilterInStrMemberCaptionParse`** — same shape but using the bare `.member_caption` accessor. Asserts `getFunName()` = `"Member_Caption"` (canonical-cased), confirming the two AST shapes are distinct and the handler must match both names explicitly.

**Behavior + SQL-emission (DB required)** — implemented in `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`:

3. **`testInStrSubselectIsBatchedSql`** — same MDX as the existing `testSubselectFilterInStrRestrictsOuterAxisWithNqe`, but install a SQL hook via `mondrian.rolap.RolapUtil.setHook` (precedent: `mondrian/src/it/java/mondrian/rolap/DataSourceChangeListenerTest`) to capture every JDBC statement that fires during execution. After execute: assert (a) exactly one captured statement matches the dialect's position-function pattern (`INSTR(sku_name, 'Carrots')` on H2 / `positionUTF8` on ClickHouse), (b) zero statements match the per-member bare-equality pattern `master_sku_key = \d+`. This is the test that distinguishes the optimized path from the old slow path — cell counts alone don't, since both produce 27/28 cells.

4. **`testCaptionSubselectLabelVariantsAreBatchedSql`** — verifies contains `<> 0`, does-not-contain, equals, does-not-equal, begins-with, does-not-begin-with, ends-with, and does-not-end-with. Each subselect is compared against the direct-axis baseline and the SQL hook asserts the static caption handler emitted the expected dimension-table SQL.

5. **`testCaptionSubselectRangeIsBatchedSql`** — verifies range/between-style caption comparisons joined by `AND` are translated to one dimension-table SQL and produce the same outer-axis set as direct-axis evaluation.

6. **`testInStrSubselectFallsBackOnUnsupportedShape`** — `Filter([X].Members, InStr(...member_caption...) > 0 AND [Measures].[Unit Sales] > 100)` (compound measure condition). The caption handler must return null; the existing fallback must produce a correct (smaller) result. SQL hook asserts no `INSTR`-style query was emitted by the handler.

7. **`testInStrSubselectEmptySubstringReturnsEmptyAxis`** — `InStr(...) > 0` with a substring that exists in zero members. Asserts the SQL subquery predicate produces 0 outer-axis members and that the static handler ran, not the fallback.

8. **`testInStrSubselectOnNonUniqueLevelFallsBack`** — pick a FoodMart level that has `uniqueMembers="false"` (or omit the attribute), apply the InStr filter, assert the static handler returned null and the fallback produced the correct restricted set. Guards against accidentally enabling the fast path on a non-unique level and silently dropping the parent-level constraints.

Live verification continues against the prod CH stack via `scripts/run_issue77_regression.sh` — expected q02 wall time drops from ~105 s to single-digit seconds.

### What this proposal does NOT change

- `evalFallbackDisjunction`, `expandSubcubePredicateDisjunction` other handlers, `RolapEvaluator.getSubcubePredicate` — unchanged.
- The `LiteralStarPredicate.FALSE` empty-set handling — preserved.
- The live-evaluator threading for `NonEmpty` / `TopCount` — preserved (those still need it).
- Existing tests — all 13 in `SubcubeFilterPushdownIT` should keep passing. The q01-q04 acceptance pack against prod CH should pass with q02 in single-digit seconds.

## Caching

The implementation does not add a separate Java key-list cache. The static caption handler validates the AST, builds a deterministic SQL subquery, and attaches it to the star predicate tree. Normal Mondrian SQL/segment caching remains responsible for repeated query execution.

The earlier idea of caching general `getSubcubePredicates(...)` output remains rejected. Dynamic fallback predicates can depend on evaluator state:

- `Filter([X].Members, [Measures].[Sales] > 100)` depends on slicer / axis evaluator state.
- `NonEmpty(set, measure)` depends on the CellReader and live evaluator state.
- `TopCount(set, n, measure)` depends on evaluator state.

Those paths must not reuse a context-independent caption signature unless they get a separate dependency-signature design.

## Phased follow-ups

If this proposal lands and we want to widen later:

1. **Generalize `Or(And(col=k))…` folding** in `buildSubcubeAxisPredicate` — collapses any disjunction of single-column-equality predicates into a `ListColumnPredicate`. Helps cases where the subcube produces many single-key predicates by other means (explicit member set, named set, etc.).
2. **Wildcard and transform functions** — `Like`, `Matches`, `LCase`, `UCase`, `Mid`. These need careful dialect mapping and escaping.
3. **Measure-mixed compound conditions** — `caption predicate AND [Measures].X > 0` still needs live evaluator state and remains fallback-only.
4. **Non-unique levels** — widen the SELECT to include parent-level key columns and emit a compound predicate so the fast path covers non-unique levels.
5. **Snowflake `<Join>` levels** — emit FROM + JOIN clauses; reuse `SqlMemberSource`'s join builder.
6. **Context-independent static-handler cache design.** If repeated metadata SQL remains visible in profiles, consider a narrow cache for static handlers (Members / Children / Descendants / caption predicates). Do not cache dynamic `evalFallbackDisjunction` results without a separate dependency-signature design.

Each follow-up is independent and can ship without revisiting this proposal's contract.

## Risks

- **SQL injection / backslash semantics.** The substring literal must be dialect-quoted. Use `SqlQuery.getDialect().quoteStringLiteral(...)` (or the `Dialect`-exposed equivalent), never raw concat and never `Util.singleQuoteString` (which only handles `'` → `''` and ignores backslash, which MySQL non-strict mode treats as an escape character). Add a unit test with `'` and `\` in the substring to confirm proper quoting.
- **Dialect coverage.** ClickHouse / MySQL / H2 / Postgres / Generic covered (functions `positionUTF8` / `LOCATE` / `INSTR` / `POSITION` respectively). Rare dialects fall back. Acceptable.
- **Multi-table levels (snowflake).** If the level's dimension is defined via `<Join>` instead of `<Table>`, the SQL needs FROM + JOIN, not a single `FROM <dim-table>`. **Scope decision: snowflake levels are out of scope for this proposal.** The handler returns null (falls back) when the level's `<Hierarchy>` content is anything other than a `<Table>` element. The faulting prod schema (`dim_fitnessshock_product`) is a flat table, so q02 is unaffected. Snowflake support is a phased follow-up.
- **Caption vs. uniqueName.** Mondrian's `MEMBER_CAPTION` is the level's `nameColumn`. The `MEMBER_UNIQUE_NAME` is a different property that includes parent levels — if Excel ever emits a filter on UNIQUE_NAME instead, the pattern won't match and we'll fall back (correct behavior). No surprise.
- **Sort stability.** The SQL doesn't specify ORDER BY; the resulting key set is unordered. The downstream subcube predicate is a SET — ordering doesn't affect semantics. Acceptable.
- **Re-entry / caching.** The handler does **not** need to use the `inEvalFallback` guard because it never compiles or evaluates an MDX expression and never asks the evaluator for cells; it only builds a SQL subquery string. Re-entry through native evaluators (the original `inEvalFallback` trigger) cannot reach this path.
- **SQL-subquery rendering.** Any SQL consumer that renders `StarPredicate` must understand `SqlInSubqueryPredicate`. The legacy path uses `StarPredicate.toSql`; NQE explicitly handles it in `NativeQuerySqlGenerator`.

## Open questions

1. Should we add a feature flag (`mondrian.rolap.subcube.captionFilterPushdown.enable`, default `true`)? My read: no — it's strictly additive, fails closed to the existing fallback, and adding flags multiplies test matrix. But if any reviewer is worried about a rollout-pause path, the flag is cheap.
2. The `start` argument of InStr is currently restricted to literal `1`. Excel always emits `1`. We mirror Excel's exact emission; any other constant falls through to the fallback. Lock this in via the parser-level test.
3. Should `SqlInSubqueryPredicate` get a narrower equality/canonicalization key that fingerprints normalized SQL rather than using the rendered SQL text verbatim? Current text-based canonicalization is deterministic for this handler and acceptable for the initial implementation.

## Verification matrix (before merge)

- `mondrian/scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT` — must be 13/13, including the generalized label-filter cases.
- `MVN_ARGS='-Dtest=SubcubePredicateParsingTest -DfailIfNoTests=false' ./scripts/mondrian-mvn.sh` — must be 17/17 (existing 15 + 2 parser-level AST tests for the InStr/`.member_caption` vs `.Properties("MEMBER_CAPTION")` shapes)
- `./scripts/test.sh` — no new failures vs main
- Live against prod CH via `scripts/run_issue77_regression.sh`:
  - q01 direct-axis Filter — ≤ 2 s
  - q02 subselect Filter — ≤ 5 s (target; was 105 s on issue77c-bccc7c4)
  - q03 TopCount subselect — ≤ 3 s (unchanged path)
  - q04 NonEmpty(Filter) subselect — ≤ 3 s (unchanged path)
- Container SQL log for q02 contains a `positionUTF8` (or dialect equivalent) subquery predicate inside the constrained fact/aggregate SQL; no per-member `master_sku_key = X` SqlTupleReader probes and no huge literal key list.

## Tag for the resulting image

`issue77d-<short-sha>` after the change lands on dronsv/emondrian-clickhouse main + a fresh outer submodule bump.
