# NativeSqlCalc denominator macros — discovery & #52 feasibility gate

**Status:** discovery only — no production changes
**Date:** 2026-05-18
**Decision asked:** Should #52 (WD single-scan) be implemented as plan A
(schema-only fix using existing primitives), B (one new macro + #52 as
first consumer), or C (full retained-denominator framework + #73 +
#74 + #75)?
**Recommendation, with caveats:** **A**, paired with a narrow
**#74a-only** (validation for existing macros). Defer #73 / #74b / #75
until a second concrete consumer or an actual silent-wrong incident
appears.

## 1. Macro inventory

Full inventory produced by code scan of
`mondrian/src/main/java/mondrian/rolap/NativeSqlCalc.java` (2586
lines) and `NativeSqlConfig.java` (341 lines). All macros recognised
today, with file:line, failure mode on bad input.

| Macro | Parsed | Rendered | Validated at schema-load? | Failure mode on bad input |
|---|---|---|---|---|
| `${factTable}`, `${factAlias}` | NativeSqlCalc:515-516 | substitutePlaceholders:1924 | no | MondrianException at render — unresolved placeholder |
| `${joinClauses}` | NativeSqlCalc:730,739 | substitutePlaceholders:1924 | no | same |
| `${whereClause}` | NativeSqlCalc:743 | same | no | same |
| `${whereClauseExcept:H1,H2,…}` | NativeSqlCalc:1903-1913 | same | **no** | **silent fallback `"1=1"` if predicate list empty; silent KEEP if hierarchy name unresolved (NOT excluded)** |
| `${axisExpr1..N}`, `${axisCount}` | NativeSqlCalc:701-705,687 | same | no | MondrianException if N > axisCount |
| `${axisPresenceSelectList}`, `${axisResultSelectList}`, `${axisSelectList}`, `${axisGroupByList}` | NativeSqlCalc:708-711 | NativeSqlCalc:2205-2249 | Contract A: template-text presence of `relationAlias` if `axisResultSelectList`/`axisGroupByList` used (NativeSqlConfig:126-136) | MondrianException on unresolved; otherwise empty if no axes |
| `${axisGroupByListCube}`, `${axisCubeSelectFlags}` | NativeSqlCalc:717-719 | NativeSqlCalc:2270-2312 | **yes — Contract B**: both required together; `rollupAxes=true` enforces (NativeSqlConfig:250-289) | MondrianException at schema-load on pairing violation |
| `${denominatorSelect:H1,H2,…}` | NativeSqlCalc:1914-1916 | dispatchDenominatorMacro:1966-1970 (renders 2483-2492) | **no** | **silent**: unresolved hierarchy silently kept; all-excepted → empty string |
| `${denominatorGroupBy:H1,H2,…}` and `${denominatorGroupBy:srcAlias:H1,H2,…}` | NativeSqlCalc:1917-1919 | dispatchDenominatorMacro:1972-1984 (renders 2512-2530) | **no** | **silent**: same as above |
| `${denominatorJoin:lhs:rhs:H1,H2,…}` | NativeSqlCalc:1920-1922 | dispatchDenominatorMacro:1986-1997 (renders 2541-2559) | partial — MondrianException if fewer than 3 colon parts (NativeSqlCalc:1988-1990) | **silent on unresolved hierarchy names** |
| `${<userVariable>}` from `nativeSql.variables` annotation | NativeSqlConfig:100-101, NativeSqlCalc:746-750 | substitutePlaceholders:1924 | no | MondrianException at render if variable name absent from map |

### Three observed failure-mode classes

1. **Fail-fast (MondrianException at template render)** — unknown
   simple placeholders, `axisExprN` beyond axis count,
   `denominatorJoin` argument-count mismatch, missing
   `nativeSql.variables` key.
2. **Fail-at-schema-load** — Contract A (`axisGroupByListCube` ↔
   `axisCubeSelectFlags` paired with `rollupAxes=true`).
3. **Silent fallback (no error, semantically wrong output)** —
   unresolved hierarchy names in
   `whereClauseExcept`/`denominatorSelect`/`denominatorGroupBy`/`denominatorJoin`
   are silently kept; all-excepted denominator produces empty SQL
   fragments.

The silent-fallback class is the relevant risk surface for any future
schema author working on these templates.

## 2. Lifecycle map

```
XML annotation (nativeSql.template / nativeSql.variables)
  ↓ schema-load: NativeSqlConfig.fromAnnotations (line 78)
  ↓ Contract A validation (NativeSqlConfig:250-289)
  ↓ NativeSqlDef stored in registry
  ↓
  ↓ query execution → RolapEvaluator → measure render
  ↓ NativeSqlCalc.evaluate (line 169) → evaluateViaRegistry (line 210)
  ↓
  ↓ NativeSqlCalc.buildPlaceholders (line 507)
  │   ← measure name, template text + index, baseCube/schema metadata,
  │     resolved RolapCubeHierarchy, query-time axis bindings ALL
  │     overlap HERE — this is the only point in the lifecycle where
  │     fail-fast validation for dynamic macros could plug in without
  │     re-resolving anything.
  ↓
  ↓ substitutePlaceholders (line 1865) → final SQL
```

Implication for #74: there is no single schema-load point that has
resolved hierarchy objects available, because hierarchies are
attached to per-query evaluator context. The earliest fail-fast hook
for dynamic-macro hierarchy resolution is `buildPlaceholders`. That
is "per-query early" rather than "schema-load early". Cheap enough
(once per query) but not literally at schema-load time.

A subset of validation IS possible at schema-load: parse-time checks
of macro syntax, recognised macro names, argument counts, presence
of required co-macros. Anything that depends on a hierarchy name
resolving has to wait until per-query.

## 3. Current WD template decomposition

`schema_demo.xml:777-861`, `[Взвеш. дистрибуция %]`. Two templates
(opt-in to `agg_brand_store` fast path, fallback to `agg_store`).

Template 0 (fast path):

```sql
WITH presence AS (
  SELECT f.${storeKey} AS store_key, f.${timeKey} AS time_key,
         any(f.store_period_total) AS spt
         ${axisPresenceSelectList}
  FROM agg_brand_store f
  WHERE ${whereClause}                              -- full WHERE incl. product
  GROUP BY store_key, time_key${axisPresenceSelectList}
),
d AS (
  SELECT ${denominatorGroupBy:src:product8}        sum(spt) AS total
  FROM (
    SELECT f.store_key, f.period_month,
           ${denominatorSelect:product8}           any(f.store_period_total) AS spt
    FROM agg_brand_store f
    WHERE ${whereClauseExcept:product8}            -- WHERE minus product
    GROUP BY f.store_key, f.period_month,
             ${denominatorGroupBy:product8} tuple()
  ) src
  GROUP BY ${denominatorGroupBy:src:product8} tuple()
)
SELECT ${axisResultSelectList}
  CASE WHEN d.total=0 THEN NULL
       ELSE toFloat64(sum(pr.spt))/toFloat64(d.total)*${multiplier} END AS val
FROM presence pr
${denominatorJoin:pr:d:product8}
GROUP BY ${axisGroupByList}d.total
```

`product8` is the literal list
`Продукт.Бренд,Продукт.Производитель,Продукт.СКЮ,Продукт.Подкатегория,Продукт.Категория,Продукт.Вес,Продукт.Код товара в сети,Продукт.Название в сети`
repeated verbatim **four times** (once in `whereClauseExcept`, once
in `denominatorSelect`, once each in `denominatorGroupBy`, once in
`denominatorJoin`).

Decomposition:

| Aspect | Value |
|---|---|
| Numerator grain | `(store_key, period_month, axes)` filtered by full WHERE (including product) |
| Denominator grain | `(store_key, period_month, axes minus product8)` filtered by WHERE minus product8 |
| Retained dimensions | visible axis dims minus `product8` |
| Reset dimensions | `product8` (8 product hierarchies, manually listed) |
| Visible XMLA grid dimensions | `${axisResultSelectList}` = full axis set |
| Fact / agg source | `agg_brand_store` (fast); `agg_store` (fallback, template.1) |
| Join strategy | `pr` × `d` joined on retained axis keys via `${denominatorJoin}` |
| Scans of `agg_brand_store` today | 2 (presence + denominator inner SELECT) — was 3 before `fact-first-96cd1cc` |

`${denominatorJoin:pr:d:product8}` is the macro that does the
"retained-axis JOIN": for each non-reset hierarchy it emits
`AND pr.kN = d.kN`. So the retained-axis JOIN semantics IS already
supported, parameterised by the reset list.

## 4. Other current consumers (not WD)

Grep over both production schemas:

| Schema | Calculated members using denominator macros |
|---|---|
| `schema_demo.xml` | `Анти-АКБ`, `Взвеш. дистрибуция %`, `Нум. дистрибуция %`, `ОКБ`, `ОКБ native`, `Оффтейк руб`, `Оффтейк шт` — 7 measures |
| `schema_confectionery.xml` | `Взвеш. дистрибуция %`, `Оффтейк руб`, `Оффтейк шт`, `СКЮ/ТТ` — 4 measures |

So denominator-style retained-axis SQL is not a WD-only pattern — it
is the dominant pattern for ratio/share/concentration measures in
this codebase. **The same reset list — `product8` — is repeated
across all of them (verified by grep). The duplication risk is
already realised.**

## 5. Target single-scan SQL sketch (#52)

Target shape, using only existing macros — no new grammar:

```sql
WITH presence_keys AS (
  SELECT DISTINCT f.${storeKey} AS store_key, f.${timeKey} AS time_key
  FROM agg_brand_store f
  WHERE ${whereClause}                  -- full WHERE incl. product
)
SELECT ${axisResultSelectList}
  CASE WHEN sum(s.store_period_total)=0 THEN NULL
       ELSE toFloat64(sum(
              multiIf(
                (s.store_key, s.period_month) IN
                  (SELECT store_key, time_key FROM presence_keys),
                s.store_period_total, 0)))
            / toFloat64(sum(s.store_period_total)) * ${multiplier}
       END AS val
FROM (
  SELECT ${denominatorSelect:product8}
         f.store_key AS store_key, f.period_month AS period_month,
         any(f.store_period_total) AS store_period_total
  FROM agg_brand_store f
  WHERE ${whereClauseExcept:product8}
  GROUP BY ${denominatorGroupBy:product8} f.store_key, f.period_month
) s
GROUP BY ${axisGroupByList}
```

Properties:

- One scan of `agg_brand_store` for `presence_keys` (filtered).
- One scan for the outer subquery; with the existing CH projection
  `store_totals` on `(store_key, period_month) → sum(*)`, ClickHouse
  routes this to the projection (~47 K rows instead of 3.83 M).
- `multiIf((s.store_key, s.period_month) IN (subq), w, 0)` is the
  conditional-aggregation pattern. The subquery is evaluated once
  by CH (it is non-correlated).
- All macros used here are already implemented:
  `${whereClause}`, `${whereClauseExcept:...}`,
  `${denominatorSelect:...}`, `${denominatorGroupBy:...}`,
  `${axisResultSelectList}`, `${axisGroupByList}`,
  `${storeKey}`, `${timeKey}`, `${multiplier}`.
- The `${denominatorJoin:pr:d:product8}` macro is no longer needed
  (no presence × denom JOIN) — replaced by `IN (subquery)`.

This is a pure schema-XML edit. No Mondrian code change.

## 6. Gap matrix — existing primitives vs retained-axis requirements

| Question | Evidence | Answer |
|---|---|---|
| Can #52 single-scan be expressed only with existing macros? | Section 5 sketch uses no new grammar | **yes** |
| If yes, how brittle? | Reset list `product8` must be passed to 3 different macros (`whereClauseExcept`, `denominatorSelect`, `denominatorGroupBy`). Currently the same list is already repeated 4× in the existing template, so the new template is no more brittle than the existing one (1 fewer copy, actually). | **same as today** |
| Must reset dimensions be enumerated manually? | Yes today; the new sketch keeps the same convention. | **yes** |
| Can a typo silently produce wrong results? | Yes — unresolved hierarchy in `whereClauseExcept` / `denominatorSelect/GroupBy/Join` is silently KEPT (Section 1, failure-mode class 3). Today the same risk exists for WD and for the other 10 consumers in production schemas. | **yes — but the risk already exists today across 11 consumers; #52 does not add new risk** |
| Will current validation see the typo? | No. Inventory shows no typo / unresolved-hierarchy validation for dynamic macros. | **no** |
| Is a new macro needed for **correctness**? | The single-scan template can be expressed correctly today. The new macro would only reduce the per-measure copy-paste of the reset list. | **no** |
| Is a new macro needed for **ergonomics**? | Marginal — the existing pattern already repeats the reset list 4× per measure. A named-reset macro would consolidate to 1×, but the existing 11 consumers already accept the 4× cost. | **marginally — but ergonomics win is small relative to refactor cost** |

## 7. Recommendation

**Plan A — ship #52 alone, plus a narrow #74a — is the right next
move.**

Reasoning:

- #52 is purely a schema XML edit using existing macros. Effort: 1–2
  hours including A/B verification. Risk: low — toggle escape exists
  (`nativeSql.enabled=false`); identical-hash A/B is straightforward.
  Win: 2 scans → 1 scan on the dominant fact table; expected pack
  contribution drop from 60–80 s to 10–20 s on prod.
- The 11 existing consumers ALREADY accept the manual-reset-list
  duplication pattern. Adding a generalised retained-denominator
  macro now would not retroactively fix any of them unless each one
  is also refactored — that is a separate, larger project with its
  own risk surface.
- The biggest realised risk in the existing system is the
  silent-fallback class (Section 1, class 3): a typo in any of the
  11 consumers' reset lists today silently produces wrong
  denominator values. **That risk is already shipping in
  production today and is independent of #52 or #73.** It can be
  addressed by a narrow validator that resolves the named
  hierarchies in `buildPlaceholders` and throws if any reference is
  unresolved. This is the "#74a-only" sub-scope — independent of
  #73, useful immediately to all 11 consumers, fits in ~50 lines of
  Java + tests.

The "full C" plan as originally drafted bundles three different
concerns into one stream:

| Concern | Independent value |
|---|---|
| #52 perf fix | high; concrete; today |
| #73 new macro language | low until a second new ratio measure is added |
| #74a typo validation for existing macros | high; cross-cutting; independent |
| #74b validation for hypothetical new macros | zero until #73 lands |
| #75 acceptance matrix | useful only after consumers exist that need it |

`A + #74a` captures the two high-value bands; `#73 / #74b / #75`
remain available if discovery in a future iteration reveals a real
ergonomic blocker rather than a speculative one.

## 8. Minimal next patch plan

1. **`#52` schema rewrite** (1 file, ~50 lines of XML changed):
   - Rewrite `[Взвеш. дистрибуция %]` template in `schema_demo.xml`
     (and the sibling CUBE variant) to the Section 5 shape.
   - Same change for `schema_confectionery.xml` after the demo-stack
     A/B confirms correctness.
   - A/B against debug-dump with `aggregates.Use=false` to force
     fact scan, plus aggregates-on case to confirm no regression on
     the routed path.
   - Compare ClickHouse `system.query_log.read_rows` and
     `query_duration_ms`.
   - Acceptance: identical result-cell hash, lower `read_rows` on
     fact-scan path (or equal — CH projection routing may already
     do this).

2. **`#74a` typo validator** (1 source file, ~50 LoC; 1 test file,
   ~4 cases):
   - In `NativeSqlCalc.buildPlaceholders`, resolve every hierarchy
     name appearing in `whereClauseExcept` / `denominatorSelect` /
     `denominatorGroupBy` / `denominatorJoin` against
     `baseCube.lookupHierarchy(name, true)`.
   - On unresolved name: throw `MondrianException("native SQL
     calc [{measure}] template[{idx}] macro {macroName} references
     unknown hierarchy {name}")`.
   - Unit tests: typo, ambiguous, unknown dimension prefix.
   - Acceptance: 11 existing consumers continue to render with no
     change; an injected typo on a test schema fails fast.

3. **Re-evaluate `#73 / #74b / #75`** after #52 lands. If a new
   ratio measure is then drafted and its author hits a real
   ergonomic limit, the case for #73 will be concrete. Otherwise
   keep it as backlog.

## What this discovery did NOT do

- No new macro grammar designed.
- No Mondrian source modified.
- No schema modified.
- No commitment to plan C as an obligation.
