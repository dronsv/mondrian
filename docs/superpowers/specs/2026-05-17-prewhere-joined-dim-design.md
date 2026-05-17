# PREWHERE pushdown for joined-dimension predicates (scope B)

**Status:** approved
**Date:** 2026-05-17
**Issue:** dronsv/emondrian-clickhouse#18 (expand scope)
**Builds on:** `3af9ab99f` (narrow PREWHERE), `a2f71e9ac` / `b14e8589c`
(telemetry), `b3b07a33d` (default-on)

## Motivation

The narrow PREWHERE implementation only emits `PREWHERE` for
predicates whose column lives on the fact table. In every production
schema observed locally (`schema_demo` Konfet, `FitnessShock`) all
user-facing dimensions go through `foreignKey=` joins to dim tables.
Result: `applied=true` is never observed, the original acceptance
criterion ("lower `read_rows` on representative slicer-heavy queries")
is not satisfied by the narrow scope, even with default-on.

ClickHouse `PREWHERE` in a JOIN query can only reference columns of
the LEFT (main) table. Direct PREWHERE on a joined dim column is not
supported by the engine. The standard ClickHouse pattern for
slicer-style pushdown is the FK-subquery rewrite:

```sql
-- before
SELECT ... FROM fact f
  JOIN dim d ON f.fk = d.pk
WHERE d.col = 'X'

-- after
SELECT ... FROM fact f
  JOIN dim d ON f.fk = d.pk
PREWHERE f.fk IN (SELECT pk FROM dim WHERE col = 'X')
WHERE d.col = 'X'
```

The duplicate `WHERE` is intentional: it preserves semantic equivalence
under any ClickHouse JOIN reorder choice, and the optimizer deduplicates
the work in practice.

## Goal — scope B

Cover the common Excel slicer shape: single-column equality (`=`) or
`IN (...)` on a unique-leaf level of a dimension that uses a plain
`foreignKey=` join to a non-snowflake dim table.

Out of scope for this iteration (separate follow-ups):

- multi-column tuple `IN` (composite parent-tuples)
- snowflake / `<View>` / `<Join>` dim relations
- "drop JOIN entirely" (option C)
- exclude paths and tuple slicers

## Design

### Eligibility (the new helper rejects unless all hold)

1. ClickHouse dialect.
2. `aggStar == null` — agg queries are skipped, same as the existing
   `addSimplePredicate`/`addConditionPredicate` path.
3. `baseCube != null`.
4. Predicate is single-column (no parent-tuple composite). The
   composite tuple case is naturally produced by
   `generateMultiValueInExpr` when the slicer level is not unique;
   this iteration only handles the single-column case.
5. Level is unique (`level.isUnique()` — the schema declares
   `uniqueMembers="true"`).
6. Dimension uses a direct `foreignKey=` join to a single `<Table>`
   dim relation. Snowflake / `<View>` / `<Join>` relations decline.
7. The dim PK column is resolvable from the level's hierarchy
   `primaryKey` and the dim relation.
8. The pre-built fact-side predicate fragment (the SQL the caller
   already produced for the dim-side `WHERE`) is non-empty.

Each rejection records a specific `REASON_*` so workload telemetry can
attribute fallbacks.

New constants in `ClickHousePrewhereSupport`:

- `REASON_NON_UNIQUE_LEVEL`
- `REASON_SNOWFLAKE_DIM`
- `REASON_NULL_FK`
- `REASON_NULL_PK`
- `REASON_COMPOSITE_TUPLE`

(Existing reasons `REASON_DISABLED`, `REASON_AGG_QUERY`,
`REASON_NULL_BASE_CUBE`, `REASON_NULL_COLUMN`,
`REASON_NON_CLICKHOUSE_DIALECT` are reused as-is.)

### New helper

```java
public static boolean addJoinedDimPredicate(
    SqlQuery sqlQuery,
    RolapCube baseCube,
    AggStar aggStar,
    RolapLevel level,
    RolapStar.Column factForeignKeyColumn,
    String dimTableSql,        // e.g. "`dim_konfet_product`"
    String dimPrimaryKeySql,   // e.g. "`sku_unified_id`"
    String builtDimPredicate)  // the SQL fragment the caller already built
```

On success, calls `sqlQuery.addPreWhere(...)` with

```
<fact_fk_sql> IN (SELECT <dim_pk_sql> FROM <dim_table_sql> WHERE <builtDimPredicate>)
```

and returns `true`. On rejection, records a `REASON_*` via the existing
`noteFallback` channel and returns `false`. Callers leave their normal
`WHERE` / `JOIN` emission untouched — this helper only adds an
additional PREWHERE clause; the regular path keeps producing semantically
identical SQL.

### Call sites

`SqlConstraintUtils.generateMultiValueInExpr` and the single-value
counterpart used by `addContextConstraint`. After the caller has
produced the dim-side condition string and known the originating
level + fact-side column, it calls `addJoinedDimPredicate` with that
context. Failure is silent — the caller's existing return path keeps
working unchanged.

This keeps the change additive: the existing SQL is never weakened,
and the new helper either adds a PREWHERE or does nothing.

### Telemetry

`SqlQuery.logClickHousePrewhereTelemetry` already logs
`applied`/`reason`/`clauses`. No format change. Successful
`addJoinedDimPredicate` increments the existing `preWhere.size()`
counter (via `addPreWhere`) and sets `applied=true` via the existing
machinery — so the unchanged telemetry line now reports the
joined-dim hits as part of the same metric.

## Risks and mitigations

- **Subquery cost on dim** — dim tables are small in every observed
  schema (10⁰ – 10⁴ rows); a row scan is cheap. If a future deployment
  has a large dim, the toggle escape hatch
  (`mondrian.clickhouse.prewhere.enabled=false`) still disables.
- **JOIN reorder making PREWHERE redundant** — possible. The duplicate
  WHERE preserves correctness; the cost of an extra small subquery is
  a few ms in the worst case. Live A/B will tell whether it's a wash
  or a win.
- **Schema features we don't yet handle (snowflake, View, join chain)**
  — the eligibility guard declines explicitly with a typed reason so
  the workload-telemetry follow-up can quantify how many slicer
  queries fall into each shape.

## Tests

### Unit (Mockito)

In `ClickHousePrewhereSupportTest`:

- `addJoinedDimPredicate_singleEqOnUniqueLeaf_emitsPrewhereSubquery`
- `addJoinedDimPredicate_multiValueInOnUniqueLeaf_emitsPrewhereSubquery`
- `addJoinedDimPredicate_nonUniqueLevel_declinesWithReason`
- `addJoinedDimPredicate_aggQuery_reusesAggReason`
- `addJoinedDimPredicate_disabledExplicitly_reusesDisabledReason`
- `addJoinedDimPredicate_nullForeignKey_declinesWithReason`
- `addJoinedDimPredicate_emptyPredicate_declinesWithReason`

### Live A/B (debug-dump stack, FitnessShock VM)

The MDX from #41 (single brand slicer) is the canonical Excel
slicer-pushdown shape. Run the same MDX against the current default-on
image and against an image with this change; assert:

1. The fix image's SQL log contains `PREWHERE <fact_fk> IN (SELECT …`.
2. Result-cell value hash unchanged vs the no-PREWHERE baseline.
3. ClickHouse `system.query_log.read_rows` for the fact-table SQL
   query is lower under the fix image (the win we are after).

If `read_rows` is unchanged, that is itself the answer — the CH
optimizer was already pushing the JOIN filter effectively and no
further engine-side work is needed for this shape. The telemetry
delta (applied=true count) is still a useful artifact.

## What ships

- New helper in `ClickHousePrewhereSupport`.
- Call-site changes in `SqlConstraintUtils.generateMultiValueInExpr`
  and the single-value path.
- New `REASON_*` constants.
- Seven new unit tests.
- A/B report in the issue-18 comment.
- No new public properties — relies on the existing
  `mondrian.clickhouse.prewhere.enabled` toggle.
