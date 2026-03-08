# Filter Predicate Planner v2 (Design)

## Problem

Interpreter-heavy MDX patterns such as

- `Filter(setExpr, NOT IsEmpty([Measures].M))`
- `Filter(setExpr, A AND NOT IsEmpty([Measures].M))`

can spend most runtime in JVM tuple/cell evaluation after SQL has already completed.

For large `CrossJoin` sets this creates tuple explosion, timeouts, and memory pressure.

## Goals

1. Push safe predicate parts as early as possible, before full tuple materialization.
2. Preserve MDX semantics by default (safety-first rewrite policy).
3. Keep unsupported/ambiguous predicates on fallback path.
4. Add predictable diagnostics and regression oracle checks.

## Non-goals

- No broad "rewrite everything" behavior.
- No implicit rewrites for calculated-measure-heavy semantics without proof.
- No silent mixed-measure (additive + distinct/HLL) unification into one physical plan.

## Safety policy (eligibility checker)

### Allow (v2)

- Exact atom: `NOT IsEmpty([Measures].M)`
- Top-level conjunction decomposition: `A AND B AND ...`
- Partial pushdown when at least one conjunct is safe.

### Block and fallback

- Calculated measures (unless explicitly proven safe).
- `OR`, general `NOT`, nested `IIF`, arbitrary function trees.
- Predicate parts depending on current member context in non-trivial way.
- `INCLUDE_CALC_MEMBERS` cases where set composition itself changes semantics.
- Mixed additive + distinct/HLL when no proven split-plan exists.

## Execution model

1. Normalize predicate AST.
2. Split top-level `AND` into conjunctive atoms.
3. Classify atoms:
   - `pushdownSafe`
   - `residual`
   - `unsupported`
4. Planning:
   - If unsupported exists: fallback, or partial pushdown only with mandatory residual evaluation.
   - If safe atoms exist: apply early candidate reduction from storage/native non-empty path.
5. Evaluate residual predicate over reduced candidate set.
6. Enforce guardrails on candidate size and evaluation budgets.

## v1 status (already implemented)

- Fast path for exact `Filter(setExpr, NOT IsEmpty([Measures].M))`.

## v1.1 status (current incremental extension)

- Safe partial pushdown for conjunctions containing `NOT IsEmpty([Measures].M)`.
- Native evaluator remains preferred where available.
- Residual predicate is still evaluated in standard engine path over pruned candidates.

## Mixed measures (distinct/HLL)

Treat as separate planning class.

Planner must choose one of:

1. single-source proven physical plan,
2. split-plan (additive branch + distinct/HLL branch + merge by tuple key),
3. fallback.

No implicit merge into additive fast path.

## Physical-layer guidance (ClickHouse)

- Prioritize projections/agg layouts for dominant group/filter patterns.
- Treat skip indexes as secondary tuning mechanism.
- Pushdown effectiveness depends on semantic planner correctness, not storage tricks alone.

## Observability

Add/keep metrics and logs:

- candidate tuples before/after pushdown,
- SQL phase time vs JVM cell-eval time,
- fallback reason,
- guardrail trigger reason.

Logs must be deduplicated per query shape.

## Regression strategy

For each rewrite rule:

1. Compare result set against baseline interpreter semantics.
2. Cover cases:
   - sparse and empty slices,
   - calculated members,
   - nested crossjoins,
   - `Head/Order/DrilldownLevel`,
   - distinct/HLL and mixed-measure queries.

Rewrites are accepted only with semantic equivalence and stable performance.

## Rollout

- Keep each rule behind feature flags where needed.
- Default conservative mode for production.
- Promote only after regression matrix passes.
