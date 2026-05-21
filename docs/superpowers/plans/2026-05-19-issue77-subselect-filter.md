# Issue #77 — Subselect Filter pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore SQL pushdown of dynamic subselect expressions like `Filter([X].Members, condition)` so the outer axis honors the subselect set. Fixes Excel-style label filter that currently returns the full product axis for `FROM (SELECT Filter(... InStr(... member_caption, "CHIKA") ...) ...)`.

**Architecture:** Core fix in `Query.evalFallbackDisjunction`:

1. **Validate axis Exp before compiling.** `Subcube.axes[i].getSet()` returns the unresolved AST (parsed only — `setSet()` is never called with a validated version, unlike outer axes at `Query.java:696-703`). When `compiler.compile(exp)` dispatches to `UnresolvedFunCall.accept(ExpCompiler)`, that method throws `UnsupportedOperationException` (`UnresolvedFunCall.java:115-117`). The broad `catch (Exception e)` at the end of `evalFallbackDisjunction` silently consumes it and returns `noConstraintDisjunction()`.
2. **Force a `ListCalc` via `compileList`.** Even after validation, `compiler.compile(resolvedExp)` would still be silently lost: `Query.resultStyle` defaults to `ITERABLE` on Java 25 (`Query.java:147-148`, `Util.Retrowoven=false`), and `FilterFunDef.compileCall` returns an `IterCalc` (`FilterFunDef.java:95-98`, `compileCallIterable`) when iterable is acceptable. The existing `if (!(calc instanceof ListCalc listCalc))` guard then triggers another silent `noConstraintDisjunction()`. Switching to `compiler.compileList(resolvedExp)` (`AbstractExpCompiler.java:288`) forces a `ListCalc` and bypasses the iterable preference.
3. **Preserve exact empty and `NonEmpty` semantics.** Empty evaluated sets produce an explicit false predicate, not "no constraint"; callers pass a live evaluator where available so `NonEmpty(set, measure)` can use the real cell reader instead of approximating to `set`.

Both NQE (`NativeQuerySqlGenerator.buildWhereFromContext` line 1448) and legacy (`SqlConstraintUtils.addSubcubeConstraint` line 252) consumers go through the same `Query.getSubcubePredicates(...)` family, so a single Query-layer fix covers both. For dynamic forms that need cell evaluation (`NonEmpty(set, measure)`), callers pass the live evaluator so the fallback uses the real `CellReader` instead of approximating the set. The fix also un-breaks `TopCount`, `NonEmpty`, named-set, and any other dynamic subselect form — all introduced as intent in commit `0b70d8225` but never functional.

**Tech Stack:** Java 25, JUnit 3-style ITs against H2 FoodMart via `scripts/test-it-h2.sh` (`FoodMartTestCase` base + `assertQueryReturns`), JUnit 5 unit tests for AST-level checks. Plan touches only the mondrian module; downstream rollout (emondrian-clickhouse regression tests, olap_stores acceptance, image rebuild + deploy, Excel-filter log scan) is summarized at the end as out-of-scope follow-ups.

---

## Investigation Summary

**Bug confirmed not a regression.** Pre-fix code (`0b70d8225^`) returned `noConstraintDisjunction()` for any FunCall it did not statically recognize. Commit `0b70d8225` (Apr 7, our repo) added `evalFallbackDisjunction` intending to handle Filter/TopCount/NonEmpty/etc., but the new code calls `compiler.compile(exp)` on the unvalidated subcube AST → `UnsupportedOperationException` → silent catch. Observable behavior identical to the original limitation.

**Why the static patterns work but eval-fallback does not:** the static handlers (`expandMemberEnumerationFunCall` for `.Members`, `expandChildrenFunCall` for `.Children`, `expandDescendantsFunCall`) navigate via `SchemaReader` directly — they never compile or evaluate the Exp, so the unresolved-AST state never causes a failure. Only the dynamic fallback path needs the compile.

**Validation idiom already in the codebase:** outer axes in `Query.java:696-703` use the pattern
```java
Exp prevSet = axes[i].getSet();
axes[i].setSet(new UnresolvedFunCall(...).accept(compiler.getValidator()));
// ...compile against the validated form...
axes[i].setSet(prevSet);  // restore
```
That is the established way to "validate without committing" for an axis. `accept(Validator)` returns a fresh `ResolvedFunCall` (`UnresolvedFunCall.java:106-112`), so the local-variable form (without `setSet`/restore) suffices for predicate generation since we never re-read the same axis under the same call.

**Test location chosen:**
- IT against H2 FoodMart: `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`, extends `FoodMartTestCase` (uses `[Product].[Product Name]`, substring `"Carrots"` — a small known subset).
- No unit test: the originally proposed `UnresolvedFunCall.accept(ExpCompiler)` assertion would not prove the fallback behavior. The H2 IT is the meaningful end-to-end proof.

---

### File Structure

- **Modify:** `mondrian/src/main/java/mondrian/olap/Query.java`
  - method `evalFallbackDisjunction` (lines 2914-2962): validate `exp` before compile, add a `LOGGER.debug` line on the catch so future silent fallbacks are observable.
  - Add a `private static final org.apache.logging.log4j.Logger LOGGER = ...` field at the top of the class if one is not already in scope (Query currently uses `RolapUtil.PROFILE_LOGGER` only; we add a dedicated one for this path).
- **Create:** `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`
  - JUnit-3 style IT (matches the H2-FoodMart Failsafe path); extends `FoodMartTestCase`.
  - Covers: direct-axis control, subselect (NQE on), subselect (NQE off), subselect with `TopCount` (smoke), subselect with exact `NonEmpty`, and empty dynamic subselect semantics.

---

## Tasks

### Task 1: Failing IT — primary subselect bug

**Files:**
- Create: `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`

- [ ] **Step 1: Create the IT class with the failing subselect test**

```java
/*
// This software is subject to the terms of the Eclipse Public License v1.0
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;

/**
 * Regression coverage for emondrian-clickhouse#77 — Excel-style
 * {@code FROM (SELECT Filter(... InStr(... member_caption ...)) ...)}
 * subselect must restrict the outer axis.
 *
 * <p>The control variant (direct axis Filter, no subselect) is the
 * upstream-correct baseline. The subselect variants exercise the
 * dynamic-expression pushdown path in
 * {@link mondrian.olap.Query#getSubcubePredicates}.
 */
public class SubcubeFilterPushdownIT extends FoodMartTestCase {

    public SubcubeFilterPushdownIT() {
    }

    public SubcubeFilterPushdownIT(String name) {
        super(name);
    }

    private static final String SUBSTRING = "Carrots";

    private static final String DIRECT_AXIS_FILTER_MDX =
        "SELECT NON EMPTY Filter("
        + "  [Product].[Product Name].AllMembers, "
        + "  InStr(1, [Product].CurrentMember.Properties("
        + "    \"MEMBER_CAPTION\"), \"" + SUBSTRING + "\") > 0"
        + ") ON 0 FROM [Sales] WHERE [Measures].[Unit Sales]";

    private static final String SUBSELECT_FILTER_MDX =
        "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
        + "FROM (SELECT Filter("
        + "  [Product].[Product Name].AllMembers, "
        + "  InStr(1, [Product].CurrentMember.Properties("
        + "    \"MEMBER_CAPTION\"), \"" + SUBSTRING + "\") > 0"
        + ") ON 0 FROM [Sales]) WHERE [Measures].[Unit Sales]";

    private static final String FULL_AXIS_MDX =
        "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
        + "FROM [Sales] WHERE [Measures].[Unit Sales]";

    /**
     * Excel-style subselect with NQE enabled — outer axis must be
     * restricted to the Filter result, matching the direct-axis baseline.
     */
    public void testSubselectFilterInStrRestrictsOuterAxisWithNqe()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        int baseline = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int fullAxis = outerAxisCount(FULL_AXIS_MDX);
        assertTrue(
            "Substring '" + SUBSTRING + "' must select a proper subset "
                + "of the Product Name level — baseline=" + baseline
                + " fullAxis=" + fullAxis,
            baseline > 0 && baseline < fullAxis);

        int subselect = outerAxisCount(SUBSELECT_FILTER_MDX);
        assertEquals(
            "Subselect outer axis must equal direct-axis baseline "
                + "(both should be the InStr-restricted subset)",
            baseline, subselect);
    }

    private int outerAxisCount(String mdx) {
        return getTestContext().executeQuery(mdx)
            .getAxes()[0].getPositions().size();
    }
}
```

- [ ] **Step 2: Run the test and confirm it fails with the right wrong answer**

Run:
```
./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT
```

Expected: FAIL — actual count is the full Product Name axis (~1500 in FoodMart sample). If the actual count differs from "Carrots subset" but is *small*, the test substring is wrong — pick a substring with a clearer mismatch and update the expected count. Confirm the failure mode is "too many members on outer axis", not a SOAP fault or parse error.

- [ ] **Step 3: Commit the failing test**

```bash
git -C /home/andrey/work/emodrian_changes/mondrian add \
  mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java
git -C /home/andrey/work/emodrian_changes/mondrian commit -m "$(cat <<'EOF'
test(it): failing regression for #77 subselect Filter pushdown

Reproduces the Excel-style FROM (SELECT Filter(... InStr(... member_caption
...)) ...) shape against FoodMart [Product].[Product Name]. Without the fix
the outer axis returns all product members; expected restricted subset.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Apply the engine fix

**Files:**
- Modify: `mondrian/src/main/java/mondrian/olap/Query.java` (method `evalFallbackDisjunction`, lines 2914-2962)

- [ ] **Step 1: Add a logger field at the top of Query**

Locate the existing static fields (search for `PROFILE_LOGGER` usage near `Query.java:239`). At the top of `class Query`, just below the existing logger references (or near the top imports if no class-level logger exists), add:

```java
private static final org.apache.logging.log4j.Logger LOGGER =
    org.apache.logging.log4j.LogManager.getLogger(Query.class);
```

If a `LOGGER` field already exists for the class, skip this step.

- [ ] **Step 2: Modify evalFallbackDisjunction to validate before compiling**

Replace the body of `evalFallbackDisjunction` (currently `Query.java:2914-2962`) with:

```java
private List<List<StarPredicate>> evalFallbackDisjunction(
    RolapCube baseCube,
    Exp exp,
    Set<Hierarchy> ignoredHierarchies,
    Evaluator fallbackEvaluator)
{
    try {
        statement.setQuery(this);
        final Evaluator evaluator = fallbackEvaluator != null
            ? fallbackEvaluator.push()
            : RolapEvaluator.create(statement);
        final Validator validator = createValidator();
        final ExpCompiler compiler = createCompiler(
            evaluator,
            validator,
            Collections.singletonList(resultStyle));

        // Subcube axes are NOT pre-validated by the time
        // getSubcubePredicates runs (unlike outer axes — see
        // Query#compile at Query.java:696-703). The subcube AST is
        // still an UnresolvedFunCall tree, and
        // UnresolvedFunCall#accept(ExpCompiler) throws
        // UnsupportedOperationException, so compiler.compile(exp)
        // would silently fail in the catch below. Validate first to
        // produce a ResolvedFunCall the compiler can consume.
        final Exp resolvedExp = exp.accept(validator);

        // Use compileList — not compile — to force a ListCalc.
        // Query.resultStyle defaults to ITERABLE on Java 25
        // (Util.Retrowoven=false), and FilterFunDef returns an
        // IterCalc when ITERABLE is acceptable. The downstream
        // evaluateList call needs a ListCalc; compile() returning an
        // IterCalc would silently fall back here too.
        final ListCalc listCalc = compiler.compileList(resolvedExp);
        final TupleList tupleList = listCalc.evaluateList(evaluator);
        if (tupleList == null || tupleList.isEmpty()) {
            return emptySetDisjunction();
        }
        if (tupleList.size() > EVAL_MEMBER_LIMIT) {
            return noConstraintDisjunction();
        }

        // Flatten tuples into individual members
        final List<List<StarPredicate>> union =
            new ArrayList<List<StarPredicate>>();
        for (List<Member> tuple : tupleList) {
            final List<StarPredicate> conjunction =
                new ArrayList<StarPredicate>();
            for (Member m : tuple) {
                conjunction.addAll(
                    expandMemberPredicateConjunction(
                        baseCube, m, ignoredHierarchies));
            }
            if (!conjunction.isEmpty()) {
                union.add(conjunction);
            }
        }
        return union.isEmpty() ? noConstraintDisjunction() : union;
    } catch (Exception e) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "evalFallbackDisjunction: falling back to noConstraint "
                    + "for subcube axis "
                    + Util.unparse(exp) + " — " + e, e);
        }
        return noConstraintDisjunction();
    }
}
```

- [ ] **Step 3: Run the IT and confirm it now passes**

Run:
```
./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT
```

Expected: PASS. Outer axis count matches the Carrots subset (3 in FoodMart sample).

- [ ] **Step 4: Run the existing parsing test to confirm no regression in the static-pattern path**

Run:
```
./scripts/test.sh SubcubePredicateParsingTest
```

Expected: PASS — parsing was already covered, fix should not break it.

- [ ] **Step 5: Commit the fix**

```bash
git -C /home/andrey/work/emodrian_changes/mondrian add \
  mondrian/src/main/java/mondrian/olap/Query.java
git -C /home/andrey/work/emodrian_changes/mondrian commit -m "$(cat <<'EOF'
fix(subcube): validate + compileList in evalFallbackDisjunction (#77)

Two silent failure modes were stacked, each hiding behind the broad
catch in evalFallbackDisjunction:

1. Subcube.axes[i].getSet() returns the unresolved AST — outer axes
   get setSet'd with a validated form during Query#compile, but
   subcube axes do not. UnresolvedFunCall.accept(ExpCompiler) throws
   UnsupportedOperationException, so compiler.compile(exp) silently
   fell back to noConstraintDisjunction(). Fix: validate via
   exp.accept(validator) first.

2. Even after validation, compiler.compile(resolvedExp) returns an
   IterCalc for Filter under the default ITERABLE result style on
   Java 25 (FilterFunDef.compileCallIterable), and the existing
   instanceof ListCalc check then silently fell back again. Fix: use
   compiler.compileList(resolvedExp) to force a ListCalc.

Add LOGGER.debug on the catch so future silent fallbacks surface in
debug logs.

Fixes Excel-style FROM (SELECT Filter(... InStr(... member_caption ...))
...) which previously returned the full outer axis. Both NQE
(NativeQuerySqlGenerator.buildWhereFromContext) and legacy
(SqlConstraintUtils.addSubcubeConstraint) consumers benefit since both
go through Query.getSubcubePredicates. Also un-breaks TopCount,
NonEmpty, and named-set subselect forms introduced as intent in
0b70d8225 but never functional.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add NQE-off variant to lock in legacy-path behavior

**Files:**
- Modify: `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`

- [ ] **Step 1: Add the NQE-disabled test**

Append to `SubcubeFilterPushdownIT` (inside the class):

```java
    /**
     * Same subselect with NQE disabled — legacy SqlTupleReader /
     * Segment.load path must honor the same subcube predicate via
     * SqlConstraintUtils.addSubcubeConstraint.
     */
    public void testSubselectFilterInStrRestrictsOuterAxisWithoutNqe()
        throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, false);

        int baseline = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int subselect = outerAxisCount(SUBSELECT_FILTER_MDX);
        assertEquals(
            "Subselect outer axis must equal direct-axis baseline "
                + "under NQE-off legacy path",
            baseline, subselect);
    }
```

- [ ] **Step 2: Run it**

Run:
```
./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT
```

Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git -C /home/andrey/work/emodrian_changes/mondrian add \
  mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java
git -C /home/andrey/work/emodrian_changes/mondrian commit -m "$(cat <<'EOF'
test(it): cover NQE-off legacy subcube path for #77

Locks in that the fix applies to both NQE and legacy
SqlTupleReader/Segment.load via the shared
Query.getSubcubePredicates entry point.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add direct-axis control test

**Files:**
- Modify: `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`

This is a guard test for the case that already works — it ensures we never regress the direct-axis form while changing the subselect path.

- [ ] **Step 1: Add the control test**

Append to `SubcubeFilterPushdownIT`:

```java
    /**
     * Control: direct-axis Filter (no subselect). This path already
     * works in upstream and must continue to return the same matching
     * subset that the subselect form is now expected to return.
     */
    public void testDirectAxisFilterInStrIsBaseline() throws Exception {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        int matching = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int fullAxis = outerAxisCount(FULL_AXIS_MDX);
        assertTrue(
            "Direct axis Filter must restrict to a proper subset "
                + "(matching=" + matching + " fullAxis=" + fullAxis + ")",
            matching > 0 && matching < fullAxis);
    }
```

- [ ] **Step 2: Run and commit**

Run:
```
./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT
```

Expected: all three tests PASS.

```bash
git -C /home/andrey/work/emodrian_changes/mondrian add \
  mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java
git -C /home/andrey/work/emodrian_changes/mondrian commit -m "$(cat <<'EOF'
test(it): direct-axis Filter+InStr baseline for #77

Control test guarding the direct-axis form (already correct upstream)
against future regression while the subselect path is changed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Add TopCount and NonEmpty subselect smoke coverage

The same fix unblocks `TopCount`, `NonEmpty`, and named-set subselect forms — they all route through `evalFallbackDisjunction`. Lock in one smoke per form so future refactors do not re-break them quietly.

**Files:**
- Modify: `mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java`

- [ ] **Step 1: Add TopCount and NonEmpty subselect tests**

Append to `SubcubeFilterPushdownIT`:

```java
    /**
     * TopCount subselect — outer axis must be restricted to the top
     * N members chosen by the subselect.
     */
    public void testSubselectTopCountRestrictsOuterAxis() throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final int topN = 5;
        final String mdx =
            "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
            + "FROM (SELECT TopCount("
            + "  [Product].[Product Name].Members, " + topN + ", "
            + "  [Measures].[Unit Sales]"
            + ") ON 0 FROM [Sales]) "
            + "WHERE [Measures].[Unit Sales]";

        int actual = outerAxisCount(mdx);
        int fullAxis = outerAxisCount(FULL_AXIS_MDX);
        assertTrue(
            "TopCount subselect must restrict outer axis (actual="
                + actual + " topN=" + topN + " fullAxis=" + fullAxis + ")",
            actual > 0 && actual <= topN && actual < fullAxis);
    }

    /**
     * NonEmpty subselect — outer axis must be restricted to the
     * non-empty subset chosen by the subselect, not the full level.
     */
    public void testSubselectNonEmptyRestrictsOuterAxis() throws Exception
    {
        propSaver.set(
            MondrianProperties.instance().NativeQueryEngineEnable, true);

        final String mdx =
            "SELECT NON EMPTY [Product].[Product Name].Members ON 0 "
            + "FROM (SELECT NonEmpty("
            + "  Filter("
            + "    [Product].[Product Name].Members, "
            + "    InStr(1, [Product].CurrentMember.Properties("
            + "      \"MEMBER_CAPTION\"), \"" + SUBSTRING + "\") > 0"
            + "  ), [Measures].[Unit Sales]"
            + ") ON 0 FROM [Sales]) "
            + "WHERE [Measures].[Unit Sales]";

        int baseline = outerAxisCount(DIRECT_AXIS_FILTER_MDX);
        int actual = outerAxisCount(mdx);
        assertTrue(
            "NonEmpty(Filter(...)) subselect must restrict outer axis "
                + "to at most the Filter baseline (actual=" + actual
                + " baseline=" + baseline + ")",
            actual > 0 && actual <= baseline);
    }
```

- [ ] **Step 2: Run**

Run:
```
./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT
```

Expected: all five tests PASS.

- [ ] **Step 3: Commit**

```bash
git -C /home/andrey/work/emodrian_changes/mondrian add \
  mondrian/src/it/java/mondrian/rolap/SubcubeFilterPushdownIT.java
git -C /home/andrey/work/emodrian_changes/mondrian commit -m "$(cat <<'EOF'
test(it): cover TopCount and NonEmpty subselect pushdown for #77

Same evalFallbackDisjunction code path; locks in that the fix
restores SQL pushdown for the full dynamic-expression family
(Filter, TopCount, NonEmpty), not only the Excel InStr shape.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6 (dropped): Unit-level AST contract test

The originally-planned unit test would have asserted that `UnresolvedFunCall.accept(ExpCompiler)` throws — a property of an unrelated code path that would pass even if `evalFallbackDisjunction` stayed broken. The H2 IT in Task 1 is the meaningful end-to-end proof. No unit test in this plan.

(If we later want a parser-level contract test confirming that `Subcube.axes[i].getSet()` arrives as an `UnresolvedFunCall` for `Filter`, that belongs alongside `SubcubePredicateParsingTest` and not in this plan.)

---

### Task 7: Full test sweep + push branch

- [ ] **Step 1: Run the impacted IT and parsing test**

Run:
```
./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT
./scripts/test.sh SubcubePredicateParsingTest
```

Expected: all PASS.

- [ ] **Step 2: Run the broader unit-test suite to catch unrelated regressions**

Run:
```
./scripts/test.sh
```

Expected: PASS, or only failures that were also failing on `main` before this branch (compare with `git stash && ./scripts/test.sh` if uncertain). Investigate any new failures.

- [ ] **Step 3: Push and open PR**

```bash
git -C /home/andrey/work/emodrian_changes/mondrian push -u origin HEAD
gh -R dronsv/mondrian pr create \
  --title "fix(subcube): validate + compileList in evalFallbackDisjunction (#77)" \
  --body "$(cat <<'EOF'
## Summary

- Restore SQL pushdown of dynamic subselect expressions
  (`Filter`, `TopCount`, `NonEmpty`, named sets) introduced in
  0b70d8225 but never functional. Two stacked silent failures:
  (a) `evalFallbackDisjunction` called `compiler.compile(exp)` on the
  unresolved subcube AST, hitting
  `UnresolvedFunCall.accept(ExpCompiler)`'s
  `UnsupportedOperationException`; (b) even with validation,
  `compile()` returned an `IterCalc` for `Filter` under the default
  ITERABLE result style, failing the `instanceof ListCalc` check.
  Fix: validate first, then `compiler.compileList(...)`.
- Add `LOGGER.debug` on the catch so future silent fallbacks are
  observable.
- IT coverage in `SubcubeFilterPushdownIT` for the Excel-style
  `Filter(... InStr(... member_caption ...))` shape under NQE on and
  off, plus `TopCount`, exact `NonEmpty`, and empty dynamic subselect
  smokes.

Fixes dronsv/emondrian-clickhouse#77.

## Test plan

- [ ] `./scripts/test-it-h2.sh mondrian.rolap.SubcubeFilterPushdownIT` — all tests pass
- [ ] `./scripts/test.sh SubcubePredicateParsingTest` — still passes (no regression)
- [ ] `./scripts/test.sh` — no new unit-test failures vs main

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope (downstream rollout, separate plan)

The following are explicit per the user's task brief but are not part of this mondrian-engine plan. Each is a separate piece of work in a different repo:

1. **emondrian-clickhouse regression tests** — mirror the three core IT shapes (direct axis, subselect NQE-on, subselect NQE-off) using the ClickHouse FitnessShock fixture and the actual `"CHIKA"` substring. Reference fixture: `dim_fitnessshock_product` (27 matching rows by `sku_name`/`chain_product_name`/`brand_name`).
2. **olap_stores acceptance** — add a FitnessShock smoke case to the pre-deploy acceptance script with the exact Excel-style MDX from #77 so this regression class is caught before image rollout.
3. **Image rebuild + staged deploy** — rebuild the eMondrian image once the mondrian fix is merged, deploy to FitnessShock first, run the acceptance script, then roll out to remaining eMondrian instances.
4. **Log scan for similar Excel filter shapes** — grep prod logs for any `FROM (SELECT Filter(... InStr(... member_caption ...)) ...)` not only `"CHIKA"`; surface every affected cube so the rollout is comprehensive.

Each of those steps assumes Tasks 1-7 above are merged into mondrian first.

## Performance note (superseded — see follow-up spec)

Prod ClickHouse validation exposed that the live-evaluator path can call
`getSubcubePredicates` repeatedly during tuple iteration, which turns the
Excel `Filter(... InStr(...))` issue shape into many per-member
`SqlTupleReader` probes when the predicate is rebuilt on every call.

**An earlier Query-instance `subcubePredicateCache` prototype keyed only by
`(baseCube, ignoredHierarchies)` was reverted** during code review:

- The cache key omitted the `fallbackEvaluator` dependency, so dynamic forms
  like `NonEmpty(set, measure)` whose results depend on evaluator context
  could silently reuse a stale predicate within the same execution.
- A `Query` instance is also reused across executions via `setParameter`/
  `setSlicerAxis`/`clearEvalCache`, and the prototype had no invalidation
  hooks; this would have stacked a new stale-cache footgun on top of the
  pre-existing `evalCache` one.

The shipped fix takes a different route: a Level-1 static handler
(`tryInStrCaptionFilter`) recognizes the Excel `Filter(... InStr(... member_caption ...))`
AST shape and resolves matching keys via **one** SQL on the dim table, then
wraps them in a single `ListColumnPredicate` so downstream `SqlTupleReader`
emits a batched `IN (…)` query instead of per-member probes. The handler
itself is context-independent (never touches the evaluator), so a tiny
**Execution-scoped** cache keyed by `(baseCube, level, substring)` was added
in the same shipped commit — eliminating the repeated dim-table SQL within
one execution without re-introducing the unsafe Query-instance cache.

General caching of arbitrary `getSubcubePredicates` output is deferred
behind a dependency-signature design (phased follow-up #6). See:
`docs/superpowers/specs/2026-05-20-issue77-instr-sql-pushdown.md`.
