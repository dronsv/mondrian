# Contributing to Mondrian

## Testing

### Test framework

Unit tests live under `src/test/java/**` and use JUnit 5 Jupiter. Run with `mvn test`.

Integration tests live under `src/it/java/**` and use JUnit 3 + Vintage.
They are only compiled and run when the `load-foodmart` Maven profile
is active (`mvn verify -DrunITs=true`). The integration test entry
point is `mondrian.test.Main`, a reflective `TestSuite` dispatcher.

### Assertion style

**Use `org.junit.jupiter.api.Assertions.*` for new tests.** Static import
the specific methods you use (`import static org.junit.jupiter.api.Assertions.assertEquals;`).

Mondrian standardized on JUnit 5 Assertions in the TEST-B modernization
lane (2026-04, design spec section 6.4). The project does NOT use:

- AssertJ
- Hamcrest
- Google Truth

These were considered and rejected:

- Zero new dependencies for a functionally-equivalent capability.
- Existing declarative `assertEquals(expected, actual)` style maps cleanly
  to JUnit 5 built-ins.
- If a specific test genuinely benefits from AssertJ (e.g., deep collection
  assertions with fluent chaining), a file-local static import is acceptable —
  never a global migration.

### Argument order (JUnit 3 vs JUnit 5)

JUnit 5 assertion methods put the message LAST:

```java
// JUnit 5 (correct)
assertEquals(expected, actual, "message");
assertTrue(condition, "message");
assertNull(object, "message");

// JUnit 3 (wrong for JUnit 5 — will not compile)
assertEquals("message", expected, actual);
assertTrue("message", condition);
assertNull("message", object);
```
