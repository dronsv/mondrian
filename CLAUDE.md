# Mondrian Build & Development

## Repository Layout

```
mondrian/                  ← THIS is the git repo root
├── mondrian/              ← Maven module (source code lives here)
│   ├── pom.xml
│   └── src/
│       ├── main/java/mondrian/
│       └── test/java/mondrian/
├── scripts/
│   ├── build.sh           ← compile via Docker
│   ├── test.sh            ← run unit tests via Docker
│   └── test-it-h2.sh      ← run XMLA Discover ITs against embedded H2
└── CLAUDE.md
```

**IMPORTANT**: The git repo root is `mondrian/` (this directory).
Source files are at `mondrian/src/main/java/...` (relative to repo root).
Do NOT confuse with the parent `emodrian_jdk25/` directory.

## Build & Test

**No local Maven is installed.** All builds run via Docker.

### Compile
```bash
./scripts/build.sh
```

### Run tests
```bash
# All unit tests
./scripts/test.sh

# Single test class
./scripts/test.sh FactResolvedTableTest

# Multiple test classes
./scripts/test.sh "FactResolvedTableTest,AggResolvedTableTest"
```

### Run XMLA Discover ITs against embedded H2 (no MySQL required)

    ./scripts/test-it-h2.sh                                                       # default: XmlaDiscoverNativeSqlTelemetryTest both methods
    ./scripts/test-it-h2.sh mondrian.xmla.XmlaBasicTest                          # any IT class
    ./scripts/test-it-h2.sh "mondrian.xmla.XmlaBasicTest#testDSchemaRowsets"     # single JUnit-3 method

Uses Maven profile `it-h2-foodmart`: loads FoodMart into a file-based H2
instance under `mondrian/target/it-h2/` in `pre-integration-test`, then runs
the named IT class via Failsafe. Self-contained inside the Docker-wrapped
Maven — no Docker-in-Docker, no host MySQL. The default upstream
`embedded-mysql` / `load-foodmart` profiles are unchanged and remain the
path for full IT runs against real MySQL.

**Known residual:** `XmlaBasicTest.testDSchemaRowsets` fails with a golden-XML
diff under H2 (a `DBSCHEMA_SOURCE_TABLES` row present in the live
`RowsetDefinition` is missing from `XmlaBasicTest.ref.xml`). This is a
pre-existing drift unrelated to the H2 path; the test runs end-to-end against
H2 cleanly, only the golden ref needs updating to close the gap. Out of scope
for the H2-IT branch.

### Manual Docker build (if scripts don't work)
```bash
cd /home/andrey/work/emodrian_jdk25 && docker run --rm \
  -v "$(pwd)/mondrian:/build/mondrian" \
  -v "$HOME/.m2:/root/.m2" \
  -w /build/mondrian \
  maven:3.9-eclipse-temurin-25 bash -c '
    mvn -N install:install-file \
      -Dfile=mondrian/lib/javacup-10k.jar \
      -DgroupId=javacup -DartifactId=javacup -Dversion=10k -Dpackaging=jar -q 2>/dev/null
    mvn compile -f mondrian/pom.xml -DskipTests -q
  '
```

## Git Operations

**Git repo root**: `/home/andrey/work/emodrian_jdk25/mondrian`

```bash
# Always use -C or cd into the repo
git -C /home/andrey/work/emodrian_jdk25/mondrian status
git -C /home/andrey/work/emodrian_jdk25/mondrian add mondrian/src/...
git -C /home/andrey/work/emodrian_jdk25/mondrian commit -m "..."
```

The parent directory (`emodrian_jdk25/`) has its own `.gitignore` that ignores `/mondrian/`.
**Never try to git-add from the parent directory.**

## Tech Stack

- **Java 25** (source/target/release)
- **JUnit 5** (Jupiter) for unit tests
- **Mockito 5.15.2** for mocking
- **Records and sealed interfaces** are used throughout
- Unit tests: `mondrian/src/test/java/` (runs with `mvn test`)
- Integration tests: `mondrian/src/it/java/` (runs with `mvn verify -DrunITs`)

## Key Packages

- `mondrian.rolap` — OLAP engine core (NativeQueryEngine, NativeQuerySqlGenerator, etc.)
- `mondrian.rolap.nativesql` — Cell-phase native SQL registry
- `mondrian.rolap.agg` — Aggregation manager
- `mondrian.rolap.aggmatcher` — Aggregate table matching (AggStar)

## Fork-specific features (operator-facing)

### Skip heavy level properties from member/tuple SQL (V1-narrow)

Wide product/SKU levels can declare some `<Property>` columns as
on-demand so they are skipped from tuple/member reader SQL projection.

Two things must both be present:

1. Operator flag (default off, no behaviour change otherwise):
   ```properties
   mondrian.rolap.SkipOnDemandLevelProperties=true
   ```
2. Level annotation listing the property names:
   ```xml
   <Level name="SKU">
     <Annotations>
       <Annotation name="emondrian.onDemandProperties">URL,Claims,ChainURL</Annotation>
     </Annotations>
     <Property name="URL" .../>
     ...
   </Level>
   ```

Contract (schema author's responsibility): a property declared on-demand
must **not** be referenced by:
- MDX `.Properties("Name")`
- MDX `DIMENSION PROPERTIES [Level].[Name]` — V1-narrow is schema-time
  and does **not** override the skip per-query; explicit
  `DIMENSION PROPERTIES` for an on-demand property still returns
  `null`. Use a separate drillthrough/detail SQL for those columns.
- any `PropertyFormatter` / `MemberFormatter`
- `<CalculatedMember>` / `<NamedSet>` / `<Role>`
- olap4j post-hoc `member.getPropertyValue("Name")`

Accessing such a property after on-demand opt-in returns `null` silently
— that is the documented contract.

Diagnostics via log4j category `mondrian.rolap.PropertyProjection` at
INFO. Each SQL-projection decision logs reason and the projected /
skipped property lists.

See [#21](https://github.com/dronsv/mondrian/issues/21) (observability)
and [#22](https://github.com/dronsv/mondrian/issues/22) (V1-narrow design
+ cache-safety mapping).

### Query-driven RequiredPropertyProjection (V2)

Same goal as V1-narrow but **per-query inference** rather than
schema-author opt-in. The engine analyses each MDX at query-resolve
time and projects only those `<Property>` columns that are required by
the current query — anything not statically referenced is skipped.

Single flag to enable, no schema changes:

```properties
mondrian.rolap.RequiredPropertyProjection=true
```

The required set per level is computed from:
- engine-required expressions (key / caption / ordinal / parent —
  always projected, not governed by this flag)
- properties listed in MDX `DIMENSION PROPERTIES` per axis
- properties referenced by literal `.Properties("Name")` (case-
  insensitive when the global `mondrian.olap.case.sensitive=false`
  default holds) in any expression visited by the query (WHERE,
  Filter, Order, axis exprs, WITH MEMBER, the slicer)

**Fail-safe to eager** (level keeps eager projection — V1-narrow if
enabled, else all schema properties):
- the level is not mentioned by any required-property source for
  this query;
- the visitor sees an opaque construction — `.Properties(Iif(...))`,
  computed property name, UDF return, parameter; or `StrToMember` /
  `StrToTuple` / `StrToSet` anywhere in the query;
- any `DIMENSION PROPERTIES` id cannot be resolved to a hierarchy
  (whole query goes eager);
- the level has a `MemberFormatter` or any of its properties carries
  a `PropertyFormatter` — V2 cannot statically analyse Java
  formatter code, so the safe choice is eager.

**Known limitations** (tracked in #22):
- Schema-side `<CalculatedMember>` / `<NamedSet>` / `<Role>`
  expressions are not walked. References in schema-side MDX are
  invisible to V2; if a calc member needs property `X` that the
  user MDX doesn't also reference, V2 may prune `X`.
- No lazy fetch on `getPropertyValue(skipped)` — returns `null`
  silently (V2-M4 pending).

When the V2 flag is on **and** V1-narrow's
`SkipOnDemandLevelProperties` is also on, V2 takes precedence per
level (the V2 plan governs levels it mentions; V1-narrow applies to
levels V2 didn't touch).

Same `mondrian.rolap.PropertyProjection` diagnostic shows the per-site
decision. Same `member.getPropertyValue("Name") == null` contract
when a property is skipped (V2 does not currently re-fetch on demand
— that is the M4 milestone still pending in #22).

V2 commits: `9eed85d31` (M2 visitor), `532f9506d` (M1 mask),
`4f1a73803` (M3 per-query plan + SQL plumbing).
