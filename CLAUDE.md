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
│   └── test.sh            ← run tests via Docker
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
