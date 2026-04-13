#!/usr/bin/env bash
# Run Mondrian unit tests via Docker.
# Usage:
#   ./scripts/test.sh                          # all unit tests
#   ./scripts/test.sh FactResolvedTableTest    # single test class
#   ./scripts/test.sh "Foo,Bar"                # multiple test classes
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

TEST_FILTER=""
if [ -n "${1:-}" ]; then
  TEST_FILTER="-Dtest=$1 -DfailIfNoTests=false"
fi

docker run --rm \
  -v "${REPO_ROOT}:/build/mondrian" \
  -v "${HOME}/.m2:/root/.m2" \
  -w /build/mondrian \
  maven:3.9-eclipse-temurin-25 bash -c "
    mvn -N install:install-file \
      -Dfile=mondrian/lib/javacup-10k.jar \
      -DgroupId=javacup -DartifactId=javacup -Dversion=10k -Dpackaging=jar \
      -q 2>/dev/null
    mvn test -f mondrian/pom.xml ${TEST_FILTER} 2>&1 | tail -30
  "
