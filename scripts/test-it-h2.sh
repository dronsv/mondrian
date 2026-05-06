#!/usr/bin/env bash
# Run Mondrian XMLA Discover IT probes against an embedded H2 FoodMart
# datasource, fully inside the project's Docker-wrapped Maven (no
# Docker-in-Docker, no host MySQL).
#
# Usage:
#   ./scripts/test-it-h2.sh                                    # default: XmlaDiscoverNativeSqlTelemetryTest
#   ./scripts/test-it-h2.sh mondrian.xmla.XmlaBasicTest        # any IT class
#   ./scripts/test-it-h2.sh "mondrian.xmla.XmlaBasicTest#testDSchemaRowsets"  # single method
#
# Activates Maven profiles `-DrunITs -P!embedded-mysql,it-h2-foodmart`:
#   -DrunITs auto-activates load-foodmart (which runs the loader);
#   -P!embedded-mysql suppresses the MySQL Docker sidecar that would
#     otherwise fail under Docker-in-Docker;
#   -Pit-h2-foodmart overrides mondrian.foodmart.jdbcURL to H2.
#
# The H2 file lands at mondrian/target/it-h2/foodmart.mv.db (~190+ MB
# after a full FoodMart load).  AUTO_SERVER=TRUE in the URL lets the
# loader (in-process) and Failsafe forked test JVM share the file.
#
# See docs/superpowers/plans/2026-05-06-fix-it-foodmart-datasource.md.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

IT_TEST="${1:-mondrian.xmla.XmlaDiscoverNativeSqlTelemetryTest}"

docker run --rm \
  -v "${REPO_ROOT}:/build/mondrian" \
  -v "${HOME}/.m2:/root/.m2" \
  -w /build/mondrian \
  maven:3.9-eclipse-temurin-25 bash -c "
    mvn -N install:install-file \
      -Dfile=mondrian/lib/javacup-10k.jar \
      -DgroupId=javacup -DartifactId=javacup -Dversion=10k -Dpackaging=jar \
      -q 2>/dev/null
    mvn pre-integration-test failsafe:integration-test -f mondrian/pom.xml \
      -DrunITs -P!embedded-mysql,it-h2-foodmart \
      -Dit.test=${IT_TEST} \
      -Dfailsafe.failIfNoSpecifiedTests=false \
      -Dmaven.test.failure.ignore=true \
      -Dskip.copy-aspectj=true 2>&1 | tail -40
  "
