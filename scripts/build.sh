#!/usr/bin/env bash
# Compile Mondrian via Docker (no local Maven required).
# Usage: ./scripts/build.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PARENT_DIR="$(dirname "$REPO_ROOT")"

docker run --rm \
  -v "${REPO_ROOT}:/build/mondrian" \
  -v "${HOME}/.m2:/root/.m2" \
  -w /build/mondrian \
  maven:3.9-eclipse-temurin-25 bash -c '
    mvn -N install:install-file \
      -Dfile=mondrian/lib/javacup-10k.jar \
      -DgroupId=javacup -DartifactId=javacup -Dversion=10k -Dpackaging=jar \
      -q 2>/dev/null
    mvn compile -f mondrian/pom.xml -DskipTests -q
  '
echo "BUILD SUCCESS"
