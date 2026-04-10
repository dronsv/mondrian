# Maven Toolchain Setup

The Mondrian build uses the Maven toolchains plugin to explicitly select
a JDK for compilation and test execution. This is independent of the
`JAVA_HOME` you use to launch Maven itself.

## Setup

1. Copy the template to your local Maven config:
   ```bash
   cp build-support/toolchains.xml.template ~/.m2/toolchains.xml
   ```
2. Edit `~/.m2/toolchains.xml` and set `<jdkHome>` paths to your actual
   JDK installations.
3. Verify:
   ```bash
   mvn -Ptoolchain-strict validate
   ```
   Expected: build succeeds. If it fails with "Required JDK 25 not found",
   the `<jdkHome>` for JDK 25 is wrong or missing.

## Modes

**Relaxed (default)** — toolchain config is applied if present, but falls
back to the Maven runtime JDK if a matching toolchain is not found.
Intended for local ad-hoc builds.

```bash
mvn verify
```

**Strict (`-Ptoolchain-strict`)** — required for baseline capture, release
gates, and formal lane checks. Fails the build if the requested JDK
toolchain is not configured.

```bash
mvn -Ptoolchain-strict verify
```

## Why both modes

- **Reproducibility** — formal checks must pin the exact JDK to avoid
  "works on my machine" drift.
- **Developer ergonomics** — local builds shouldn't fail just because a
  developer hasn't set up toolchains yet.

The strict profile is the contract; the relaxed default is the convenience.
