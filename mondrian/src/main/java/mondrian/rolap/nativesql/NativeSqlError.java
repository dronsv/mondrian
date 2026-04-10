/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.nativesql;

/**
 * Error classification + typed sentinels for the cell-phase native registry.
 *
 * <p>Per Section 3 of the design spec: FALLBACK is opt-in via typed sentinels
 * only.  {@link #classify(Throwable)} is a pure function of the throwable
 * type — no branching on SQL state codes, error message text, or work-unit
 * metadata.  Consumers that want a specific failure to route to the legacy
 * path throw {@link UnsupportedTemplateShape} or {@link MissingOptionalArtifact}
 * explicitly at the work-unit SQL-execution adapter layer.
 *
 * <p>Anything else — including generic {@link java.sql.SQLException}, timeout,
 * cancellation, runtime exceptions from consumer {@code consume(ResultSet)},
 * and unknown throwables — defaults to PROPAGATE.  Silent fallback on unknown
 * errors would hide regressions.
 */
public final class NativeSqlError {

    private NativeSqlError() { /* utility */ }

    public enum Classification {
        /** Recoverable via legacy path; consumer opted in via typed sentinel. */
        FALLBACK,
        /** Must surface; either correctness-critical or performance-visible. */
        PROPAGATE
    }

    /**
     * Pure function.  Classifies a throwable into {@link Classification}.
     * Stable across invocations and independent of work-unit metadata.
     *
     * <p>See Section 3 of the design spec for the full taxonomy.
     */
    public static Classification classify(Throwable t) {
        if (t instanceof UnsupportedTemplateShape) return Classification.FALLBACK;
        if (t instanceof MissingOptionalArtifact) return Classification.FALLBACK;
        return Classification.PROPAGATE;
    }

    /**
     * Typed sentinel raised by a consumer's SQL builder when the templated
     * SQL cannot express a particular axis combination or measure shape.
     *
     * <p>This exception is the consumer's opt-in to the FALLBACK path: it
     * declares "this is a known-optional optimization; the legacy evaluator
     * can handle this case correctly."  Throwing this from arbitrary
     * locations without that guarantee is a consumer bug.
     */
    public static final class UnsupportedTemplateShape extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public UnsupportedTemplateShape(String reason) {
            super(reason);
        }

        public UnsupportedTemplateShape(String reason, Throwable cause) {
            super(reason, cause);
        }
    }

    /**
     * Typed sentinel raised by a work unit's SQL-execution adapter when a
     * required native artifact is missing BUT the work unit has explicitly
     * declared the artifact as optional at construction time.
     *
     * <p>Only raised after the consumer checks {@code isArtifactOptional(id)}
     * and finds {@code true}.  Missing objects that were not pre-declared as
     * optional must be rethrown as-is (they classify as PROPAGATE).
     */
    public static final class MissingOptionalArtifact extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final String artifactId;

        public MissingOptionalArtifact(String artifactId) {
            super("missing optional native artifact: " + artifactId);
            this.artifactId = artifactId;
        }

        public MissingOptionalArtifact(String artifactId, Throwable cause) {
            super("missing optional native artifact: " + artifactId, cause);
            this.artifactId = artifactId;
        }

        public String artifactId() {
            return artifactId;
        }
    }
}
