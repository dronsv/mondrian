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

import java.util.Objects;

/**
 * Tagged result of a {@link CellPhaseNativeRegistry} lookup.
 *
 * <p>Sealed interface with four closed states: {@link Miss}, {@link Success},
 * {@link ErrorFallback}, {@link ErrorPropagate}.  Consumers dispatch via the
 * {@code isXxx()} state predicates and call the typed accessors only in the
 * matching state (otherwise {@link IllegalStateException}).
 *
 * <p>Per Contract 3 of the design spec, consumer re-entry follows:
 * <pre>
 *   MISS                 → register(work); throw valueNotReadyException
 *   SUCCESS              → return work.materialize(r.successPayload())
 *   ERROR/FALLBACK       → return fallbackOrNull()
 *   ERROR/PROPAGATE      → throw wrap(r.errorThrowable())
 * </pre>
 */
public sealed interface CellLookupResult
    permits CellLookupResult.Miss,
            CellLookupResult.Success,
            CellLookupResult.ErrorFallback,
            CellLookupResult.ErrorPropagate
{
    CellLookupResult MISS = new Miss();

    static CellLookupResult success(Object payload) {
        return new Success(payload);
    }

    static CellLookupResult errorFallback(Throwable t) {
        Objects.requireNonNull(t, "throwable");
        return new ErrorFallback(t);
    }

    static CellLookupResult errorPropagate(Throwable t) {
        Objects.requireNonNull(t, "throwable");
        return new ErrorPropagate(t);
    }

    default boolean isMiss()           { return false; }
    default boolean isSuccess()        { return false; }
    default boolean isErrorFallback() { return false; }
    default boolean isErrorPropagate() { return false; }

    default Object successPayload() {
        throw new IllegalStateException(
            getClass().getSimpleName() + " has no success payload");
    }

    default Throwable errorThrowable() {
        throw new IllegalStateException(
            getClass().getSimpleName() + " has no error throwable");
    }

    // -- state implementations (closed) --

    record Miss() implements CellLookupResult {
        @Override public boolean isMiss() { return true; }
    }

    record Success(Object payload) implements CellLookupResult {
        @Override public boolean isSuccess() { return true; }
        @Override public Object successPayload() { return payload; }
    }

    record ErrorFallback(Throwable throwable) implements CellLookupResult {
        @Override public boolean isErrorFallback() { return true; }
        @Override public Throwable errorThrowable() { return throwable; }
    }

    record ErrorPropagate(Throwable throwable) implements CellLookupResult {
        @Override public boolean isErrorPropagate() { return true; }
        @Override public Throwable errorThrowable() { return throwable; }
    }
}
