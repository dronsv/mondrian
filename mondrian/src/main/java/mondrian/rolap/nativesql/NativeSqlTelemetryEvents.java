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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bounded in-memory ring buffer for recent native SQL telemetry events.
 *
 * <p>Phase 8f recent-failures-debugging substrate. Companion to
 * {@link NativeSqlTelemetry}, which owns the per-fingerprint counters
 * (see also the {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY} XMLA
 * rowset). This class owns the temporal/sequenced view: every native
 * SQL telemetry event is appended after the corresponding counter
 * bump, and the buffer is exposed via the
 * {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS} rowset for
 * diagnostic inspection.
 *
 * <p>Buffer capacity is fixed at {@link #BUFFER_CAPACITY} (2048) for
 * Phase 8f v1; not tunable via Mondrian property. Drop-oldest on
 * overflow. Never blocks; never throws (telemetry contract: advisory).
 *
 * <p>Spec: {@code docs/superpowers/specs/2026-05-08-phase-8f-events-ring-buffer-design.md}
 */
public final class NativeSqlTelemetryEvents {

    /**
     * Phase 8f v1 buffer capacity. Fixed; not tunable via Mondrian
     * property. A future stream may add a property if operators
     * report a concrete need.
     */
    public static final int BUFFER_CAPACITY = 2048;

    /**
     * Event kinds recorded into the ring buffer. Maps 1:1 onto the
     * {@link NativeSqlTelemetry} firing surface.
     */
    public enum EventType {
        EXECUTION_START,
        EXECUTION_SUCCESS,
        EXECUTION_FAILED,
        CACHED_SUCCESS_HIT,
        CACHED_ERROR_HIT,
        UNAUTHORIZED_DOWNGRADE,
        ON_ERROR_BUG,
        FINGERPRINT_KIND_VIOLATION
    }

    /**
     * Immutable record of a single telemetry event. Column-aligned
     * with the {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS}
     * rowset.
     *
     * <p>{@code message} is bounded to {@link #MESSAGE_MAX_LENGTH}
     * characters; longer raw inputs are truncated by the recorder
     * (the trailing character is replaced with a Unicode ellipsis).
     */
    public record EventRecord(
        long sequence,
        long timestampMs,
        String fingerprintId,
        EventType type,
        NativeSqlError.Classification classification,
        Long durationMs,
        String message)
    {
        /** Hard cap including the trailing ellipsis on truncation. */
        public static final int MESSAGE_MAX_LENGTH = 256;
    }

    // -- internal state, all guarded by LOCK --

    private static final Object LOCK = new Object();
    private static final EventRecord[] BUFFER =
        new EventRecord[BUFFER_CAPACITY];
    private static int head;          // next write position, 0..CAP-1
    private static int size;          // current count, 0..CAP
    private static long nextSequence; // monotonic; reset only by tests

    private NativeSqlTelemetryEvents() { /* utility */ }

    /**
     * Append an event to the ring buffer. Thread-safe. Drops oldest
     * on overflow. Never throws.
     *
     * <p>Telemetry contract: this method MUST NOT propagate any
     * exception from internal state mutation; a buffer failure is
     * silently swallowed because telemetry is advisory.
     *
     * <p>Null handling:
     * <ul>
     *   <li>{@code type == null}: the event is silently ignored. The
     *       buffer is unchanged; the next assigned sequence is
     *       unchanged.</li>
     *   <li>{@code fingerprintId == null}: the event is appended;
     *       the record's {@code fingerprintId} is null. Useful for
     *       diagnostic events that lack a fingerprint context.</li>
     *   <li>{@code rawMessageOrNull == null}: the record's
     *       {@code message} is null.</li>
     *   <li>{@code rawMessageOrNull} longer than
     *       {@link EventRecord#MESSAGE_MAX_LENGTH}: truncated to
     *       (MAX − 1) characters with a trailing Unicode ellipsis.</li>
     * </ul>
     */
    public static void record(
        EventType type,
        String fingerprintId,
        NativeSqlError.Classification classification,
        Long durationMs,
        String rawMessageOrNull)
    {
        try {
            if (type == null) {
                return;
            }
            String stored = truncate(rawMessageOrNull);
            synchronized (LOCK) {
                long seq = nextSequence;
                EventRecord rec = new EventRecord(
                    seq,
                    System.currentTimeMillis(),
                    fingerprintId,
                    type,
                    classification,
                    durationMs,
                    stored);
                BUFFER[head] = rec;
                head = (head + 1) % BUFFER_CAPACITY;
                if (size < BUFFER_CAPACITY) {
                    size++;
                }
                nextSequence = seq + 1L;
            }
        } catch (Throwable ignore) {
            // Telemetry is advisory; never propagate.
        }
    }

    /**
     * Immutable snapshot of all events currently in the ring buffer,
     * ordered by sequence ASC (oldest available first → newest).
     *
     * <p>Returned list is a defensive copy wrapped via
     * {@link Collections#unmodifiableList}; callers may iterate and
     * read without external synchronization. The buffer may continue
     * accepting new records after this method returns; the snapshot
     * does not see them.
     */
    public static List<EventRecord> snapshot() {
        synchronized (LOCK) {
            if (size == 0) {
                return Collections.emptyList();
            }
            List<EventRecord> out = new ArrayList<>(size);
            int oldest = (head - size + BUFFER_CAPACITY) % BUFFER_CAPACITY;
            for (int i = 0; i < size; i++) {
                out.add(BUFFER[(oldest + i) % BUFFER_CAPACITY]);
            }
            return Collections.unmodifiableList(out);
        }
    }

    /**
     * Test-only buffer reset. Clears all buffered events AND resets
     * the next sequence number to {@code 0}. After this call, the
     * next recorded event gets {@code EVENT_SEQUENCE = 0}.
     *
     * <p>Production code never calls this. Test isolation only;
     * callers in {@code NativeSqlTelemetryTest} and similar fixtures.
     */
    public static void resetForTests() {
        synchronized (LOCK) {
            for (int i = 0; i < BUFFER_CAPACITY; i++) {
                BUFFER[i] = null;
            }
            head = 0;
            size = 0;
            nextSequence = 0L;
        }
    }

    // -- internals --

    private static String truncate(String raw) {
        if (raw == null) {
            return null;
        }
        if (raw.length() <= EventRecord.MESSAGE_MAX_LENGTH) {
            return raw;
        }
        return raw.substring(0, EventRecord.MESSAGE_MAX_LENGTH - 1) + "…";
    }
}
