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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mondrian.rolap.nativesql.NativeSqlTelemetryEvents.EventRecord;
import mondrian.rolap.nativesql.NativeSqlTelemetryEvents.EventType;

class NativeSqlTelemetryEventsTest {

    @BeforeEach
    void resetBuffer() {
        NativeSqlTelemetryEvents.resetForTests();
    }

    @Test
    void recordAssignsSequentialIdsStartingAtZero() {
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp1", null, null, null);
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp2", null, null, null);
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp3", null, null, null);

        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(3, snap.size());
        assertEquals(0L, snap.get(0).sequence());
        assertEquals(1L, snap.get(1).sequence());
        assertEquals(2L, snap.get(2).sequence());
    }

    @Test
    void resetForTestsClearsBufferAndResetsSequence() {
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp", null, null, null);
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp", null, null, null);
        NativeSqlTelemetryEvents.resetForTests();

        assertEquals(0, NativeSqlTelemetryEvents.snapshot().size());

        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp", null, null, null);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(1, snap.size());
        assertEquals(0L, snap.get(0).sequence());
    }

    @Test
    void snapshotIsOrderedAscBySequence() {
        for (int i = 0; i < 5; i++) {
            NativeSqlTelemetryEvents.record(
                EventType.EXECUTION_START, "fp" + i, null, null, null);
        }
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        for (int i = 0; i < 5; i++) {
            assertEquals((long) i, snap.get(i).sequence());
        }
    }

    @Test
    void snapshotReturnsImmutableList() {
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp", null, null, null);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertThrows(UnsupportedOperationException.class,
            () -> snap.add(snap.get(0)));
    }

    @Test
    void bufferOverflowDropsOldest() {
        int n = NativeSqlTelemetryEvents.BUFFER_CAPACITY + 50;
        for (int i = 0; i < n; i++) {
            NativeSqlTelemetryEvents.record(
                EventType.EXECUTION_START, "fp" + i, null, null, null);
        }
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(NativeSqlTelemetryEvents.BUFFER_CAPACITY, snap.size());
        assertEquals(50L, snap.get(0).sequence());
        assertEquals((long) n - 1, snap.get(snap.size() - 1).sequence());
    }

    @Test
    void nullEventTypeIsIgnoredSilently() {
        NativeSqlTelemetryEvents.record(null, "fp", null, null, null);
        assertEquals(0, NativeSqlTelemetryEvents.snapshot().size());

        // Sequence stays at 0; the next valid record gets sequence 0.
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, "fp", null, null, null);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(1, snap.size());
        assertEquals(0L, snap.get(0).sequence());
    }

    @Test
    void nullFingerprintIdIsStored() {
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_START, null, null, null, null);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(1, snap.size());
        assertNull(snap.get(0).fingerprintId());
        assertSame(EventType.EXECUTION_START, snap.get(0).type());
    }

    @Test
    void messageTruncationLongMessage() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            sb.append('X');
        }
        String input = sb.toString();
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_FAILED,
            "fp",
            null,
            null,
            input);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(1, snap.size());
        String stored = snap.get(0).message();
        assertEquals(EventRecord.MESSAGE_MAX_LENGTH, stored.length());
        assertTrue(
            stored.endsWith("…"),
            "expected trailing Unicode ellipsis; got: '"
                + stored.substring(stored.length() - 5) + "'");
    }

    @Test
    void messageTruncationShortMessage() {
        NativeSqlTelemetryEvents.record(
            EventType.EXECUTION_FAILED, "fp", null, null, "ok");
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals("ok", snap.get(0).message());
    }

    @Test
    void concurrentRecordSequenceIsMonotonic() throws Exception {
        final int threads = 8;
        final int perThread = 500;
        final int total = threads * perThread;
        // Total > BUFFER_CAPACITY (4000 > 2048): buffer overflows; only the
        // last BUFFER_CAPACITY events survive.
        assertTrue(total > NativeSqlTelemetryEvents.BUFFER_CAPACITY);

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicInteger seqCounter = new AtomicInteger();
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        NativeSqlTelemetryEvents.record(
                            EventType.EXECUTION_START,
                            "fp",
                            null,
                            null,
                            null);
                        seqCounter.incrementAndGet();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            th.start();
            ts.add(th);
        }
        start.countDown();
        done.await();

        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        assertEquals(NativeSqlTelemetryEvents.BUFFER_CAPACITY, snap.size());

        // Strictly increasing sequence within the snapshot.
        for (int i = 1; i < snap.size(); i++) {
            assertTrue(
                snap.get(i).sequence() > snap.get(i - 1).sequence(),
                "expected strictly increasing sequence at index " + i);
        }
        // Newest sequence == total - 1; oldest == total - capacity.
        assertEquals((long) total - 1, snap.get(snap.size() - 1).sequence());
        assertEquals(
            (long) total - NativeSqlTelemetryEvents.BUFFER_CAPACITY,
            snap.get(0).sequence());
    }
}
