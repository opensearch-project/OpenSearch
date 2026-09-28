/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link StatsRecorder}.
 *
 * <p>Covers all four public methods and their contracts:
 * <ul>
 *   <li>time is always recorded (success and failure paths)</li>
 *   <li>{@code onSuccess}/{@code onFailure} are mutually exclusive</li>
 *   <li>the original exception propagates unchanged</li>
 *   <li>return values pass through</li>
 *   <li>null arguments are rejected</li>
 * </ul>
 */
public class StatsRecorderTests extends OpenSearchTestCase {

    // ──────────────────────────────────────────────────────────────────────
    // recordTimeMillis(IORunnable, ...)
    // ──────────────────────────────────────────────────────────────────────

    public void testRecordTimeMillisVoidRunsWorkAndRecordsTime() throws IOException {
        AtomicBoolean ran = new AtomicBoolean(false);
        AtomicLong recorded = new AtomicLong(-1);

        StatsRecorder.recordTimeMillis(() -> ran.set(true), recorded::set);

        assertTrue("work should have run", ran.get());
        assertTrue("elapsed millis should be recorded (>= 0)", recorded.get() >= 0L);
    }

    public void testRecordTimeMillisVoidRecordsTimeEvenOnThrow() {
        AtomicLong recorded = new AtomicLong(-1);
        IOException boom = new IOException("boom");

        IOException thrown = expectThrows(
            IOException.class,
            () -> StatsRecorder.recordTimeMillis((StatsRecorder.IORunnable) () -> { throw boom; }, recorded::set)
        );

        assertSame("the original exception must propagate", boom, thrown);
        assertTrue("elapsed millis must be recorded even when work throws", recorded.get() >= 0L);
    }

    public void testRecordTimeMillisVoidRejectsNullArgs() {
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordTimeMillis((StatsRecorder.IORunnable) null, v -> {}));
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordTimeMillis(() -> {}, null));
    }

    // ──────────────────────────────────────────────────────────────────────
    // recordTimeMillis(IOSupplier, ...) — value-returning
    // ──────────────────────────────────────────────────────────────────────

    public void testRecordTimeMillisValueReturnsResultAndRecordsTime() throws IOException {
        AtomicLong recorded = new AtomicLong(-1);

        String result = StatsRecorder.recordTimeMillis(() -> "hello", recorded::set);

        assertEquals("hello", result);
        assertTrue("elapsed millis should be recorded", recorded.get() >= 0L);
    }

    public void testRecordTimeMillisValuePassesThroughNullResult() throws IOException {
        AtomicLong recorded = new AtomicLong(-1);

        String result = StatsRecorder.recordTimeMillis(() -> null, recorded::set);

        assertNull("null result must pass through", result);
        assertTrue(recorded.get() >= 0L);
    }

    public void testRecordTimeMillisValueRecordsTimeEvenOnThrow() {
        AtomicLong recorded = new AtomicLong(-1);
        IOException boom = new IOException("boom");

        IOException thrown = expectThrows(IOException.class, () -> StatsRecorder.recordTimeMillis((StatsRecorder.IOSupplier<String>) () -> {
            throw boom;
        }, recorded::set));

        assertSame(boom, thrown);
        assertTrue("elapsed millis must be recorded even when work throws", recorded.get() >= 0L);
    }

    public void testRecordTimeMillisValueRejectsNullArgs() {
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordTimeMillis((StatsRecorder.IOSupplier<String>) null, v -> {}));
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordTimeMillis(() -> "x", null));
    }

    // ──────────────────────────────────────────────────────────────────────
    // recordOutcome(IOSupplier, ...) — value-returning
    // ──────────────────────────────────────────────────────────────────────

    public void testRecordOutcomeValueSuccessFiresOnSuccessOnly() throws IOException {
        AtomicLong recorded = new AtomicLong(-1);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);

        String result = StatsRecorder.recordOutcome(() -> "ok", recorded::set, success::incrementAndGet, failure::incrementAndGet);

        assertEquals("ok", result);
        assertEquals("onSuccess must fire exactly once", 1, success.get());
        assertEquals("onFailure must not fire on success", 0, failure.get());
        assertTrue("time must be recorded", recorded.get() >= 0L);
    }

    public void testRecordOutcomeValueFailureFiresOnFailureOnly() {
        AtomicLong recorded = new AtomicLong(-1);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);
        IOException boom = new IOException("boom");

        IOException thrown = expectThrows(IOException.class, () -> StatsRecorder.recordOutcome((StatsRecorder.IOSupplier<String>) () -> {
            throw boom;
        }, recorded::set, success::incrementAndGet, failure::incrementAndGet));

        assertSame("original exception must propagate", boom, thrown);
        assertEquals("onFailure must fire exactly once", 1, failure.get());
        assertEquals("onSuccess must not fire on failure", 0, success.get());
        assertTrue("time must be recorded even on failure", recorded.get() >= 0L);
    }

    public void testRecordOutcomeValuePropagatesRuntimeException() {
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);
        RuntimeException boom = new IllegalStateException("rt-boom");

        RuntimeException thrown = expectThrows(
            IllegalStateException.class,
            () -> StatsRecorder.recordOutcome((StatsRecorder.IOSupplier<String>) () -> {
                throw boom;
            }, v -> {}, success::incrementAndGet, failure::incrementAndGet)
        );

        assertSame(boom, thrown);
        assertEquals("onFailure fires on RuntimeException too", 1, failure.get());
        assertEquals(0, success.get());
    }

    public void testRecordOutcomeValueRejectsNullArgs() {
        expectThrows(
            NullPointerException.class,
            () -> StatsRecorder.recordOutcome((StatsRecorder.IOSupplier<String>) null, v -> {}, () -> {}, () -> {})
        );
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordOutcome(() -> "x", null, () -> {}, () -> {}));
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordOutcome(() -> "x", v -> {}, null, () -> {}));
        expectThrows(NullPointerException.class, () -> StatsRecorder.recordOutcome(() -> "x", v -> {}, () -> {}, null));
    }

    // ──────────────────────────────────────────────────────────────────────
    // recordOutcome(IORunnable, ...) — void
    // ──────────────────────────────────────────────────────────────────────

    public void testRecordOutcomeVoidSuccessFiresOnSuccessOnly() throws IOException {
        AtomicBoolean ran = new AtomicBoolean(false);
        AtomicLong recorded = new AtomicLong(-1);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);

        StatsRecorder.recordOutcome(() -> ran.set(true), recorded::set, success::incrementAndGet, failure::incrementAndGet);

        assertTrue("work should have run", ran.get());
        assertEquals(1, success.get());
        assertEquals(0, failure.get());
        assertTrue(recorded.get() >= 0L);
    }

    public void testRecordOutcomeVoidFailureFiresOnFailureOnly() {
        AtomicLong recorded = new AtomicLong(-1);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);
        IOException boom = new IOException("boom");

        IOException thrown = expectThrows(
            IOException.class,
            () -> StatsRecorder.recordOutcome(
                (StatsRecorder.IORunnable) () -> { throw boom; },
                recorded::set,
                success::incrementAndGet,
                failure::incrementAndGet
            )
        );

        assertSame(boom, thrown);
        assertEquals(1, failure.get());
        assertEquals(0, success.get());
        assertTrue(recorded.get() >= 0L);
    }

    public void testRecordOutcomeVoidRejectsNullWork() {
        expectThrows(
            NullPointerException.class,
            () -> StatsRecorder.recordOutcome((StatsRecorder.IORunnable) null, v -> {}, () -> {}, () -> {})
        );
    }

    // ──────────────────────────────────────────────────────────────────────
    // Ordering / interaction contracts
    // ──────────────────────────────────────────────────────────────────────

    public void testTimeRecordedBeforeOnSuccessCallback() throws IOException {
        // Contract: the time recorder runs in finally, before onSuccess is invoked.
        AtomicReference<String> order = new AtomicReference<>("");

        StatsRecorder.recordOutcome(
            () -> "v",
            millis -> order.updateAndGet(s -> s + "T"),
            () -> order.updateAndGet(s -> s + "S"),
            () -> order.updateAndGet(s -> s + "F")
        );

        assertEquals("time (T) must be recorded before onSuccess (S)", "TS", order.get());
    }

    public void testOnFailureRunsBeforeExceptionPropagatesAndTimeStillRecorded() {
        // Contract: onFailure runs inside catch (before rethrow); time recorder runs in finally.
        AtomicReference<String> order = new AtomicReference<>("");
        IOException boom = new IOException("boom");

        expectThrows(
            IOException.class,
            () -> StatsRecorder.recordOutcome(
                (StatsRecorder.IOSupplier<String>) () -> { throw boom; },
                millis -> order.updateAndGet(s -> s + "T"),
                () -> order.updateAndGet(s -> s + "S"),
                () -> order.updateAndGet(s -> s + "F")
            )
        );

        // onFailure (F) fires in catch, then time (T) in finally; onSuccess (S) never fires.
        assertEquals("expected onFailure then time, no onSuccess", "FT", order.get());
    }
}
