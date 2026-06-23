/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link NativeCompletionCallbacks} — the per-call listener registry behind the
 * async-completion upcall. These exercise the Part D invariants without crossing the FFM boundary:
 * register-before-initiate, completion-exactly-once, reclaim-on-sync-outcome (leak probe), and
 * throwing-listener-doesn't-escape.
 */
public class NativeCompletionCallbacksTests extends OpenSearchTestCase {

    private static MemorySegment errSegment(Arena arena, String msg) {
        byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
        MemorySegment seg = arena.allocate(bytes.length);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return seg;
    }

    public void testSuccessDeliversValueAndConsumes() {
        AtomicLong got = new AtomicLong(-1);
        long id = NativeCompletionCallbacks.register(ActionListener.wrap(got::set, e -> fail("unexpected failure")));

        int consumed = NativeCompletionCallbacks.onNativeComplete(id, 4242L, MemorySegment.NULL, 0);

        assertEquals("Java accepted ownership", 1, consumed);
        assertEquals(4242L, got.get());
        assertNull("listener removed after firing", NativeCompletionCallbacks.unregister(id));
    }

    public void testErrorPathDeliversFailureAndConsumes() {
        AtomicReference<Exception> failure = new AtomicReference<>();
        long id = NativeCompletionCallbacks.register(ActionListener.wrap(v -> fail("unexpected success"), failure::set));

        try (Arena arena = Arena.ofConfined()) {
            String msg = "boom from native";
            MemorySegment seg = errSegment(arena, msg);
            int consumed = NativeCompletionCallbacks.onNativeComplete(id, 0L, seg, msg.length());
            assertEquals(1, consumed);
        }
        assertNotNull("failure delivered", failure.get());
        assertTrue("message preserved", failure.get().getMessage().contains("boom from native"));
    }

    public void testCompletionExactlyOnce() {
        AtomicInteger fires = new AtomicInteger();
        long id = NativeCompletionCallbacks.register(ActionListener.wrap(v -> fires.incrementAndGet(), e -> fires.incrementAndGet()));

        int first = NativeCompletionCallbacks.onNativeComplete(id, 1L, MemorySegment.NULL, 0);
        int second = NativeCompletionCallbacks.onNativeComplete(id, 1L, MemorySegment.NULL, 0);

        assertEquals("first completion accepted", 1, first);
        assertEquals("second completion finds no listener — Rust reclaims", 0, second);
        assertEquals("listener fired exactly once", 1, fires.get());
    }

    public void testUnregisterReclaimsBeforeCompletion() {
        AtomicInteger fires = new AtomicInteger();
        long id = NativeCompletionCallbacks.register(ActionListener.wrap(v -> fires.incrementAndGet(), e -> fires.incrementAndGet()));

        // Sync-outcome / initiation-failure path: caller reclaims the listener itself.
        ActionListener<Long> reclaimed = NativeCompletionCallbacks.unregister(id);
        assertNotNull("unregister returns the live listener", reclaimed);

        // A racing completion now finds nothing and tells Rust to reclaim (consumed=0).
        int consumed = NativeCompletionCallbacks.onNativeComplete(id, 7L, MemorySegment.NULL, 0);
        assertEquals(0, consumed);
        assertEquals("listener never fired via the registry", 0, fires.get());
    }

    public void testThrowingListenerDoesNotEscapeAndReturnsNotConsumed() {
        // A raw listener whose onResponse throws directly (ActionListener.wrap would instead reroute
        // an onResponse exception to onFailure, so use an explicit listener to hit the catch-all).
        ActionListener<Long> thrower = new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                throw new RuntimeException("listener blew up");
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("listener blew up (failure)");
            }
        };
        long id = NativeCompletionCallbacks.register(thrower);

        // A Throwable escaping the upcall stub would crash the JVM; onNativeComplete must swallow it
        // and report not-consumed so Rust reclaims the batch.
        int consumed = NativeCompletionCallbacks.onNativeComplete(id, 1L, MemorySegment.NULL, 0);
        assertEquals(0, consumed);
    }

    public void testPendingMapDrainsToEmpty() {
        int before = NativeCompletionCallbacks.pendingCount();
        long a = NativeCompletionCallbacks.register(ActionListener.wrap(v -> {}, e -> {}));
        long b = NativeCompletionCallbacks.register(ActionListener.wrap(v -> {}, e -> {}));
        assertEquals(before + 2, NativeCompletionCallbacks.pendingCount());

        NativeCompletionCallbacks.onNativeComplete(a, 0L, MemorySegment.NULL, 0);
        NativeCompletionCallbacks.unregister(b);

        assertEquals("no listeners leak after completion + reclaim", before, NativeCompletionCallbacks.pendingCount());
    }
}
