/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend.jni;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link ConsumableNativeHandle}'s ownership-transfer contract.
 *
 * <p>The class guards against two specific failure modes:
 * <ul>
 *   <li><b>Double-free</b>: the Rust side consumed the pointer via
 *       {@code Box::from_raw}, then the Java-side {@code close()} calls
 *       {@code df_close_X} which tries to free the same memory again.</li>
 *   <li><b>Leak</b>: the consuming FFM call never dispatched (pre-invoke
 *       Java failure, aborted flow), so the Java wrapper is responsible for
 *       calling {@code df_close_X} exactly once.</li>
 * </ul>
 *
 * <p>Both paths rely on the {@code doCloseNative()} callback being invoked
 * exactly zero or one times, never twice. These tests nail that contract
 * down with a counting subclass so a future change to
 * {@link ConsumableNativeHandle} that accidentally re-introduces a
 * double-close will fail loudly.
 *
 * <p>Reference: the real subclass
 * {@code org.opensearch.be.datafusion.nativelib.SessionContextHandle} is used
 * from {@code DatafusionContext#close()} and
 * {@code DataFusionSessionState#close()} — both paths can reach
 * {@code close()} on the same instance, so idempotency is load-bearing.
 */
public class ConsumableNativeHandleTests extends OpenSearchTestCase {

    /**
     * Counts calls to {@link #doCloseNative()} so tests can assert exact
     * invocation counts.
     */
    private static final class CountingHandle extends ConsumableNativeHandle {
        final AtomicInteger nativeCloses = new AtomicInteger(0);

        CountingHandle(long ptr) {
            super(ptr);
        }

        @Override
        protected void doCloseNative() {
            nativeCloses.incrementAndGet();
        }
    }

    // ---- close() without consumption ------------------------------------

    public void testCloseWithoutConsumeCallsNativeOnce() {
        CountingHandle handle = new CountingHandle(100L);
        handle.close();
        assertEquals("doCloseNative should run once on the never-consumed path", 1, handle.nativeCloses.get());
    }

    public void testDoubleCloseWithoutConsumeStillCallsNativeOnce() {
        CountingHandle handle = new CountingHandle(101L);
        handle.close();
        handle.close();
        assertEquals("close() must be idempotent — second call is a no-op", 1, handle.nativeCloses.get());
    }

    // ---- markConsumed() ownership-transferred path ----------------------

    public void testMarkConsumedSkipsNativeClose() {
        CountingHandle handle = new CountingHandle(200L);
        handle.markConsumed();
        assertEquals(
            "markConsumed() must not call doCloseNative — the native side already freed the pointer",
            0,
            handle.nativeCloses.get()
        );
    }

    public void testCloseAfterMarkConsumedIsNoOp() {
        CountingHandle handle = new CountingHandle(201L);
        handle.markConsumed();
        handle.close();
        assertEquals(
            "An explicit close() after markConsumed() must remain a no-op — otherwise Rust's Box::from_raw would be followed by a second free",
            0,
            handle.nativeCloses.get()
        );
    }

    public void testMarkConsumedAfterCloseDoesNotRunNativeTwice() {
        // Order reversed from the normal happy path. The bridge always calls
        // markConsumed() after the FFM downcall returns, but the test ensures
        // that even if some future caller inverted the sequence, the native
        // close is never invoked twice.
        CountingHandle handle = new CountingHandle(202L);
        handle.close();
        assertEquals(1, handle.nativeCloses.get());
        handle.markConsumed();
        assertEquals("markConsumed() after close() must not trigger another native close", 1, handle.nativeCloses.get());
    }

    public void testMarkConsumedIsIdempotent() {
        CountingHandle handle = new CountingHandle(203L);
        handle.markConsumed();
        handle.markConsumed();
        handle.close();
        assertEquals(0, handle.nativeCloses.get());
    }

    // ---- State observation ---------------------------------------------

    public void testGetPointerAfterMarkConsumedThrows() {
        CountingHandle handle = new CountingHandle(300L);
        handle.markConsumed();
        // markConsumed() closes the Java wrapper eagerly; subsequent getPointer
        // should refuse to hand out the now-dangling value.
        expectThrows(IllegalStateException.class, handle::getPointer);
    }

    public void testIsLivePointerFalseAfterMarkConsumed() {
        CountingHandle handle = new CountingHandle(301L);
        assertTrue(NativeHandle.isLivePointer(301L));
        handle.markConsumed();
        assertFalse(
            "markConsumed() must remove the pointer from the live registry so validatePointer rejects it on a stale re-use",
            NativeHandle.isLivePointer(301L)
        );
    }

    public void testValidatePointerAfterMarkConsumedThrows() {
        CountingHandle handle = new CountingHandle(302L);
        handle.markConsumed();
        expectThrows(IllegalStateException.class, () -> NativeHandle.validatePointer(302L, "consumed"));
    }
}
