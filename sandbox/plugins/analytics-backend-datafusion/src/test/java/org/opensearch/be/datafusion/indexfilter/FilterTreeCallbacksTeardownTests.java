/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the refcounted deferred-close binding lifecycle in
 * {@link FilterTreeCallbacks}.
 *
 * <p>Regression coverage for the node-killing teardown race
 * (<a href="https://github.com/opensearch-project/OpenSearch/issues/22753">#22753</a>):
 * a partially-consumed native stream's final drop runs on a DataFusion runtime thread
 * after Java-side teardown, so its {@code release*} upcalls arrive after the old
 * {@code unregister} removed the binding — and the stub's re-thrown
 * {@code AssertionError} crossed the FFM boundary, fatally crashing the JVM.
 */
public class FilterTreeCallbacksTeardownTests extends OpenSearchTestCase {

    private static final long CTX = 4242L;

    /** Minimal handle counting create/release/close calls. */
    private static final class CountingHandle implements FilterDelegationHandle {
        final AtomicInteger nextKey = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();

        @Override
        public int createProvider(int annotationId) {
            return nextKey.getAndIncrement();
        }

        @Override
        public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            return nextKey.getAndIncrement();
        }

        @Override
        public long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
            return 0L;
        }

        @Override
        public void releaseCollector(int collectorKey) {
            released.incrementAndGet();
        }

        @Override
        public void releaseProvider(int providerKey) {
            released.incrementAndGet();
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }

    @Override
    public void tearDown() throws Exception {
        FilterTreeCallbacks.unregister(CTX);
        super.tearDown();
    }

    /** requestClose with no outstanding native handles closes immediately. */
    public void testRequestCloseWithNoOutstandingHandlesClosesImmediately() {
        CountingHandle handle = new CountingHandle();
        FilterTreeCallbacks.register(CTX, handle, null);

        FilterTreeCallbacks.requestClose(CTX);

        assertEquals("handle must be closed", 1, handle.closed.get());
        // Binding is gone: a subsequent create trips the lifecycle assert.
        expectThrows(AssertionError.class, () -> FilterTreeCallbacks.createProvider(CTX, 1));
    }

    /**
     * The #22753 regression: teardown runs while native handles are outstanding.
     * The binding must survive until the late release upcalls arrive; the last
     * release completes the close. No throw anywhere.
     */
    public void testLateReleaseAfterRequestCloseCompletesTeardown() {
        CountingHandle handle = new CountingHandle();
        FilterTreeCallbacks.register(CTX, handle, null);

        int providerKey = FilterTreeCallbacks.createProvider(CTX, 7);
        int collectorKey = FilterTreeCallbacks.createCollector(CTX, providerKey, 1L, 0, 100);
        assertTrue(providerKey >= 0 && collectorKey >= 0);

        // Java-side teardown finishes first (the reported interleaving).
        FilterTreeCallbacks.requestClose(CTX);
        assertEquals("close must be deferred while handles are outstanding", 0, handle.closed.get());

        // Late upcalls from the native drop (previously: AssertionError -> JVM fatal).
        // The binding is still alive, so these must succeed without tripping any assert.
        FilterTreeCallbacks.releaseCollector(CTX, collectorKey);
        assertEquals("still one handle outstanding", 0, handle.closed.get());

        FilterTreeCallbacks.releaseProvider(CTX, providerKey);
        assertEquals("last release must complete the deferred close", 1, handle.closed.get());
        assertEquals("both releases must reach the handle", 2, handle.released.get());
    }

    /** After the deferred close completes, a further release IS a genuine bug — asserts in tests. */
    public void testReleaseAfterFullTeardownAsserts() {
        CountingHandle handle = new CountingHandle();
        FilterTreeCallbacks.register(CTX, handle, null);
        int key = FilterTreeCallbacks.createProvider(CTX, 7);
        FilterTreeCallbacks.requestClose(CTX);
        FilterTreeCallbacks.releaseProvider(CTX, key); // completes close

        // Double release: the binding is gone, the lifecycle assert fires.
        expectThrows(AssertionError.class, () -> FilterTreeCallbacks.releaseProvider(CTX, key));
        assertEquals("handle must not be closed twice", 1, handle.closed.get());
    }

    /** Creates racing requestClose must never double-close or lose the close. */
    public void testConcurrentReleasesAndRequestCloseCloseExactlyOnce() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CountingHandle handle = new CountingHandle();
            FilterTreeCallbacks.register(CTX, handle, null);
            int k1 = FilterTreeCallbacks.createProvider(CTX, 1);
            int k2 = FilterTreeCallbacks.createCollector(CTX, k1, 1L, 0, 10);

            CountDownLatch start = new CountDownLatch(1);
            Thread releaser1 = new Thread(() -> {
                await(start);
                FilterTreeCallbacks.releaseCollector(CTX, k2);
            });
            Thread releaser2 = new Thread(() -> {
                await(start);
                FilterTreeCallbacks.releaseProvider(CTX, k1);
            });
            Thread closer = new Thread(() -> {
                await(start);
                FilterTreeCallbacks.requestClose(CTX);
            });
            releaser1.start();
            releaser2.start();
            closer.start();
            start.countDown();
            releaser1.join();
            releaser2.join();
            closer.join();

            assertEquals("iter " + iter + ": handle must be closed exactly once", 1, handle.closed.get());
            FilterTreeCallbacks.unregister(CTX);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
