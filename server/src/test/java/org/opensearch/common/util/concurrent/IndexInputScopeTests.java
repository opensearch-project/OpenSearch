/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexInputScopeTests extends OpenSearchTestCase {

    public void testRegisterAndCloseAll() {
        IndexInputScope scope = new IndexInputScope();
        AtomicInteger closeCount = new AtomicInteger();
        Closeable input1 = closeCount::incrementAndGet;
        Closeable input2 = closeCount::incrementAndGet;
        Closeable input3 = closeCount::incrementAndGet;

        scope.register(input1);
        scope.register(input2);
        scope.register(input3);
        scope.closeAll();

        assertEquals(3, closeCount.get());
    }

    public void testCloseAllOnEmptyScope() {
        IndexInputScope scope = new IndexInputScope();
        scope.closeAll(); // should not throw
    }

    public void testCloseAllSuppressesIOException() {
        IndexInputScope scope = new IndexInputScope();
        AtomicInteger closeCount = new AtomicInteger();

        scope.register(() -> { throw new IOException("simulated failure"); });
        scope.register(closeCount::incrementAndGet);

        scope.closeAll(); // should not throw, should continue closing remaining inputs
        assertEquals(1, closeCount.get());
    }

    public void testCloseAllClearsList() {
        IndexInputScope scope = new IndexInputScope();
        AtomicInteger closeCount = new AtomicInteger();
        scope.register(closeCount::incrementAndGet);

        scope.closeAll();
        assertEquals(1, closeCount.get());

        // second closeAll should not close again
        scope.closeAll();
        assertEquals(1, closeCount.get());
    }

    public void testScopedValueBoundDuringRun() {
        assertFalse(IndexInputScope.SCOPE.isBound());

        AtomicBoolean wasBound = new AtomicBoolean();
        IndexInputScope scope = new IndexInputScope();
        ScopedValue.where(IndexInputScope.SCOPE, scope).run(() -> {
            wasBound.set(IndexInputScope.SCOPE.isBound());
            assertSame(scope, IndexInputScope.SCOPE.get());
        });

        assertTrue(wasBound.get());
        assertFalse(IndexInputScope.SCOPE.isBound());
    }

    public void testScopedValueNotBoundOutsideTask() {
        assertFalse(IndexInputScope.SCOPE.isBound());
    }

    public void testScopeIsolationBetweenTasks() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            IndexInputScope scope = new IndexInputScope();
            ScopedValue.where(IndexInputScope.SCOPE, scope).run(() -> {
                scope.register(closeCount::incrementAndGet);
                scope.register(closeCount::incrementAndGet);
                scope.closeAll();
                latch.countDown();
            });
        });

        Thread t2 = new Thread(() -> {
            IndexInputScope scope = new IndexInputScope();
            ScopedValue.where(IndexInputScope.SCOPE, scope).run(() -> {
                scope.register(closeCount::incrementAndGet);
                scope.closeAll();
                latch.countDown();
            });
        });

        t1.start();
        t2.start();
        latch.await();

        assertEquals(3, closeCount.get());
    }

    public void testScopeInThreadPoolExecutor() throws Exception {
        AtomicBoolean scopeBound = new AtomicBoolean();
        AtomicInteger closeCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            "test-scope",
            1,
            10,
            OpenSearchExecutors.daemonThreadFactory("test-scope"),
            new ThreadContext(Settings.EMPTY)
        );

        try {
            executor.execute(() -> {
                scopeBound.set(IndexInputScope.SCOPE.isBound());
                if (IndexInputScope.SCOPE.isBound()) {
                    IndexInputScope.SCOPE.get().register(closeCount::incrementAndGet);
                    IndexInputScope.SCOPE.get().register(closeCount::incrementAndGet);
                }
                done.countDown();
            });

            done.await();
            assertBusy(() -> assertEquals(2, closeCount.get()));
            assertTrue(scopeBound.get());
        } finally {
            executor.shutdown();
        }
    }

    public void testScopeClosesOnTaskException() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            "test-scope-ex",
            1,
            10,
            OpenSearchExecutors.daemonThreadFactory("test-scope-ex"),
            new ThreadContext(Settings.EMPTY)
        );

        try {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    // expected
                }

                @Override
                protected void doRun() {
                    if (IndexInputScope.SCOPE.isBound()) {
                        IndexInputScope.SCOPE.get().register(closeCount::incrementAndGet);
                    }
                    throw new RuntimeException("simulated task failure");
                }

                @Override
                public void onAfter() {
                    done.countDown();
                }
            });

            done.await();
            assertBusy(() -> assertEquals(1, closeCount.get()));
        } finally {
            executor.shutdown();
        }
    }

    public void testScopeNoOpWithoutRegistration() throws Exception {
        // Simulates running without the plugin — scope is created but nothing registers
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean scopeBound = new AtomicBoolean();

        OpenSearchThreadPoolExecutor executor = OpenSearchExecutors.newFixed(
            "test-no-plugin",
            1,
            10,
            OpenSearchExecutors.daemonThreadFactory("test-no-plugin"),
            new ThreadContext(Settings.EMPTY)
        );

        try {
            executor.execute(() -> {
                scopeBound.set(IndexInputScope.SCOPE.isBound());
                // Don't register anything — simulating no plugin installed
                done.countDown();
            });

            done.await();
            assertTrue("Scope should be bound even without plugin", scopeBound.get());
            // closeAll() ran on empty list — no errors, no side effects
        } finally {
            executor.shutdown();
        }
    }
}
