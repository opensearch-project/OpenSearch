/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentInvocationLinearizerTests extends OpenSearchTestCase {
    private ExecutorService executorService;

    @Before
    public void setup() {
        executorService = Executors.newFixedThreadPool(4);
    }

    public void testLinearizeShouldNotInvokeMethodMoreThanOnce() {
        ConcurrentInvocationLinearizer<String, String> invocationLinearizer = new ConcurrentInvocationLinearizer<>(executorService);
        List<Future<String>> futures = new ArrayList<>();
        AtomicInteger invocationCount = new AtomicInteger(0);
        for (int i = 0; i < 4; i++) {
            int finalI = i;
            futures.add(invocationLinearizer.linearize("input", (s) -> {
                invocationCount.incrementAndGet();
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "val" + finalI;
            }));
        }
        futures.forEach(stringFuture -> {
            try {
                // make sure all futures are same object with value same as same first value submitted
                assertEquals("val0", stringFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        // make sure method calls got linearized
        assertEquals(1, invocationCount.get());
        // make sure cache is cleaned out of the box
        assertTrue(invocationLinearizer.getInvokeOnceCache().isEmpty());
    }

    public void testLinearizeShouldLeaveCacheEmptyEvenWhenFutureFail() throws Exception {
        ConcurrentInvocationLinearizer<String, String> invocationLinearizer = new ConcurrentInvocationLinearizer<>(executorService);
        Future<String> future = invocationLinearizer.linearize("input", s -> { throw new RuntimeException("exception"); });
        assertThrows(ExecutionException.class, () -> future.get());
        // make sure cache is cleaned out of the box
        assertTrue(invocationLinearizer.getInvokeOnceCache().isEmpty());
    }

    @After
    public void cleanUp() {
        executorService.shutdownNow();
        terminate(executorService);
    }
}
