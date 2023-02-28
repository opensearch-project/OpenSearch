/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ConcurrentInvocationLinearizerTests extends OpenSearchTestCase {
    private ExecutorService executorService;

    @Before
    public void setup() {
        executorService = Executors.newSingleThreadExecutor();
    }

    public void testLinearizeShouldNotInvokeMethodMoreThanOnce() throws Exception {
        final ConcurrentInvocationLinearizer<String, String> invocationLinearizer = new ConcurrentInvocationLinearizer<>();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final Future<Future<String>> first = executorService.submit(() -> invocationLinearizer.linearizeInternal("input", s -> {
            startLatch.countDown();
            try {
                finishLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            return "expected";
        }));

        startLatch.await(); // Wait for first caller to start work
        final Future<String> second = invocationLinearizer.linearizeInternal("input", s -> { throw new AssertionError(); });
        final Future<String> third = invocationLinearizer.linearizeInternal("input", s -> { throw new AssertionError(); });
        finishLatch.countDown(); // Unblock first caller
        MatcherAssert.assertThat(first.get().get(), equalTo("expected"));
        MatcherAssert.assertThat(second.get(), equalTo("expected"));
        MatcherAssert.assertThat(third.get(), equalTo("expected"));
        MatcherAssert.assertThat(invocationLinearizer.getInvokeOnceCache(), is(anEmptyMap()));
    }

    public void testLinearizeSharesFailures() throws Exception {
        final ConcurrentInvocationLinearizer<String, String> invocationLinearizer = new ConcurrentInvocationLinearizer<>();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        final Future<Future<String>> first = executorService.submit(() -> invocationLinearizer.linearizeInternal("input", s -> {
            startLatch.countDown();
            try {
                finishLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            throw new IOException("io exception");
        }));

        startLatch.await(); // Wait for first caller to start work
        final Future<String> second = invocationLinearizer.linearizeInternal("input", s -> { throw new AssertionError(); });
        finishLatch.countDown(); // Unblock first caller
        final ExecutionException e1 = assertThrows(ExecutionException.class, () -> first.get().get());
        MatcherAssert.assertThat(e1.getCause(), instanceOf(IOException.class));
        final ExecutionException e2 = assertThrows(ExecutionException.class, second::get);
        MatcherAssert.assertThat(e2.getCause(), instanceOf(IOException.class));
        MatcherAssert.assertThat(invocationLinearizer.getInvokeOnceCache(), is(anEmptyMap()));
    }

    public void testLinearizeShouldLeaveCacheEmptyEvenWhenFutureFail() {
        ConcurrentInvocationLinearizer<String, String> invocationLinearizer = new ConcurrentInvocationLinearizer<>();
        assertThrows(
            RuntimeException.class,
            () -> invocationLinearizer.linearize("input", s -> { throw new RuntimeException("exception"); })
        );
        MatcherAssert.assertThat("Expected nothing to be cached on failure", invocationLinearizer.getInvokeOnceCache(), is(anEmptyMap()));
    }

    public void testExceptionHandling() {
        final ConcurrentInvocationLinearizer<String, String> invocationLinearizer = new ConcurrentInvocationLinearizer<>();
        assertThrows(
            RuntimeException.class,
            () -> invocationLinearizer.linearize("input", s -> { throw new RuntimeException("exception"); })
        );
        assertThrows(IOException.class, () -> invocationLinearizer.linearize("input", s -> { throw new IOException("exception"); }));
        assertThrows(AssertionError.class, () -> invocationLinearizer.linearize("input", s -> { throw new AssertionError("exception"); }));
        final RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> invocationLinearizer.linearize("input", s -> { throw sneakyThrow(new TestCheckedException()); })
        );
        MatcherAssert.assertThat(e.getCause(), instanceOf(TestCheckedException.class));
    }

    @After
    public void cleanUp() {
        executorService.shutdownNow();
        terminate(executorService);
    }

    // Some unholy hackery with generics to trick the compiler into throwing an undeclared checked exception
    private static <E extends Throwable> E sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    private static class TestCheckedException extends Exception {}
}
