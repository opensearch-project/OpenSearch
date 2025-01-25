/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the BaseQueryRewriteContext class to verify the fix for racing conditions
 * in async action registration and execution.
 */
public class BaseQueryRewriteContextTests {
    private BaseQueryRewriteContext context;
    private Client mockClient;

    @Before
    public void setUp() {
        mockClient = mock(Client.class);
        context = new BaseQueryRewriteContext(
            mock(NamedXContentRegistry.class),
            mock(NamedWriteableRegistry.class),
            mockClient,
            () -> System.currentTimeMillis()
        );
    }

    /**
     * Tests concurrent registration and execution of async actions.
     *
     * This test simulates a scenario where multiple threads are simultaneously
     * registering a large number of async actions, followed by a single execution
     * of all registered actions. It verifies that:
     * 1. All registered actions are executed correctly.
     * 2. The total number of executed actions matches the expected count.
     * 3. There are no remaining async actions after execution.
     * 4. No exceptions occur during the process, indicating thread-safety.
     *
     * @throws InterruptedException if the test is interrupted while waiting for threads to complete
     */
    @Test
    public void testConcurrentRegistrationAndExecution() throws InterruptedException {
        int numThreads = 10;
        int actionsPerThread = 1000;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDown startCountDown = new CountDown(1);
        CountDown endCountDown = new CountDown(numThreads);
        AtomicInteger totalExecutedActions = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                while (startCountDown.isCountedDown() == false) {
                    Thread.yield();
                }
                for (int j = 0; j < actionsPerThread; j++) {
                    context.registerAsyncAction((client, listener) -> {
                        totalExecutedActions.incrementAndGet();
                        listener.onResponse(null);
                    });
                }
                endCountDown.countDown();
            });
        }

        startCountDown.countDown();
        while (endCountDown.isCountedDown() == false) {
            Thread.yield();
        }

        CountDown executionCountDown = new CountDown(1);
        context.executeAsyncActions(new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                executionCountDown.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Execution failed: " + e.getMessage());
            }
        });

        while (executionCountDown.isCountedDown() == false) {
            Thread.yield();
        }
        ensureAllActionsExecuted();
        assertEquals(numThreads * actionsPerThread, totalExecutedActions.get());
        assertFalse(context.hasAsyncActions());

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
    }

    /**
     * Tests the fix for the racing condition by simulating concurrent registration and execution.
     *
     * This test creates a scenario where multiple threads are simultaneously:
     * 1. Registering async actions
     * 2. Periodically executing the registered actions
     *
     * It verifies that:
     * 1. No exceptions occur during the process, indicating thread-safety.
     * 2. All actions are eventually executed, leaving no remaining async actions.
     * 3. The fix prevents any race conditions that could occur in this situation.
     *
     * @throws InterruptedException if the test is interrupted while waiting for threads to complete
     */
    @Test
    public void testRacingConditionFixed() throws InterruptedException {
        int numThreads = 5;
        int actionsPerThread = 1000;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDown startCountDown = new CountDown(1);
        CountDown endCountDown = new CountDown(numThreads);
        AtomicInteger totalExecutedActions = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                while (startCountDown.isCountedDown() == false) {
                    Thread.yield();
                }
                for (int j = 0; j < actionsPerThread; j++) {
                    context.registerAsyncAction((client, listener) -> {
                        totalExecutedActions.incrementAndGet();
                        listener.onResponse(null);
                    });
                    if (j % 100 == 0) {
                        context.executeAsyncActions(ActionListener.wrap(v -> {}, e -> fail("Execution failed: " + e.getMessage())));
                    }
                }
                endCountDown.countDown();
            });
        }

        startCountDown.countDown();
        while (endCountDown.isCountedDown() == false) {
            Thread.yield();
        }

        // Final execution to ensure all remaining actions are processed
        context.executeAsyncActions(ActionListener.wrap(v -> {}, e -> fail("Final execution failed: " + e.getMessage())));

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));  // Increased timeout
        ensureAllActionsExecuted();
        assertEquals(numThreads * actionsPerThread, totalExecutedActions.get());
        assertFalse(context.hasAsyncActions());
    }

    private void ensureAllActionsExecuted() {
        int maxAttempts = 10;
        for (int i = 0; i < maxAttempts && context.hasAsyncActions(); i++) {
            context.executeAsyncActions(ActionListener.wrap(v -> {}, e -> fail("Execution failed: " + e.getMessage())));
            try {
                Thread.sleep(100); // Give some time for actions to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
