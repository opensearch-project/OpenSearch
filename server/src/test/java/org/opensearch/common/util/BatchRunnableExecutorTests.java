/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.TimeoutAwareRunnable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BatchRunnableExecutorTests extends OpenSearchTestCase {
    private Supplier<TimeValue> timeoutSupplier;
    private TimeoutAwareRunnable runnable1;
    private TimeoutAwareRunnable runnable2;
    private TimeoutAwareRunnable runnable3;
    private List<TimeoutAwareRunnable> runnableList;

    public void setupRunnables() {
        timeoutSupplier = mock(Supplier.class);
        runnable1 = mock(TimeoutAwareRunnable.class);
        runnable2 = mock(TimeoutAwareRunnable.class);
        runnable3 = mock(TimeoutAwareRunnable.class);
        runnableList = Arrays.asList(runnable1, runnable2, runnable3);
    }

    public void testRunWithoutTimeout() {
        setupRunnables();
        timeoutSupplier = () -> TimeValue.timeValueSeconds(1);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        BatchRunnableExecutor executor = new BatchRunnableExecutor(runnableList, timeoutSupplier) {
            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        };
        executor.run();
        verify(runnable1, times(1)).run();
        verify(runnable2, times(1)).run();
        verify(runnable3, times(1)).run();
        verify(runnable1, never()).onTimeout();
        verify(runnable2, never()).onTimeout();
        verify(runnable3, never()).onTimeout();
        assertEquals(0, countDownLatch.getCount());
    }

    public void testRunWithTimeout() {
        setupRunnables();
        timeoutSupplier = () -> TimeValue.timeValueNanos(1);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        BatchRunnableExecutor executor = new BatchRunnableExecutor(runnableList, timeoutSupplier) {
            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        };
        executor.run();
        verify(runnable1, times(1)).onTimeout();
        verify(runnable2, times(1)).onTimeout();
        verify(runnable3, times(1)).onTimeout();
        verify(runnable1, never()).run();
        verify(runnable2, never()).run();
        verify(runnable3, never()).run();
        assertEquals(0, countDownLatch.getCount());
    }

    public void testRunWithPartialTimeout() {
        setupRunnables();
        timeoutSupplier = () -> TimeValue.timeValueMillis(50);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        BatchRunnableExecutor executor = new BatchRunnableExecutor(runnableList, timeoutSupplier) {
            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        };
        doAnswer(invocation -> {
            Thread.sleep(100);
            return null;
        }).when(runnable1).run();
        executor.run();
        verify(runnable1, atMost(1)).run();
        verify(runnable2, atMost(1)).run();
        verify(runnable3, atMost(1)).run();
        verify(runnable2, atMost(1)).onTimeout();
        verify(runnable3, atMost(1)).onTimeout();
        verify(runnable2, atMost(1)).onTimeout();
        verify(runnable3, atMost(1)).onTimeout();
        assertEquals(0, countDownLatch.getCount());
    }

    public void testRunWithEmptyRunnableList() {
        setupRunnables();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        BatchRunnableExecutor executor = new BatchRunnableExecutor(Collections.emptyList(), timeoutSupplier) {
            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        };
        executor.run();
        verify(runnable1, never()).onTimeout();
        verify(runnable2, never()).onTimeout();
        verify(runnable3, never()).onTimeout();
        verify(runnable1, never()).run();
        verify(runnable2, never()).run();
        verify(runnable3, never()).run();
        assertEquals(1, countDownLatch.getCount());
    }
}
