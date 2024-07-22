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
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchRunnableExecutorTest extends OpenSearchTestCase {
    private Supplier<TimeValue> timeoutSupplier;
    private TimeoutAwareRunnable runnable1;
    private TimeoutAwareRunnable runnable2;
    private TimeoutAwareRunnable runnable3;
    private List<TimeoutAwareRunnable> runnableList;

    public void testRunWithoutTimeout() {
        timeoutSupplier = mock(Supplier.class);
        runnable1 = mock(TimeoutAwareRunnable.class);
        runnable2 = mock(TimeoutAwareRunnable.class);
        runnable3 = mock(TimeoutAwareRunnable.class);
        runnableList = Arrays.asList(runnable1, runnable2, runnable3);
        when(timeoutSupplier.get()).thenReturn(TimeValue.timeValueSeconds(1));
        BatchRunnableExecutor executor = new BatchRunnableExecutor(runnableList, timeoutSupplier);
        executor.run();
        verify(runnable1, times(1)).run();
        verify(runnable2, times(1)).run();
        verify(runnable3, times(1)).run();
        verify(runnable1, never()).onTimeout();
        verify(runnable2, never()).onTimeout();
        verify(runnable3, never()).onTimeout();
    }

    public void testRunWithTimeout() {
        timeoutSupplier = mock(Supplier.class);
        runnable1 = mock(TimeoutAwareRunnable.class);
        runnable2 = mock(TimeoutAwareRunnable.class);
        runnable3 = mock(TimeoutAwareRunnable.class);
        runnableList = Arrays.asList(runnable1, runnable2, runnable3);
        when(timeoutSupplier.get()).thenReturn(TimeValue.timeValueNanos(1));
        BatchRunnableExecutor executor = new BatchRunnableExecutor(runnableList, timeoutSupplier);
        executor.run();
        verify(runnable1, times(1)).onTimeout();
        verify(runnable2, times(1)).onTimeout();
        verify(runnable3, times(1)).onTimeout();
        verify(runnable1, never()).run();
        verify(runnable2, never()).run();
        verify(runnable3, never()).run();
    }

    // public void testRunWithPartialTimeout() {
    // timeoutSupplier = mock(Supplier.class);
    // runnable1 = mock(TimeoutAwareRunnable.class);
    // runnable2 = mock(TimeoutAwareRunnable.class);
    // runnable3 = mock(TimeoutAwareRunnable.class);
    // runnableList = Arrays.asList(runnable1, runnable2, runnable3);
    // when(timeoutSupplier.get()).thenReturn(TimeValue.timeValueMillis(100));
    // BatchRunnableExecutor executor = new BatchRunnableExecutor(runnableList, timeoutSupplier);
    // doAnswer(invocationOnMock -> {
    // Thread.sleep(1000);
    // return null;
    // }).when(runnable1).run();
    // executor.run();
    // verify(runnable1, times(1)).run();
    // verify(runnable2, never()).run();
    // verify(runnable3, never()).run();
    // verify(runnable2, times(1)).onTimeout();
    // verify(runnable3, times(1)).onTimeout();
    // }

    public void testRunWithEmptyRunnableList() {
        timeoutSupplier = mock(Supplier.class);
        runnable1 = mock(TimeoutAwareRunnable.class);
        runnable2 = mock(TimeoutAwareRunnable.class);
        runnable3 = mock(TimeoutAwareRunnable.class);
        runnableList = Arrays.asList(runnable1, runnable2, runnable3);
        BatchRunnableExecutor executor = new BatchRunnableExecutor(Collections.emptyList(), timeoutSupplier);
        executor.run();
        verify(runnable1, never()).onTimeout();
        verify(runnable2, never()).onTimeout();
        verify(runnable3, never()).onTimeout();
        verify(runnable1, never()).run();
        verify(runnable2, never()).run();
        verify(runnable3, never()).run();
    }
}
