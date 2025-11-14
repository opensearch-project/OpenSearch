/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.common.util.FloatArray;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class TaskAwareBigArraysTests extends OpenSearchTestCase {

    private TaskAwareBigArrays taskAwareBigArrays;
    private Task mockTask;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
        CircuitBreakerService breakerService = new NoneCircuitBreakerService();
        taskAwareBigArrays = new TaskAwareBigArrays(recycler, breakerService, "test");
        mockTask = mock(Task.class);
        taskAwareBigArrays.setTask(mockTask);
    }

    public void testByteArrayAllocationTracking() {
        ByteArray array = taskAwareBigArrays.newByteArray(100, false);
        verify(mockTask).addApplicationManagedBytes(array.ramBytesUsed());
        array.close();
    }

    public void testIntArrayAllocationTracking() {
        IntArray array = taskAwareBigArrays.newIntArray(100, false);
        verify(mockTask).addApplicationManagedBytes(array.ramBytesUsed());
        array.close();
    }

    public void testLongArrayAllocationTracking() {
        LongArray array = taskAwareBigArrays.newLongArray(100, false);
        verify(mockTask).addApplicationManagedBytes(array.ramBytesUsed());
        array.close();
    }

    public void testDoubleArrayAllocationTracking() {
        DoubleArray array = taskAwareBigArrays.newDoubleArray(100, false);
        verify(mockTask).addApplicationManagedBytes(array.ramBytesUsed());
        array.close();
    }

    public void testFloatArrayAllocationTracking() {
        FloatArray array = taskAwareBigArrays.newFloatArray(100, false);
        verify(mockTask).addApplicationManagedBytes(array.ramBytesUsed());
        array.close();
    }

    public void testObjectArrayAllocationTracking() {
        ObjectArray<String> array = taskAwareBigArrays.newObjectArray(100);
        verify(mockTask).addApplicationManagedBytes(array.ramBytesUsed());
        array.close();
    }

    public void testNoTrackingWhenTaskIsNull() {
        taskAwareBigArrays.setTask(null);
        ByteArray array = taskAwareBigArrays.newByteArray(100, false);
        verifyNoInteractions(mockTask);
        array.close();
    }

    public void testMultipleAllocationsTracked() {
        ByteArray array1 = taskAwareBigArrays.newByteArray(50, false);
        IntArray array2 = taskAwareBigArrays.newIntArray(25, false);

        verify(mockTask).addApplicationManagedBytes(array1.ramBytesUsed());
        verify(mockTask).addApplicationManagedBytes(array2.ramBytesUsed());

        array1.close();
        array2.close();
    }
}
