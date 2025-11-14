/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.common.util.FloatArray;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.core.indices.breaker.CircuitBreakerService;

public class TaskAwareBigArrays extends BigArrays {
    private Task task;

    public TaskAwareBigArrays(PageCacheRecycler recycler, CircuitBreakerService breakerService, String breakerName) {
        super(recycler, breakerService, breakerName);
    }

    public TaskAwareBigArrays(PageCacheRecycler recycler, CircuitBreakerService breakerService, String breakerName, boolean checkBreaker) {
        super(recycler, breakerService, breakerName, checkBreaker);
    }

    public void setTask(Task task) {
        this.task = task;
    }

    @Override
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        ByteArray array = super.newByteArray(size, clearOnResize);
        trackAllocation(array.ramBytesUsed());
        return array;
    }

    @Override
    public IntArray newIntArray(long size, boolean clearOnResize) {
        IntArray array = super.newIntArray(size, clearOnResize);
        trackAllocation(array.ramBytesUsed());
        return array;
    }

    @Override
    public LongArray newLongArray(long size, boolean clearOnResize) {
        LongArray array = super.newLongArray(size, clearOnResize);
        trackAllocation(array.ramBytesUsed());
        return array;
    }

    @Override
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        DoubleArray array = super.newDoubleArray(size, clearOnResize);
        trackAllocation(array.ramBytesUsed());
        return array;
    }

    @Override
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        FloatArray array = super.newFloatArray(size, clearOnResize);
        trackAllocation(array.ramBytesUsed());
        return array;
    }

    @Override
    public <T> ObjectArray<T> newObjectArray(long size) {
        ObjectArray<T> array = super.newObjectArray(size);
        trackAllocation(array.ramBytesUsed());
        return array;
    }

    private void trackAllocation(long bytes) {
        if (task != null) {
            task.addApplicationManagedBytes(bytes);
        }
    }

}
