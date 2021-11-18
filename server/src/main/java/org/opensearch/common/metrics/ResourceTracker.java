/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metrics;

import com.sun.management.ThreadMXBean;
import org.opensearch.common.SuppressForbidden;

import java.lang.management.ManagementFactory;

/*
    Simple tracker for CPU consumption and memory allocated by the current thread
*/
@SuppressForbidden(reason = "ThreadMXBean enables tracking resource consumption by a thread. "
    + "It is platform dependent and i am not aware of an alternate mechanism to extract this info")
public class ResourceTracker {

    static ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    long startingAllocatedBytes;
    long startingCPUTime;
    long memoryAllocated;
    long cpuTime;

    public ResourceTracker() {
        reset();
    }

    /**
     * Takes current snapshot of resource usage by thread since the creation of this object
     */
    public void updateMetrics() {
        this.memoryAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId()) - startingAllocatedBytes;
        this.cpuTime = threadMXBean.getCurrentThreadCpuTime() - startingCPUTime;
    }

    /**
     * Wipes out local state and resets to a clean tracker
     */
    public void reset() {
        this.startingCPUTime = threadMXBean.getCurrentThreadCpuTime();
        this.startingAllocatedBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        this.memoryAllocated = 0;
        this.cpuTime = 0;
    }

    /**
     * Tracks CPU usage by the current thread
     * @return cpu time in nanoseconds
     */
    public long getCpuTime() {
        return this.cpuTime;
    }

    /**
     * Returns memory allocated by the thread between object creation and last update of metrics
     * @return memory allocated in bytes
     */
    public long getMemoryAllocated() {
        return this.memoryAllocated;
    }

    @Override
    public String toString() {
        return "resource_tracker[memoryAllocatedBytes=" + memoryAllocated + ",cpuTime=" + cpuTime + "]";
    }
}
