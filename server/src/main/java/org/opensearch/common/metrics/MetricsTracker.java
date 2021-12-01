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
    Simple tracker for CPU consumption and memory allocated by the current thread.
    In the current model, it is important for updateMetrics to be called from the thread that created the object

    This will evolve to support frequent updates and directly update task with resource usage
*/
@SuppressForbidden(reason = "ThreadMXBean enables tracking resource consumption by a thread. "
    + "It is platform dependent and i am not aware of an alternate mechanism to extract this info")
public class MetricsTracker {

    static ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    long startingAllocatedBytes;
    long startingCPUTime;
    long memoryAllocated;
    long cpuTime;

    /**
     * Takes current snapshot of resource usage by thread since the creation of this object
     */
    public void updateMetrics() {
        this.memoryAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId()) - startingAllocatedBytes;
        this.cpuTime = threadMXBean.getCurrentThreadCpuTime() - startingCPUTime;
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
        return "metrics_tracker[memoryAllocatedBytes=" + memoryAllocated + ",cpuTime=" + cpuTime + "]";
    }
}
