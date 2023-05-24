/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.performanceanalyzer.listener;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import org.opensearch.performanceanalyzer.commons.OSMetricsGeneratorFactory;
import org.opensearch.performanceanalyzer.commons.collectors.OSMetricsCollector;
import org.opensearch.performanceanalyzer.commons.jvm.ThreadList;
import org.opensearch.performanceanalyzer.commons.metrics.AllMetrics;
import org.opensearch.performanceanalyzer.commons.metrics.ThreadIDUtil;
import org.opensearch.performanceanalyzer.commons.metrics_generator.CPUPagingActivityGenerator;
import org.opensearch.performanceanalyzer.commons.metrics_generator.SchedMetricsGenerator;
import org.opensearch.performanceanalyzer.commons.metrics_generator.linux.LinuxDiskIOMetricsGenerator;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIO;
import org.opensearch.tracing.TaskEventListener;

public class DiskStatsTaskEventListener implements TaskEventListener {
    private final boolean threadContentionEnabled;

    public DiskStatsTaskEventListener() {
        threadContentionEnabled = true;
    }

    @Override
    public void onStart(String operationName, String eventName, Thread t) {
        emitOtherMetrics("Start-PA-" + t.getName() + eventName, t.getId(), false);
    }

    @Override
    public void onEnd(String operationName, String eventName, Thread t) {
        emitOtherMetrics("End-PA-" + t.getName(), t.getId(), true);
    }

    private synchronized void emitOtherMetrics(String eventName, long jTid, boolean emit) {
        ThreadList.ThreadState threadState;
        long nativeThreadID = ThreadIDUtil.INSTANCE.getNativeThreadId(jTid);
        if (nativeThreadID == -1) {
            // TODO - make it async
            ThreadList.getNativeTidMap(threadContentionEnabled);
            nativeThreadID = ThreadIDUtil.INSTANCE.getNativeThreadId(jTid);

            if (nativeThreadID == -1) {
                return;
            }
            threadState = ThreadList.getThreadState(jTid);
        } else {
            threadState = ThreadList.getThreadState(jTid);
        }
        CPUPagingActivityGenerator threadCPUPagingActivityGenerator =
            OSMetricsGeneratorFactory.getInstance().getPagingActivityGenerator();
        threadCPUPagingActivityGenerator.addSample(String.valueOf(nativeThreadID));

        SchedMetricsGenerator schedMetricsGenerator =
            OSMetricsGeneratorFactory.getInstance().getSchedMetricsGenerator();
        schedMetricsGenerator.addSample(String.valueOf(nativeThreadID));

        ThreadDiskIO.addSample(String.valueOf(nativeThreadID));
        AttributesBuilder attrBuilder = Attributes.builder();

        attrBuilder.put("eventName", eventName);
        attrBuilder.put("nativeTid", nativeThreadID);

        if (emit) {
            LinuxDiskIOMetricsGenerator diskIOMetricsGenerator = ThreadDiskIO.getIOUtilization();

            if (diskIOMetricsGenerator.hasDiskIOMetrics(String.valueOf(nativeThreadID))) {
                attrBuilder.put(
                    AttributeKey.doubleKey("ReadThroughputBps"),
                    diskIOMetricsGenerator.getAvgReadThroughputBps(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("WriteThroughputBps"),
                    diskIOMetricsGenerator.getAvgWriteThroughputBps(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("TotalThroughputBps"),
                    diskIOMetricsGenerator.getAvgTotalThroughputBps(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("ReadSyscallRate"),
                    diskIOMetricsGenerator.getAvgReadSyscallRate(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("WriteSyscallRate"),
                    diskIOMetricsGenerator.getAvgWriteSyscallRate(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("TotalSyscallRate"),
                    diskIOMetricsGenerator.getAvgTotalSyscallRate(
                        String.valueOf(nativeThreadID)));

                attrBuilder.put(
                    AttributeKey.doubleKey("PageCacheReadThroughputBps"),
                    diskIOMetricsGenerator.getAvgPageCacheReadThroughputBps(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("PageCacheWriteThroughputBps"),
                    diskIOMetricsGenerator.getAvgPageCacheWriteThroughputBps(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey("PageCacheTotalThroughputBps"),
                    diskIOMetricsGenerator.getAvgPageCacheTotalThroughputBps(
                        String.valueOf(nativeThreadID)));
            }

            if (threadCPUPagingActivityGenerator.hasPagingActivity(
                String.valueOf(nativeThreadID))) {
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.CPU_UTILIZATION.toString()),
                    threadCPUPagingActivityGenerator.getCPUUtilization(
                        String.valueOf(nativeThreadID)));

                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.PAGING_MAJ_FLT_RATE.toString()),
                    threadCPUPagingActivityGenerator.getMajorFault(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.PAGING_MIN_FLT_RATE.toString()),
                    threadCPUPagingActivityGenerator.getMinorFault(
                        String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.PAGING_RSS.toString()),
                    threadCPUPagingActivityGenerator.getResidentSetSize(
                        String.valueOf(nativeThreadID)));
            }
            if (schedMetricsGenerator.hasSchedMetrics(String.valueOf(nativeThreadID))) {
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.SCHED_RUNTIME.toString()),
                    schedMetricsGenerator.getAvgRuntime(String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.SCHED_WAITTIME.toString()),
                    schedMetricsGenerator.getAvgWaittime(String.valueOf(nativeThreadID)));
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.SCHED_CTX_RATE.toString()),
                    schedMetricsGenerator.getContextSwitchRate(String.valueOf(nativeThreadID)));
            }
            if (threadState != null) {
                attrBuilder.put(
                    AttributeKey.stringKey(
                        OSMetricsCollector.MetaDataFields.threadName.toString()),
                    threadState.threadName);
                attrBuilder.put(
                    AttributeKey.doubleKey(AllMetrics.OSMetrics.HEAP_ALLOC_RATE.name()),
                    threadState.heapAllocRate);

                attrBuilder.put(
                    AttributeKey.longKey(AllMetrics.OSMetrics.THREAD_BLOCKED_EVENT.toString()),
                    threadState.blockedCount);
                attrBuilder.put(
                    AttributeKey.longKey(AllMetrics.OSMetrics.THREAD_WAITED_TIME.toString()),
                    threadState.blockedTime);
                attrBuilder.put(
                    AttributeKey.longKey(AllMetrics.OSMetrics.THREAD_WAITED_EVENT.toString()),
                    threadState.waitedCount);
                attrBuilder.put(
                    AttributeKey.longKey(AllMetrics.OSMetrics.THREAD_WAITED_TIME.toString()),
                    threadState.waitedTime);
            }
        }
        Attributes attr = attrBuilder.build();

        /**System.out.println(Span.current());
        attr.forEach(
            (k, v) -> {
                if (v instanceof Double) {
                    System.out.print(k + ":" + v + ", ");
                } else if (v instanceof Long) {
                    System.out.print("Long=" + k + ":" + v + ", ");

                } else {
                    System.out.print("ND=" + k + ":" + v + ", ");
                }
            });
        System.out.println("\nDone"); **/
        Span.current().addEvent(eventName, attrBuilder.build());
    }

    @Override
    public boolean isApplicable(String s, String s1) {
        return true;
    }
}
