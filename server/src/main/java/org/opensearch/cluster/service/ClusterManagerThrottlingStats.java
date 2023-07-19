/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains stats of Cluster Manager Task Throttling.
 * It stores the total cumulative count of throttled tasks per task type.
 */
public class ClusterManagerThrottlingStats implements ClusterManagerTaskThrottlerListener, Writeable, ToXContentFragment {

    private Map<String, CounterMetric> throttledTasksCount;

    public ClusterManagerThrottlingStats() {
        throttledTasksCount = new ConcurrentHashMap<>();
    }

    private void incrementThrottlingCount(String type, final int counts) {
        throttledTasksCount.computeIfAbsent(type, k -> new CounterMetric()).inc(counts);
    }

    public long getThrottlingCount(String type) {
        return throttledTasksCount.get(type) == null ? 0 : throttledTasksCount.get(type).count();
    }

    public long getTotalThrottledTaskCount() {
        CounterMetric totalCount = new CounterMetric();
        throttledTasksCount.forEach((aClass, counterMetric) -> { totalCount.inc(counterMetric.count()); });
        return totalCount.count();
    }

    @Override
    public void onThrottle(String type, int counts) {
        incrementThrottlingCount(type, counts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(throttledTasksCount.size());
        for (Map.Entry<String, CounterMetric> entry : throttledTasksCount.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt((int) entry.getValue().count());
        }
    }

    public ClusterManagerThrottlingStats(StreamInput in) throws IOException {
        int throttledTaskEntries = in.readVInt();
        throttledTasksCount = new ConcurrentHashMap<>();
        for (int i = 0; i < throttledTaskEntries; i++) {
            String taskType = in.readString();
            int throttledTaskCount = in.readVInt();
            onThrottle(taskType, throttledTaskCount);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("cluster_manager_throttling");
        builder.startObject("stats");
        builder.field("total_throttled_tasks", getTotalThrottledTaskCount());
        builder.startObject("throttled_tasks_per_task_type");
        for (Map.Entry<String, CounterMetric> entry : throttledTasksCount.entrySet()) {
            builder.field(entry.getKey(), entry.getValue().count());
        }
        builder.endObject();
        builder.endObject();
        return builder.endObject();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            ClusterManagerThrottlingStats that = (ClusterManagerThrottlingStats) o;

            if (this.throttledTasksCount.size() == that.throttledTasksCount.size()) {
                for (Map.Entry<String, CounterMetric> entry : this.throttledTasksCount.entrySet()) {
                    if (that.throttledTasksCount.get(entry.getKey()).count() != entry.getValue().count()) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        Map<String, Long> countMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, CounterMetric> entry : this.throttledTasksCount.entrySet()) {
            countMap.put(entry.getKey(), entry.getValue().count());
        }
        return countMap.hashCode();
    }
}
