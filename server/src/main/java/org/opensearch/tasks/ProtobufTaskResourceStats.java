/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
*/

package org.opensearch.tasks;

import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.proto.TaskResourceStatsProto;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Resource information about a currently running task.
* <p>
* Writeable TaskResourceStats objects are used to represent resource
* snapshot information about currently running task.
*
* @opensearch.internal
*/
public class ProtobufTaskResourceStats implements ProtobufWriteable, ToXContentFragment {
    private final TaskResourceStatsProto.TaskResourceStats taskResourceStats;

    public ProtobufTaskResourceStats(Map<String, TaskResourceStatsProto.TaskResourceStats.TaskResourceUsage> resourceUsage) {
        this.taskResourceStats = TaskResourceStatsProto.TaskResourceStats.newBuilder().putAllResourceUsage(resourceUsage).build();
    }

    /**
     * Read from a stream.
    */
    public ProtobufTaskResourceStats(byte[] in) throws IOException {
        this.taskResourceStats = TaskResourceStatsProto.TaskResourceStats.parseFrom(in);
    }

    public Map<String, TaskResourceStatsProto.TaskResourceStats.TaskResourceUsage> getResourceUsageInfo() {
        return this.taskResourceStats.getResourceUsageMap();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.taskResourceStats.toByteArray());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Map<String, TaskResourceStatsProto.TaskResourceStats.TaskResourceUsage> resourceUsage = this.taskResourceStats
            .getResourceUsageMap();
        for (Map.Entry<String, TaskResourceStatsProto.TaskResourceStats.TaskResourceUsage> resourceUsageEntry : resourceUsage.entrySet()) {
            builder.startObject(resourceUsageEntry.getKey());
            if (resourceUsageEntry.getValue() != null) {
                builder.field("cpu_time_in_nanos", resourceUsageEntry.getValue().getCpuTimeInNanos());
                builder.field("memory_in_bytes", resourceUsageEntry.getValue().getMemoryInBytes());
            }
            builder.endObject();
        }
        return builder;
    }
}
