/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.tasks;

import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.tasks.proto.TaskResourceStatsProto;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedInputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Resource information about a currently running task.
* <p>
* Writeable TaskResourceStats objects are used to represent resource
* snapshot information about currently running task.
*
* @opensearch.internal
*/
public class ProtobufTaskResourceStats implements ProtobufWriteable {
    private final TaskResourceStatsProto.TaskResourceStats taskResourceStats;

    /**
     * Read from a stream.
    */
    public ProtobufTaskResourceStats(CodedInputStream in) throws IOException {
        this.taskResourceStats = TaskResourceStatsProto.TaskResourceStats.parseFrom(in.readByteArray());
    }

    public Map<String, TaskResourceStatsProto.TaskResourceStats.TaskResourceUsage> getResourceUsageInfo() {
        return this.taskResourceStats.getResourceUsageMap();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        this.taskResourceStats.writeTo(out);
    }
}
