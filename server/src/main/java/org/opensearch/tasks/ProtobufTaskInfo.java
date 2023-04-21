/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.tasks;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Information about a currently running task.
* <p>
* Tasks are used for communication with transport actions. As a result, they can contain callback
* references as well as mutable state. That makes it impractical to send tasks over transport channels
* and use in APIs. Instead, immutable and writeable ProtobufTaskInfo objects are used to represent
* snapshot information about currently running tasks.
*
* @opensearch.internal
*/
public final class ProtobufTaskInfo implements ProtobufWriteable {
    private final ProtobufTaskId taskId;

    private final String type;

    private final String action;

    private final String description;

    private final long startTime;

    private final long runningTimeNanos;

    private final ProtobufTask.Status status;

    private final boolean cancellable;

    private final boolean cancelled;

    private final ProtobufTaskId parentTaskId;

    private final Map<String, String> headers;

    private final ProtobufTaskResourceStats resourceStats;

    private ProtobufStreamInput protobufStreamInput;

    private ProtobufStreamOutput protobufStreamOutput;

    public ProtobufTaskInfo(
        ProtobufTaskId taskId,
        String type,
        String action,
        String description,
        ProtobufTask.Status status,
        long startTime,
        long runningTimeNanos,
        boolean cancellable,
        boolean cancelled,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers,
        ProtobufTaskResourceStats resourceStats
    ) {
        if (cancellable == false && cancelled == true) {
            throw new IllegalArgumentException("task cannot be cancelled");
        }
        this.taskId = taskId;
        this.type = type;
        this.action = action;
        this.description = description;
        this.status = status;
        this.startTime = startTime;
        this.runningTimeNanos = runningTimeNanos;
        this.cancellable = cancellable;
        this.cancelled = cancelled;
        this.parentTaskId = parentTaskId;
        this.headers = headers;
        this.resourceStats = resourceStats;
    }

    /**
     * Read from a stream.
    */
    @SuppressWarnings("unchecked")
    public ProtobufTaskInfo(CodedInputStream in) throws IOException {
        protobufStreamInput = new ProtobufStreamInput();
        taskId = ProtobufTaskId.readFromStream(in);
        type = in.readString();
        action = in.readString();
        description = protobufStreamInput.readOptionalString(in);
        //TODO: fix this
        status = null;
        startTime = in.readInt64();
        runningTimeNanos = in.readInt64();
        cancellable = in.readBool();
        if (protobufStreamInput.getVersion().onOrAfter(Version.V_2_0_0)) {
            cancelled = in.readBool();
        } else {
            cancelled = false;
        }
        if (cancellable == false && cancelled == true) {
            throw new IllegalArgumentException("task cannot be cancelled");
        }
        parentTaskId = ProtobufTaskId.readFromStream(in);
        headers = protobufStreamInput.readMap(CodedInputStream::readString, CodedInputStream::readString, in);
        if (protobufStreamInput.getVersion().onOrAfter(Version.V_2_1_0)) {
            resourceStats = protobufStreamInput.readOptionalWriteable(ProtobufTaskResourceStats::new, in);
        } else {
            resourceStats = null;
        }
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        protobufStreamOutput = new ProtobufStreamOutput();
        taskId.writeTo(out);
        out.writeString(1, type);
        out.writeString(2, action);
        out.writeString(3, description);
        //TODO: fix this
        // out.writeOptionalNamedWriteable(status);
        out.writeInt64(4, startTime);
        out.writeInt64(5, runningTimeNanos);
        out.writeBool(6, cancellable);
        if (protobufStreamOutput.getVersion().onOrAfter(Version.V_2_0_0)) {
            out.writeBool(7, cancelled);
        }
        parentTaskId.writeTo(out);
        protobufStreamOutput.writeMap(headers, CodedOutputStream::writeString, CodedOutputStream::writeString, out);
        if (protobufStreamOutput.getVersion().onOrAfter(Version.V_2_1_0)) {
            out.writeOptionalWriteable(resourceStats, out);
        }
    }

    public ProtobufTaskId getTaskId() {
        return taskId;
    }

    public long getId() {
        return taskId.getId();
    }

    public String getType() {
        return type;
    }

    public String getAction() {
        return action;
    }

    public String getDescription() {
        return description;
    }

    /**
     * The status of the running task. Only available if TaskInfos were build
    * with the detailed flag.
    */
    public ProtobufTask.Status getStatus() {
        return status;
    }

    /**
     * Returns the task start time
    */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the task running time
    */
    public long getRunningTimeNanos() {
        return runningTimeNanos;
    }

    /**
     * Returns true if the task supports cancellation
    */
    public boolean isCancellable() {
        return cancellable;
    }

    /**
     * Returns true if the task has been cancelled
    */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Returns the parent task id
    */
    public ProtobufTaskId getParentTaskId() {
        return parentTaskId;
    }

    /**
     * Returns the task headers
    */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Returns the task resource information
    */
    public ProtobufTaskResourceStats getResourceStats() {
        return resourceStats;
    }
}
