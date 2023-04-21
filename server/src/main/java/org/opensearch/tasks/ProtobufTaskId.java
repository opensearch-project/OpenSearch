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
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.tasks.proto.TaskIdProto;

import java.io.IOException;

/**
 * Task id that consists of node id and id of the task on the node
*
* @opensearch.internal
*/
public final class ProtobufTaskId implements ProtobufWriteable {

    public static final ProtobufTaskId EMPTY_TASK_ID = new ProtobufTaskId();

    private final TaskIdProto.TaskId taskId;

    public ProtobufTaskId(String nodeId, long id) {
        this.taskId = TaskIdProto.TaskId.newBuilder().setNodeId(nodeId).setId(id).build();
    }

    /**
     * Builds {@link #EMPTY_TASK_ID}.
    */
    private ProtobufTaskId() {
        this.taskId = TaskIdProto.TaskId.newBuilder().setNodeId("").setId(-1L).build();
    }

    public ProtobufTaskId(String taskId) {
        if (Strings.hasLength(taskId) && "unset".equals(taskId) == false) {
            String[] s = Strings.split(taskId, ":");
            if (s == null || s.length != 2) {
                throw new IllegalArgumentException("malformed task id " + taskId);
            }
            String nodeId = s[0];
            try {
                long id = Long.parseLong(s[1]);
                this.taskId = TaskIdProto.TaskId.newBuilder().setNodeId(nodeId).setId(id).build();
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("malformed task id " + taskId, ex);
            }
        } else {
            this.taskId = EMPTY_TASK_ID.taskId;
        }
    }

    /**
     * Read a {@linkplain ProtobufTaskId} from a stream. {@linkplain ProtobufTaskId} has this rather than the usual constructor that takes a
    * {@linkplain CodedInputStream} so we can return the {@link #EMPTY_TASK_ID} without allocating.
    */
    public static ProtobufTaskId readFromStream(CodedInputStream in) throws IOException {
        String nodeId = in.readString();
        if (nodeId.isEmpty()) {
            /*
            * The only TaskId allowed to have the empty string as its nodeId is the EMPTY_TASK_ID and there is only ever one of it and it
            * never writes its taskId to save bytes on the wire because it is by far the most common TaskId.
            */
            return EMPTY_TASK_ID;
        }
        return new ProtobufTaskId(nodeId, in.readInt64());
    }

    @Override
    public void writeTo(com.google.protobuf.CodedOutputStream out) throws IOException {
        this.taskId.writeTo(out);
    }

    public String getNodeId() {
        return this.taskId.getNodeId();
    }

    public long getId() {
        return this.taskId.getId();
    }

    public boolean isSet() {
        return this.taskId.getId() != -1L;
    }
}
