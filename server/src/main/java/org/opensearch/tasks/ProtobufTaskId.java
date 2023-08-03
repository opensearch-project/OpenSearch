/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
*/

package org.opensearch.tasks;

import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.tasks.proto.TaskIdProto;

import java.io.IOException;
import java.io.OutputStream;

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
        if (org.opensearch.core.common.Strings.hasLength(taskId) && "unset".equals(taskId) == false) {
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
    * {@linkplain byte[]} so we can return the {@link #EMPTY_TASK_ID} without allocating.
    */
    public ProtobufTaskId(byte[] in) throws IOException {
        this.taskId = TaskIdProto.TaskId.parseFrom(in);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.taskId.toByteArray());
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
