/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.tasks;

import org.opensearch.client.Validatable;

import java.util.Objects;

public class DeleteTaskRequest implements Validatable {
    private final String nodeId;
    private final long taskId;

    public DeleteTaskRequest(String nodeId, long taskId) {
        this.nodeId = nodeId;
        this.taskId = taskId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, taskId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DeleteTaskRequest other = (DeleteTaskRequest) obj;
        return Objects.equals(nodeId, other.nodeId) && taskId == other.taskId;
    }
}
