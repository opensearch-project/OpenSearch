/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client.tasks;

import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * client side counterpart of server side
 * {@link org.opensearch.action.TaskOperationFailure}
 */
public class TaskOperationFailure {

    private final String nodeId;
    private final long taskId;
    private final OpenSearchException reason;
    private final String status;

    public TaskOperationFailure(String nodeId, long taskId, String status, OpenSearchException reason) {
        this.nodeId = nodeId;
        this.taskId = taskId;
        this.status = status;
        this.reason = reason;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getTaskId() {
        return taskId;
    }

    public OpenSearchException getReason() {
        return reason;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskOperationFailure)) return false;
        TaskOperationFailure that = (TaskOperationFailure) o;
        return getTaskId() == that.getTaskId()
            && Objects.equals(getNodeId(), that.getNodeId())
            && Objects.equals(getReason(), that.getReason())
            && Objects.equals(getStatus(), that.getStatus());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getTaskId(), getReason(), getStatus());
    }

    @Override
    public String toString() {
        return "TaskOperationFailure{"
            + "nodeId='"
            + nodeId
            + '\''
            + ", taskId="
            + taskId
            + ", reason="
            + reason
            + ", status='"
            + status
            + '\''
            + '}';
    }

    public static TaskOperationFailure fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<TaskOperationFailure, Void> PARSER = new ConstructingObjectParser<>(
        "task_info",
        true,
        constructorObjects -> {
            int i = 0;
            String nodeId = (String) constructorObjects[i++];
            long taskId = (long) constructorObjects[i++];
            String status = (String) constructorObjects[i++];
            OpenSearchException reason = (OpenSearchException) constructorObjects[i];
            return new TaskOperationFailure(nodeId, taskId, status, reason);
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("node_id"));
        PARSER.declareLong(constructorArg(), new ParseField("task_id"));
        PARSER.declareString(constructorArg(), new ParseField("status"));
        PARSER.declareObject(constructorArg(), (parser, c) -> OpenSearchException.fromXContent(parser), new ParseField("reason"));
    }
}
