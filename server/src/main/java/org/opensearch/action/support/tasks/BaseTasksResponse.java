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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support.tasks;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.TaskId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.ExceptionsHelper.rethrowAndSuppress;

/**
 * Base class for responses of task-related operations
 *
 * @opensearch.internal
 */
public class BaseTasksResponse extends ActionResponse {
    protected static final String TASK_FAILURES = "task_failures";
    protected static final String NODE_FAILURES = "node_failures";

    private List<TaskOperationFailure> taskFailures;
    private List<OpenSearchException> nodeFailures;

    public BaseTasksResponse(List<TaskOperationFailure> taskFailures, List<? extends OpenSearchException> nodeFailures) {
        this.taskFailures = taskFailures == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(taskFailures));
        this.nodeFailures = nodeFailures == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(nodeFailures));
    }

    public BaseTasksResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        List<TaskOperationFailure> taskFailures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            taskFailures.add(new TaskOperationFailure(in));
        }
        size = in.readVInt();
        this.taskFailures = Collections.unmodifiableList(taskFailures);
        List<FailedNodeException> nodeFailures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodeFailures.add(new FailedNodeException(in));
        }
        this.nodeFailures = Collections.unmodifiableList(nodeFailures);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(taskFailures.size());
        for (TaskOperationFailure exp : taskFailures) {
            exp.writeTo(out);
        }
        out.writeVInt(nodeFailures.size());
        for (OpenSearchException exp : nodeFailures) {
            exp.writeTo(out);
        }
    }

    /**
     * The list of task failures exception.
     */
    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    /**
     * The list of node failures exception.
     */
    public List<OpenSearchException> getNodeFailures() {
        return nodeFailures;
    }

    /**
     * Rethrow task failures if there are any.
     */
    public void rethrowFailures(String operationName) {
        rethrowAndSuppress(
            Stream.concat(
                getNodeFailures().stream(),
                getTaskFailures().stream()
                    .map(
                        f -> new OpenSearchException(
                            "{} of [{}] failed",
                            f.getCause(),
                            operationName,
                            new TaskId(f.getNodeId(), f.getTaskId())
                        )
                    )
            ).collect(toList())
        );
    }

    protected void toXContentCommon(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (getTaskFailures() != null && getTaskFailures().size() > 0) {
            builder.startArray(TASK_FAILURES);
            for (TaskOperationFailure ex : getTaskFailures()) {
                builder.startObject();
                builder.value(ex);
                builder.endObject();
            }
            builder.endArray();
        }

        if (getNodeFailures() != null && getNodeFailures().size() > 0) {
            builder.startArray(NODE_FAILURES);
            for (OpenSearchException ex : getNodeFailures()) {
                builder.startObject();
                ex.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseTasksResponse response = (BaseTasksResponse) o;
        return taskFailures.equals(response.taskFailures) && nodeFailures.equals(response.nodeFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskFailures, nodeFailures);
    }
}
