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

package org.opensearch.action.admin.cluster.node.tasks.get;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.TaskResult;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Returns the list of tasks currently running on the nodes
 *
 * @opensearch.internal
 */
public class GetTaskResponse extends ActionResponse implements ToXContentObject {

    private final TaskResult task;

    public GetTaskResponse(TaskResult task) {
        this.task = requireNonNull(task, "task is required");
    }

    public GetTaskResponse(StreamInput in) throws IOException {
        super(in);
        task = in.readOptionalWriteable(TaskResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(task);
    }

    /**
     * Get the actual result of the fetch.
     */
    public TaskResult getTask() {
        return task;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        task.innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }
}
