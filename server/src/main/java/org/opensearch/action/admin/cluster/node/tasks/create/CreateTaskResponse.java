/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.create;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;

import java.io.IOException;

/**
 * Returns a new task.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CreateTaskResponse extends ActionResponse {

    private Task task;

    public CreateTaskResponse(Task task) {
        this.task = task;
    }

    public CreateTaskResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable((Writeable) task);
    }

    public Task getTask() {
        return task;
    }
}
