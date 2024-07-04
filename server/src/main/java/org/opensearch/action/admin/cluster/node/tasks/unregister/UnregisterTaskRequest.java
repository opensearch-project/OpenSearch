/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.unregister;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;

import java.io.IOException;

@PublicApi(since = "1.0.0")
public class UnregisterTaskRequest extends ActionRequest {

    private Task task;

    public UnregisterTaskRequest(Task task) {
        this.task = task;
    }

    public UnregisterTaskRequest(StreamInput in) throws IOException {
        super(in);
    }

    public Task getTask() {
        return task;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
