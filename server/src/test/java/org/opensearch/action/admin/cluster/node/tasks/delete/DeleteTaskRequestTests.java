/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.delete;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteTaskRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        DeleteTaskRequest request = new DeleteTaskRequest().setTaskId(new TaskId("node", 1L));

        DeleteTaskRequest copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new DeleteTaskRequest(in);
            }
        }

        assertEquals(request.getTaskId(), copy.getTaskId());
    }

    public void testNodeRequestCopiesTaskId() {
        DeleteTaskRequest request = new DeleteTaskRequest().setTaskId(new TaskId("node", 1L));

        DeleteTaskRequest nodeRequest = request.nodeRequest("coordinating-node", 2L);

        assertEquals(request.getTaskId(), nodeRequest.getTaskId());
    }
}
