/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.tasks.ProtobufTaskId;
import org.opensearch.tasks.ProtobufTaskAwareRequest;
import org.opensearch.tasks.TaskId;

import java.io.IOException;

/**
 * A transport request with Protobuf serialization.
*
* @opensearch.internal
*/
public abstract class ProtobufTransportRequest extends ProtobufTransportMessage implements ProtobufTaskAwareRequest {

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
    */
    private ProtobufTaskId parentTaskId = ProtobufTaskId.EMPTY_TASK_ID;

    public ProtobufTransportRequest() {}

    public ProtobufTransportRequest(CodedInputStream in) throws IOException {
        parentTaskId = ProtobufTaskId.readFromStream(in);
    }

    /**
     * Set a reference to task that created this request.
    */
    @Override
    public void setParentTask(ProtobufTaskId taskId) {
        this.parentTaskId = taskId;
    }

    /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
    */
    @Override
    public ProtobufTaskId getParentTask() {
        return parentTaskId;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        parentTaskId.writeTo(out);
    }
}
