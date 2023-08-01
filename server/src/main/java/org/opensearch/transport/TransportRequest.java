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

package org.opensearch.transport;

import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.server.proto.TransportRequestProto;
import org.opensearch.server.proto.TransportRequestProto.TransportReq;
import org.opensearch.tasks.ProtobufTaskAwareRequest;
import org.opensearch.tasks.ProtobufTaskId;
import org.opensearch.tasks.TaskAwareRequest;
import org.opensearch.tasks.TaskId;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A transport request
 *
 * @opensearch.internal
 */
public abstract class TransportRequest extends TransportMessage implements TaskAwareRequest, ProtobufTaskAwareRequest {

    private TransportRequestProto.TransportReq transportReq;
    /**
     * Empty transport request
     *
     * @opensearch.internal
     */
    public static class Empty extends TransportRequest {
        public static final Empty INSTANCE = new Empty();

        public Empty() {}

        public Empty(StreamInput in) throws IOException {
            super(in);
        }

        public Empty(byte[] in) throws IOException {
            super(in);
        }
    }

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
     */
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
    */
    private ProtobufTaskId protobufParentTaskId = ProtobufTaskId.EMPTY_TASK_ID;

    public TransportRequest() {}

    public TransportRequest(ProtobufClusterStateRequest clusterStateRequest) {
        this.transportReq = TransportReq.newBuilder().setClusterStateRequest(clusterStateRequest.request()).build();
    }

    public TransportRequest(StreamInput in) throws IOException {
        parentTaskId = TaskId.readFromStream(in);
    }

    public TransportRequest(byte[] in) throws IOException {
        protobufParentTaskId = new ProtobufTaskId(in);
    }

    /**
     * Set a reference to task that created this request.
     */
    @Override
    public void setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
    }

    /**
     * Set a reference to task that created this request.
    */
    @Override
    public void setProtobufParentTask(ProtobufTaskId taskId) {
        this.protobufParentTaskId = taskId;
    }

    /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     */
    @Override
    public TaskId getParentTask() {
        return parentTaskId;
    }

     /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
    */
    @Override
    public ProtobufTaskId getProtobufParentTask() {
        return protobufParentTaskId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        parentTaskId.writeTo(out);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        protobufParentTaskId.writeTo(out);
    }

    public TransportReq transportReq() {
        return transportReq;
    }
}
