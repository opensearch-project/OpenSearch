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

package org.opensearch.action.admin.indices.flush;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.client.IndicesAdminClient;
import org.opensearch.transport.client.Requests;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A flush request to flush one or more indices. The flush process of an index basically frees memory from the index
 * by flushing data to the index storage and clearing the internal transaction log. By default, OpenSearch uses
 * memory heuristics in order to automatically trigger flush operations as required in order to clear memory.
 * <p>
 * Best created with {@link Requests#flushRequest(String...)}.
 *
 * @see Requests#flushRequest(String...)
 * @see IndicesAdminClient#flush(FlushRequest)
 * @see FlushResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FlushRequest extends BroadcastRequest<FlushRequest> {

    private boolean force = false;
    private boolean waitIfOngoing = true;

    /**
     * Constructs a new flush request against one or more indices. If nothing is provided, all indices will
     * be flushed.
     */
    public FlushRequest(String... indices) {
        super(indices);
    }

    public FlushRequest(StreamInput in) throws IOException {
        super(in);
        force = in.readBoolean();
        waitIfOngoing = in.readBoolean();
    }

    /**
     * Returns {@code true} iff a flush should block
     * if a another flush operation is already running. Otherwise {@code false}
     */
    public boolean waitIfOngoing() {
        return this.waitIfOngoing;
    }

    /**
     * if set to {@code true} the flush will block
     * if a another flush operation is already running until the flush can be performed.
     * The default is <code>true</code>
     */
    public FlushRequest waitIfOngoing(boolean waitIfOngoing) {
        this.waitIfOngoing = waitIfOngoing;
        return this;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public boolean force() {
        return force;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public FlushRequest force(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationError = super.validate();
        if (force && waitIfOngoing == false) {
            validationError = addValidationError("wait_if_ongoing must be true for a force flush", validationError);
        }
        return validationError;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(force);
        out.writeBoolean(waitIfOngoing);
    }

    @Override
    public String toString() {
        return "FlushRequest{" + "waitIfOngoing=" + waitIfOngoing + ", force=" + force + "}";
    }
}
