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

package org.opensearch.action.support.single.instance;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Transport Request for an Index Shard Operation
 *
 * @opensearch.internal
 */
// TODO: This request and its associated transport action can be folded into UpdateRequest which is its only concrete production code
// implementation
public abstract class InstanceShardOperationRequest<Request extends InstanceShardOperationRequest<Request>> extends ActionRequest
    implements
        IndicesRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    protected String index;
    // null means its not set, allows to explicitly direct a request to a specific shard
    protected ShardId shardId = null;

    private String concreteIndex;

    protected InstanceShardOperationRequest() {}

    protected InstanceShardOperationRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(in);
        // Do a full read if no shard id is given (indicating that this instance isn't read as part of a BulkShardRequest or that `in` is of
        // an older version) and is in the format used by #writeTo.
        if (shardId == null) {
            index = in.readString();
            this.shardId = in.readOptionalWriteable(ShardId::new);
        } else {
            // We know a shard id so we read the format given by #writeThin
            this.shardId = shardId;
            if (in.readBoolean()) {
                index = in.readString();
            } else {
                index = shardId.getIndexName();
            }
        }
        timeout = in.readTimeValue();
        concreteIndex = in.readOptionalString();
    }

    public InstanceShardOperationRequest(String index) {
        this.index = index;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = ValidateActions.addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    public String index() {
        return index;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @SuppressWarnings("unchecked")
    public final Request index(String index) {
        this.index = index;
        return (Request) this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final Request timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public String concreteIndex() {
        return concreteIndex;
    }

    void concreteIndex(String concreteIndex) {
        this.concreteIndex = concreteIndex;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalWriteable(shardId);
        out.writeTimeValue(timeout);
        out.writeOptionalString(concreteIndex);
    }

    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (shardId != null && index.equals(shardId.getIndexName())) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(index);
        }
        out.writeTimeValue(timeout);
        out.writeOptionalString(concreteIndex);
    }
}
