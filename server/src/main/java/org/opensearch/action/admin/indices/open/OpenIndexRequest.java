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

package org.opensearch.action.admin.indices.open;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to open an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class OpenIndexRequest extends AcknowledgedRequest<OpenIndexRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, false, true);
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;
    private boolean shouldStoreResult;

    public OpenIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
    }

    public OpenIndexRequest() {}

    /**
     * Constructs a new open index request for the specified index.
     */
    public OpenIndexRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * The indices to be opened
     * @return the indices to be opened
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be opened
     * @param indices the indices to be opened
     * @return the request itself
     */
    @Override
    public OpenIndexRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the current behaviour when it comes to index names and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public OpenIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that should be active for indices opening to return.
     * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
     * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active before returning.
     * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Indices opening will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link OpenIndexResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public OpenIndexRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public OpenIndexRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * Should this task store its result after it has finished?
     */
    public void setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        waitForActiveShards.writeTo(out);
    }

    @Override
    public String toString() {
        return "open indices " + Arrays.toString(indices());
    }

    @Override
    public String getDescription() {
        return this.toString();
    }
}
