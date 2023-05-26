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

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to add a block to an index.
 *
 * @opensearch.internal
 */
public class AddIndexBlockRequest extends AcknowledgedRequest<AddIndexBlockRequest> implements IndicesRequest.Replaceable {

    private final APIBlock block;
    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public AddIndexBlockRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        block = APIBlock.readFrom(in);
    }

    /**
     * Constructs a new request for the specified block and indices
     */
    public AddIndexBlockRequest(APIBlock block, String... indices) {
        this.block = Objects.requireNonNull(block);
        this.indices = Objects.requireNonNull(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (block == APIBlock.READ_ONLY_ALLOW_DELETE) {
            validationException = addValidationError("read_only_allow_delete block is for internal use only", validationException);
        }
        return validationException;
    }

    /**
     * Returns the indices to be blocked
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be blocked
     * @param indices the indices to be blocked
     * @return the request itself
     */
    @Override
    public AddIndexBlockRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal wild wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public AddIndexBlockRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Returns the block to be added
     */
    public APIBlock getBlock() {
        return block;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        block.writeTo(out);
    }
}
