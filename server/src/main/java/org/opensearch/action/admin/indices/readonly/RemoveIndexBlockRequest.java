/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to remove a block from an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.10.0")
public class RemoveIndexBlockRequest extends AcknowledgedRequest<RemoveIndexBlockRequest> implements IndicesRequest.Replaceable {

    private final APIBlock block;
    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public RemoveIndexBlockRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        block = APIBlock.readFrom(in);
    }

    /**
     * Constructs a new request for the specified block and indices
     */
    public RemoveIndexBlockRequest(APIBlock block, String... indices) {
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
        if (block == APIBlock.SEARCH_ONLY) {
            validationException = addValidationError(
                "search_only block is managed by the scale API, use POST /{index}/_scale with {\"search_only\": false} to remove it",
                validationException
            );
        }
        return validationException;
    }

    /**
     * Returns the indices to remove the block from
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to remove the block from
     * @param indices the indices to remove the block from
     * @return the request itself
     */
    @Override
    public RemoveIndexBlockRequest indices(String... indices) {
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
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public RemoveIndexBlockRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Returns the block to be removed
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
