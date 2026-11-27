/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Builder for remove index block request
 *
 * @opensearch.api
 */
@PublicApi(since = "3.11.0")
public class RemoveIndexBlockRequestBuilder extends AcknowledgedRequestBuilder<
    RemoveIndexBlockRequest,
    RemoveIndexBlockResponse,
    RemoveIndexBlockRequestBuilder> {

    public RemoveIndexBlockRequestBuilder(OpenSearchClient client, RemoveIndexBlockAction action, APIBlock block, String... indices) {
        super(client, action, new RemoveIndexBlockRequest(block, indices));
    }

    /**
     * Sets the indices to remove the block from
     *
     * @param indices the indices to remove the block from
     * @return the request itself
     */
    public RemoveIndexBlockRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and indices wildcard expressions
     * @return the request itself
     */
    public RemoveIndexBlockRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
