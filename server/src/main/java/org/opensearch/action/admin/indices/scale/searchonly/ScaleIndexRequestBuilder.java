/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * A builder for constructing {@link ScaleIndexRequest} objects to perform search-only scale operations.
 * <p>
 * This builder simplifies the creation of requests to scale indices up or down for search-only mode.
 * It provides methods to configure the scaling direction and follows the builder pattern to allow
 * method chaining for constructing requests.
 * <p>
 * The builder is part of the public API since OpenSearch 3.0.0.
 */
@PublicApi(since = "3.0.0")
public class ScaleIndexRequestBuilder extends ActionRequestBuilder<ScaleIndexRequest, AcknowledgedResponse> {

    /**
     * Constructs a new builder for scaling an index.
     * <p>
     * By default, the operation is a scale-up operation (from search-only to normal mode).
     *
     * @param client the client to use for executing the request
     * @param index  the name of the index to scale
     */
    public ScaleIndexRequestBuilder(OpenSearchClient client, String index) {
        this(client, false, index);
    }

    /**
     * Constructs a new builder for scaling an index, allowing explicit direction specification.
     *
     * @param client    the client to use for executing the request
     * @param scaleDown true for scaling down to search-only mode, false for scaling up to normal mode
     * @param index     the name of the index to scale
     */
    public ScaleIndexRequestBuilder(OpenSearchClient client, boolean scaleDown, String index) {
        super(client, ScaleIndexAction.INSTANCE, new ScaleIndexRequest(index, scaleDown));
    }

    /**
     * Sets the scale direction (up/down).
     * <p>
     * This method configures whether the request will:
     * <ul>
     *   <li>Scale down an index to search-only mode (removing write capability but preserving search), or</li>
     *   <li>Scale up an index from search-only mode back to full read-write operation</li>
     * </ul>
     *
     * @param searchOnly true if scaling down to search-only mode, false if scaling up to normal operation
     * @return this builder (for method chaining)
     */
    public ScaleIndexRequestBuilder setSearchOnly(boolean searchOnly) {
        request.scaleDown(searchOnly);
        return this;
    }
}
