/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Base REST handler for tiering operations.
 * prepareRequest and validateIndices will be added in the implementation PR.
 */
public abstract class RestBaseTierAction extends BaseRestHandler {

    /** The target tier for this action. */
    protected final String targetTier;

    /**
     * Constructs a RestBaseTierAction for the given target tier.
     * @param targetTier the target tier
     */
    protected RestBaseTierAction(String targetTier) {
        this.targetTier = targetTier;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/{index}/_tier/" + targetTier));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
