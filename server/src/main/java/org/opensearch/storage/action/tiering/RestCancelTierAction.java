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
 * REST handler for cancelling ongoing tiering operations.
 * This handler provides an endpoint to cancel migrations that may be stuck
 * in RUNNING_SHARD_RELOCATION state, allowing manual recovery.
 *
 * validateIndices and full prepareRequest logic will be added in the implementation PR.
 */
public class RestCancelTierAction extends BaseRestHandler {

    /** Constructs a new RestCancelTierAction. */
    public RestCancelTierAction() {}

    @Override
    public String getName() {
        return "cancel_tier_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_tier/_cancel/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
