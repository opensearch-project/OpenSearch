/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.rest;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for getting tiering status of a single index.
 * prepareRequest logic will be added in the implementation PR.
 */
public class RestGetTieringStatusAction extends BaseRestHandler {

    /** Constructs a new RestGetTieringStatusAction. */
    public RestGetTieringStatusAction() {}

    private static final String DETAILED_FLAG = "detailed";
    private static final String INDEX_NAME = "index";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/{index}/_tier"));
    }

    @Override
    public String getName() {
        return "get_index_tiering_status";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
