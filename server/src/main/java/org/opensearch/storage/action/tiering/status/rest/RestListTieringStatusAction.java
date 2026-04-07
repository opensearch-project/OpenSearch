/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.rest;

import org.opensearch.common.Table;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for listing tiering status of all indices.
 */
public class RestListTieringStatusAction extends AbstractCatAction {

    /** Constructs a new RestListTieringStatusAction. */
    public RestListTieringStatusAction() {}

    private static final String TARGET_TIER = "target";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_tier/all"));
    }

    @Override
    public String getName() {
        return "list_tiering_status";
    }

    @Override
    public RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void documentation(StringBuilder stringBuilder) {
        stringBuilder.append("/_tier/all\n");
    }

    @Override
    public Table getTableWithHeader(RestRequest request) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
