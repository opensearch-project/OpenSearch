/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.rest;

import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.storage.action.tiering.status.GetTieringStatusAction;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for getting tiering status of a single index.
 * prepareRequest logic will be added in the implementation PR.
 */
public class RestGetTieringStatusAction extends BaseRestHandler {

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
        String[] indices = Strings.splitStringByCommaToArray(request.param(INDEX_NAME));
        Boolean isDetailedFlagEnabled = Boolean.parseBoolean(request.param(DETAILED_FLAG));

        if (indices.length != 1) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "request should contain single index"));
        }

        final GetTieringStatusRequest getTieringStatusRequest = new GetTieringStatusRequest();
        getTieringStatusRequest.setIndex(indices[0]);
        getTieringStatusRequest.setDetailedFlagEnabled(isDetailedFlagEnabled);
        return channel -> client.admin()
            .cluster()
            .execute(GetTieringStatusAction.INSTANCE, getTieringStatusRequest, new RestToXContentListener<>(channel));
    }
}
