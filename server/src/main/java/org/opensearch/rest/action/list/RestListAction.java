/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Base _list API endpoint
 *
 * @opensearch.api
 */
public class RestListAction extends BaseRestHandler {

    private static final String LIST = ":‑|";
    private static final String LIST_NL = LIST + "\n";
    private final String HELP;

    public RestListAction(List<AbstractListAction> listActions) {
        StringBuilder sb = new StringBuilder();
        sb.append(LIST_NL);
        for (AbstractListAction listAction : listActions) {
            listAction.documentation(sb);
        }
        HELP = sb.toString();
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_list"));
    }

    @Override
    public String getName() {
        return "list_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, HELP));
    }

}
