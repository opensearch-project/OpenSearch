/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.rest.action.cat;

import org.opensearch.client.node.ProtobufNodeClient;
import org.opensearch.rest.ProtobufBaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Base _cat API endpoint
*
* @opensearch.api
*/
public class ProtobufRestCatAction extends ProtobufBaseRestHandler {

    private static final String CAT = "=^.^=";
    private static final String CAT_NL = CAT + "\n";
    private final String HELP;

    public ProtobufRestCatAction(List<ProtobufAbstractCatAction> catActions) {
        StringBuilder sb = new StringBuilder();
        sb.append(CAT_NL);
        for (ProtobufAbstractCatAction catAction : catActions) {
            catAction.documentation(sb);
        }
        HELP = sb.toString();
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat"));
    }

    @Override
    public String getName() {
        return "cat_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final ProtobufNodeClient client) throws IOException {
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, HELP));
    }

}
