/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.PitUsageRequest;
import org.opensearch.action.search.PitUsageResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for retrieving usage information of point in time searches
 */
public class RestPitUsageAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "pit_usage_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String allPitIdsQualifier = "_all";
        final PitUsageRequest pitUsageRequest;
        if (request.path().contains(allPitIdsQualifier)) {
            pitUsageRequest = new PitUsageRequest(allPitIdsQualifier);
        } else {
            pitUsageRequest = new PitUsageRequest();
            request.withContentOrSourceParamParserOrNull((xContentParser -> {
                if (xContentParser != null) {
                    try {
                        pitUsageRequest.fromXContent(xContentParser);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to parse request body", e);
                    }
                }
            }));
        }
        return channel -> client.pitUsage(pitUsageRequest, new RestToXContentListener<PitUsageResponse>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(GET, "/_search/point_in_time/_usage"), new Route(GET, "/_search/point_in_time/_all/_usage"))
        );
    }
}
