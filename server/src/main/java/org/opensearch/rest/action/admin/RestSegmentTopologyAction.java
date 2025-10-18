/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin;

import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST action to analyze segment topology for indices
 *
 * @opensearch.api
 */
@org.opensearch.common.annotation.PublicApi(since = "3.3.0")
public class RestSegmentTopologyAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/segment_topology"), new Route(GET, "/_cat/segment_topology/{index}"));
    }

    @Override
    public String getName() {
        return "segment_topology_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index", "_all"));
        IndicesSegmentsRequest segmentsRequest = new IndicesSegmentsRequest(indices);
        segmentsRequest.verbose(true);

        return channel -> client.admin().indices().segments(segmentsRequest, new RestToXContentListener<IndicesSegmentResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndicesSegmentResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("message", "Segment topology analysis endpoint is available");
                builder.field("note", "This endpoint provides basic segment topology information");
                builder.field("indices_analyzed", response.getIndices().size());

                // Simple summary for now
                int totalShards = 0;
                for (var indexSegments : response.getIndices().values()) {
                    totalShards += indexSegments.getShards().size();
                }
                builder.field("total_shards", totalShards);

                builder.endObject();

                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
