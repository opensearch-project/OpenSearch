/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.action.shard.routing.weighted.get;

import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;

public class ClusterGetWeightedRoutingResponseTests extends AbstractXContentTestCase<ClusterGetWeightedRoutingResponse> {
    @Override
    protected ClusterGetWeightedRoutingResponse createTestInstance() {
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 0.0, "zone_C", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("", weights);
        ClusterGetWeightedRoutingResponse response = new ClusterGetWeightedRoutingResponse(weightedRouting, true, 0);
        return response;
    }

    @Override
    protected ClusterGetWeightedRoutingResponse doParseInstance(XContentParser parser) throws IOException {
        return ClusterGetWeightedRoutingResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

}
