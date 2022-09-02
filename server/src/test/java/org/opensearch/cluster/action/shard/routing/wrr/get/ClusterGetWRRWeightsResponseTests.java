/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.action.shard.routing.wrr.get;

import org.opensearch.action.admin.cluster.shards.routing.wrr.get.ClusterGetWRRWeightsResponse;
import org.opensearch.cluster.routing.WRRWeights;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;

public class ClusterGetWRRWeightsResponseTests extends AbstractXContentTestCase<ClusterGetWRRWeightsResponse> {
    @Override
    protected ClusterGetWRRWeightsResponse createTestInstance() {
        Map<String, Object> weights = Map.of("zone_A", "1", "zone_B", "0", "zone_C", "1");
        WRRWeights wrrWeights = new WRRWeights("", weights);
        ClusterGetWRRWeightsResponse response = new ClusterGetWRRWeightsResponse("1", wrrWeights);
        return response;
    }

    @Override
    protected ClusterGetWRRWeightsResponse doParseInstance(XContentParser parser) throws IOException {
        return ClusterGetWRRWeightsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

}
