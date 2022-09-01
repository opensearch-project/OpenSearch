/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.routing.WRRWeights;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;

public class WeightedRoundRobinRoutingMetadataTests extends AbstractXContentTestCase<WeightedRoundRobinRoutingMetadata> {
    @Override
    protected WeightedRoundRobinRoutingMetadata createTestInstance() {
        Map<String, Object> weights = Map.of("a", "1", "b", "1", "c", "0");
        WRRWeights wrrWeights = new WRRWeights("zone", weights);
        WeightedRoundRobinRoutingMetadata wrrMetadata = new WeightedRoundRobinRoutingMetadata(wrrWeights);
        return wrrMetadata;
    }

    @Override
    protected WeightedRoundRobinRoutingMetadata doParseInstance(XContentParser parser) throws IOException {
        return WeightedRoundRobinRoutingMetadata.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

}
