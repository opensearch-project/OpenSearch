/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WeightedRoutingMetadataTests extends AbstractXContentTestCase<WeightedRoutingMetadata> {
    @Override
    protected WeightedRoutingMetadata createTestInstance() {
        Map<String, Double> weights1 = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights1);
        Map<String, Double> weights2 = Map.of("a", 1.0, "b", 0.0, "c", 0.0);
        WeightedRouting weightedRouting2 = new WeightedRouting("rack", weights2);

        List<WeightedRouting> weightedRoutings = new ArrayList<>();
        weightedRoutings.add(weightedRouting);
        weightedRoutings.add(weightedRouting2);

        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRoutings);
        return weightedRoutingMetadata;
    }

    @Override
    protected WeightedRoutingMetadata doParseInstance(XContentParser parser) throws IOException {
        return WeightedRoutingMetadata.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
