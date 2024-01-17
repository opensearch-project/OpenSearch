/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Granular tests for the {@link TopQueriesResponse} class.
 */
public class TopQueriesResponseTests extends OpenSearchTestCase {

    /**
     * Check serialization and deserialization
     */
    public void testToXContent() throws Exception {
        TopQueries topQueries = QueryInsightsTestUtils.createTopQueries();
        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), 10);
        TopQueriesResponse deserializedResponse = roundTripResponse(response);
        assertEquals(response.toString(), deserializedResponse.toString());
    }

    /**
     * Serialize and deserialize a TopQueriesResponse.
     * @param response A response to serialize.
     * @return The deserialized, "round-tripped" response.
     */
    private static TopQueriesResponse roundTripResponse(TopQueriesResponse response) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new TopQueriesResponse(in);
            }
        }
    }
}
