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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Granular tests for the {@link TopQueriesResponse} class.
 */
public class TopQueriesResponseTests extends OpenSearchTestCase {

    /**
     * Check serialization and deserialization
     */
    public void testSerialize() throws Exception {
        TopQueries topQueries = QueryInsightsTestUtils.createRandomTopQueries();
        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), 10, MetricType.LATENCY);
        TopQueriesResponse deserializedResponse = roundTripResponse(response);
        assertEquals(response.toString(), deserializedResponse.toString());
    }

    public void testToXContent() throws IOException {
        char[] expectedXcontent =
            "{\"top_queries\":[{\"timestamp\":1706574180000,\"node_id\":\"node_for_top_queries_test\",\"search_type\":\"query_then_fetch\",\"latency\":1}]}"
                .toCharArray();
        TopQueries topQueries = QueryInsightsTestUtils.createFixedTopQueries();
        ClusterName clusterName = new ClusterName("test-cluster");
        TopQueriesResponse response = new TopQueriesResponse(clusterName, List.of(topQueries), new ArrayList<>(), 10, MetricType.LATENCY);
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        char[] xContent = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString().toCharArray();
        Arrays.sort(expectedXcontent);
        Arrays.sort(xContent);

        assertEquals(Arrays.hashCode(expectedXcontent), Arrays.hashCode(xContent));
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
