/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Tests for {@link TopQueries}.
 */
public class TopQueriesTests extends OpenSearchTestCase {

    public void testTopQueries() throws IOException {
        TopQueries topQueries = createTopQueries();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            topQueries.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TopQueries readTopQueries = new TopQueries(in);
                assertExpected(topQueries, readTopQueries);
            }
        }
    }

    /**
     * checks all properties that are expected to be unchanged.
     */
    private void assertExpected(TopQueries topQueries, TopQueries readTopQueries) throws IOException {
        for (int i = 0; i < topQueries.getLatencyRecords().size(); i ++) {
            compareJson(topQueries.getLatencyRecords().get(i), readTopQueries.getLatencyRecords().get(i));
        }
    }

    private void compareJson(ToXContent param1, ToXContent param2) throws IOException {
        if (param1 == null || param2 == null) {
            assertNull(param1);
            assertNull(param2);
            return;
        }

        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder param1Builder = jsonBuilder();
        param1.toXContent(param1Builder, params);

        XContentBuilder param2Builder = jsonBuilder();
        param2.toXContent(param2Builder, params);

        assertEquals(param1Builder.toString(), param2Builder.toString());
    }

    private static TopQueries createTopQueries() {
        DiscoveryNode node = new DiscoveryNode(
            "node_for_top_queries_test",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );

        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("userId", "user1");

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        List<SearchQueryLatencyRecord> records = new ArrayList<>();
        records.add(new SearchQueryLatencyRecord(
            randomLong(),
            SearchType.QUERY_THEN_FETCH,
            "{\"size\":20}",
            randomInt(),
            randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(5, 10)),
            propertyMap,
            phaseLatencyMap
        ));

        return new TopQueries(node, records);
    }
}
