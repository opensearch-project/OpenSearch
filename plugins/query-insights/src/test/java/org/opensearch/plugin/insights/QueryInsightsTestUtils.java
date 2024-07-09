/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.util.Maps;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueries;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.OpenSearchTestCase.buildNewFakeTransportAddress;
import static org.opensearch.test.OpenSearchTestCase.random;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLengthBetween;
import static org.opensearch.test.OpenSearchTestCase.randomArray;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.OpenSearchTestCase.randomLongBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

final public class QueryInsightsTestUtils {

    public QueryInsightsTestUtils() {}

    public static List<SearchQueryRecord> generateQueryInsightRecords(int count) {
        return generateQueryInsightRecords(count, count, System.currentTimeMillis(), 0);
    }

    /**
     * Creates a List of random Query Insight Records for testing purpose
     */
    public static List<SearchQueryRecord> generateQueryInsightRecords(int lower, int upper, long startTimeStamp, long interval) {
        List<SearchQueryRecord> records = new ArrayList<>();
        int countOfRecords = randomIntBetween(lower, upper);
        long timestamp = startTimeStamp;
        for (int i = 0; i < countOfRecords; ++i) {
            Map<MetricType, Number> measurements = Map.of(
                MetricType.LATENCY,
                randomLongBetween(1000, 10000),
                MetricType.CPU,
                randomLongBetween(1000, 10000),
                MetricType.MEMORY,
                randomLongBetween(1000, 10000)
            );

            Map<String, Long> phaseLatencyMap = new HashMap<>();
            int countOfPhases = randomIntBetween(2, 5);
            for (int j = 0; j < countOfPhases; ++j) {
                phaseLatencyMap.put(randomAlphaOfLengthBetween(5, 10), randomLong());
            }
            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, SearchType.QUERY_THEN_FETCH.toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, "{\"size\":20}");
            attributes.put(Attribute.TOTAL_SHARDS, randomIntBetween(1, 100));
            attributes.put(Attribute.INDICES, randomArray(1, 3, Object[]::new, () -> randomAlphaOfLengthBetween(5, 10)));
            attributes.put(Attribute.PHASE_LATENCY_MAP, phaseLatencyMap);
            attributes.put(
                Attribute.TASK_RESOURCE_USAGES,
                List.of(
                    new TaskResourceInfo(
                        randomAlphaOfLengthBetween(5, 10),
                        randomLongBetween(1, 1000),
                        randomLongBetween(1, 1000),
                        randomAlphaOfLengthBetween(5, 10),
                        new TaskResourceUsage(randomLongBetween(1, 1000), randomLongBetween(1, 1000))
                    ),
                    new TaskResourceInfo(
                        randomAlphaOfLengthBetween(5, 10),
                        randomLongBetween(1, 1000),
                        randomLongBetween(1, 1000),
                        randomAlphaOfLengthBetween(5, 10),
                        new TaskResourceUsage(randomLongBetween(1, 1000), randomLongBetween(1, 1000))
                    )
                )
            );

            records.add(new SearchQueryRecord(timestamp, measurements, attributes));
            timestamp += interval;
        }
        return records;
    }

    public static TopQueries createRandomTopQueries() {
        DiscoveryNode node = new DiscoveryNode(
            "node_for_top_queries_test",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        List<SearchQueryRecord> records = generateQueryInsightRecords(10);

        return new TopQueries(node, records);
    }

    public static TopQueries createFixedTopQueries() {
        DiscoveryNode node = new DiscoveryNode(
            "node_for_top_queries_test",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        List<SearchQueryRecord> records = new ArrayList<>();
        records.add(createFixedSearchQueryRecord());

        return new TopQueries(node, records);
    }

    public static SearchQueryRecord createFixedSearchQueryRecord() {
        long timestamp = 1706574180000L;
        Map<MetricType, Number> measurements = Map.of(MetricType.LATENCY, 1L);

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        Map<Attribute, Object> attributes = new HashMap<>();
        attributes.put(Attribute.SEARCH_TYPE, SearchType.QUERY_THEN_FETCH.toString().toLowerCase(Locale.ROOT));

        return new SearchQueryRecord(timestamp, measurements, attributes);
    }

    public static void compareJson(ToXContent param1, ToXContent param2) throws IOException {
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

    @SuppressWarnings("unchecked")
    public static boolean checkRecordsEquals(List<SearchQueryRecord> records1, List<SearchQueryRecord> records2) {
        if (records1.size() != records2.size()) {
            return false;
        }
        for (int i = 0; i < records1.size(); i++) {
            if (!records1.get(i).equals(records2.get(i))) {
                return false;
            }
            Map<Attribute, Object> attributes1 = records1.get(i).getAttributes();
            Map<Attribute, Object> attributes2 = records2.get(i).getAttributes();
            for (Map.Entry<Attribute, Object> entry : attributes1.entrySet()) {
                Attribute attribute = entry.getKey();
                Object value = entry.getValue();
                if (!attributes2.containsKey(attribute)) {
                    return false;
                }
                if (value instanceof Object[] && !Arrays.deepEquals((Object[]) value, (Object[]) attributes2.get(attribute))) {
                    return false;
                } else if (value instanceof Map
                    && !Maps.deepEquals((Map<Object, Object>) value, (Map<Object, Object>) attributes2.get(attribute))) {
                        return false;
                    }
            }
        }
        return true;
    }

    public static boolean checkRecordsEqualsWithoutOrder(
        List<SearchQueryRecord> records1,
        List<SearchQueryRecord> records2,
        MetricType metricType
    ) {
        Set<SearchQueryRecord> set2 = new TreeSet<>((a, b) -> SearchQueryRecord.compare(a, b, metricType));
        set2.addAll(records2);
        if (records1.size() != records2.size()) {
            return false;
        }
        for (int i = 0; i < records1.size(); i++) {
            if (!set2.contains(records1.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void registerAllQueryInsightsSettings(ClusterSettings clusterSettings) {
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_EXPORTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_CPU_EXPORTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_MEMORY_EXPORTER_SETTINGS);
    }
}
