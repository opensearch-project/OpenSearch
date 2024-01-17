/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.action.search.SearchType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLengthBetween;
import static org.opensearch.test.OpenSearchTestCase.randomArray;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

final public class QueryInsightsTestUtils {

    public QueryInsightsTestUtils() {}

    public static List<SearchQueryLatencyRecord> generateQueryInsightRecords(int count) {
        return generateQueryInsightRecords(count, count);
    }

    /**
     * Creates a List of random Query Insight Records for testing purpose
     */
    public static List<SearchQueryLatencyRecord> generateQueryInsightRecords(int lower, int upper) {
        List<SearchQueryLatencyRecord> records = new ArrayList<>();
        int countOfRecords = randomIntBetween(lower, upper);
        for (int i = 0; i < countOfRecords; ++i) {
            Map<String, Object> propertyMap = new HashMap<>();
            int countOfProperties = randomIntBetween(2, 5);
            for (int j = 0; j < countOfProperties; ++j) {
                propertyMap.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
            }
            Map<String, Long> phaseLatencyMap = new HashMap<>();
            int countOfPhases = randomIntBetween(2, 5);
            for (int j = 0; j < countOfPhases; ++j) {
                phaseLatencyMap.put(randomAlphaOfLengthBetween(5, 10), randomLong());
            }
            records.add(
                new SearchQueryLatencyRecord(
                    System.currentTimeMillis(),
                    SearchType.QUERY_THEN_FETCH,
                    "{\"size\":20}",
                    randomIntBetween(1, 100),
                    randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(5, 10)),
                    propertyMap,
                    phaseLatencyMap,
                    phaseLatencyMap.values().stream().mapToLong(x -> x).sum()
                )
            );
        }
        return records;
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

    public static boolean checkRecordsEquals(List<SearchQueryLatencyRecord> records1, List<SearchQueryLatencyRecord> records2) {
        if (records1.size() != records2.size()) {
            return false;
        }
        for (int i = 0; i < records1.size(); i++) {
            if (!records1.get(i).equals(records2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkRecordsEqualsWithoutOrder(List<SearchQueryLatencyRecord> records1, List<SearchQueryLatencyRecord> records2) {
        Set<SearchQueryLatencyRecord> set2 = new TreeSet<>(new LatencyRecordComparator());
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

    static class LatencyRecordComparator implements Comparator<SearchQueryLatencyRecord> {
        @Override
        public int compare(SearchQueryLatencyRecord record1, SearchQueryLatencyRecord record2) {
            if (record1.equals(record2)) {
                return 0;
            }
            return record1.compareTo(record2);
        }
    }
}
