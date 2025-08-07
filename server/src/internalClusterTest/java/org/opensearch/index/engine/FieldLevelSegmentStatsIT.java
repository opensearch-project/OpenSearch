/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test for field-level segment statistics
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class FieldLevelSegmentStatsIT extends OpenSearchIntegTestCase {

    /**
     * Comprehensive test for field-level segment stats API covering single and multiple indices
     */
    public void testFieldLevelSegmentStatsEndToEnd() {
        String index1 = "test-field-stats";
        String index2 = "test-index-2";

        // Create first index with explicit mapping
        assertAcked(
            prepareCreate(index1).setMapping(
                "title",
                "type=text",
                "keyword_field",
                "type=keyword",
                "numeric_field",
                "type=long",
                "date_field",
                "type=date"
            )
        );

        // Create second index with different mapping
        assertAcked(prepareCreate(index2).setMapping("field1", "type=text", "field2", "type=keyword"));

        // Index documents to first index
        int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(index1)
                .setId(String.valueOf(i))
                .setSource(
                    "title",
                    "Document title " + i,
                    "keyword_field",
                    "keyword_" + (i % 10),
                    "numeric_field",
                    i,
                    "date_field",
                    "2025-01-" + ((i % 28) + 1 < 10 ? "0" + ((i % 28) + 1) : String.valueOf((i % 28) + 1))
                )
                .get();
        }

        // Index documents to second index
        for (int i = 0; i < 50; i++) {
            client().prepareIndex(index2).setId(String.valueOf(i)).setSource("field1", "text " + i, "field2", "keyword" + i).get();
        }

        refresh(index1, index2);

        // Test single index with field-level stats
        IndicesStatsRequest singleIndexRequest = new IndicesStatsRequest();
        singleIndexRequest.indices(index1);
        singleIndexRequest.segments(true);
        singleIndexRequest.includeFieldLevelSegmentFileSizes(true);

        IndicesStatsResponse singleIndexResponse = client().admin().indices().stats(singleIndexRequest).actionGet();

        SegmentsStats segmentStats = singleIndexResponse.getIndex(index1).getTotal().getSegments();
        assertNotNull("Segment stats should not be null", segmentStats);

        Map<String, Map<String, Long>> fieldLevelStats = segmentStats.getFieldLevelFileSizes();
        assertNotNull("Field-level stats should not be null", fieldLevelStats);
        assertFalse("Field-level stats should not be empty", fieldLevelStats.isEmpty());

        // Verify expected fields are present
        assertTrue("Should have stats for title", fieldLevelStats.containsKey("title"));
        assertTrue("Should have stats for keyword_field", fieldLevelStats.containsKey("keyword_field"));
        assertTrue("Should have stats for numeric_field", fieldLevelStats.containsKey("numeric_field"));
        assertTrue("Should have stats for date_field", fieldLevelStats.containsKey("date_field"));

        // Verify stats have positive values
        for (Map.Entry<String, Map<String, Long>> fieldEntry : fieldLevelStats.entrySet()) {
            assertFalse("Field " + fieldEntry.getKey() + " should have file stats", fieldEntry.getValue().isEmpty());
            for (Map.Entry<String, Long> fileEntry : fieldEntry.getValue().entrySet()) {
                assertThat("File sizes should be positive", fileEntry.getValue(), greaterThan(0L));
            }
        }

        // Test multiple indices
        IndicesStatsRequest multiIndexRequest = new IndicesStatsRequest();
        multiIndexRequest.indices(index1, index2);
        multiIndexRequest.segments(true);
        multiIndexRequest.includeFieldLevelSegmentFileSizes(true);

        IndicesStatsResponse multiIndexResponse = client().admin().indices().stats(multiIndexRequest).actionGet();

        // Verify both indices have field-level stats
        SegmentsStats stats1 = multiIndexResponse.getIndex(index1).getTotal().getSegments();
        assert stats1 != null;
        assertFalse("Index1 field-level stats should not be empty", stats1.getFieldLevelFileSizes().isEmpty());
        assertTrue("Index1 should have stats for title", stats1.getFieldLevelFileSizes().containsKey("title"));

        SegmentsStats stats2 = multiIndexResponse.getIndex(index2).getTotal().getSegments();
        assert stats2 != null;
        assertFalse("Index2 field-level stats should not be empty", stats2.getFieldLevelFileSizes().isEmpty());
        assertTrue("Index2 should have stats for field1", stats2.getFieldLevelFileSizes().containsKey("field1"));
        assertTrue("Index2 should have stats for field2", stats2.getFieldLevelFileSizes().containsKey("field2"));

        // Test backward compatibility (without field-level stats)
        IndicesStatsRequest normalRequest = new IndicesStatsRequest();
        normalRequest.indices(index1);
        normalRequest.segments(true);
        normalRequest.includeFieldLevelSegmentFileSizes(false);

        IndicesStatsResponse normalResponse = client().admin().indices().stats(normalRequest).actionGet();
        SegmentsStats normalSegmentStats = normalResponse.getIndex(index1).getTotal().getSegments();

        assertNotNull("Normal segment stats should not be null", normalSegmentStats);
        assertTrue("Normal request should not include field-level stats", normalSegmentStats.getFieldLevelFileSizes().isEmpty());
    }
}
