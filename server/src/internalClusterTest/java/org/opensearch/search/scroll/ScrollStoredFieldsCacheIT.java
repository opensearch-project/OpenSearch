/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.scroll;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for scroll query StoredFieldsReader caching optimization.
 *
 * Tests verify that scroll queries correctly return all documents when using
 * the sequential reader cache optimization for stored fields.
 */
@LuceneTestCase.SuppressCodecs("*")
public class ScrollStoredFieldsCacheIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public ScrollStoredFieldsCacheIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    /**
     * Tests that scroll queries with sequential document access correctly utilize
     * the StoredFieldsReader cache and return all documents without data corruption.
     */
    public void testScrollWithSequentialReaderCache() throws Exception {
        int numDocs = randomIntBetween(100, 500);
        int scrollSize = randomIntBetween(10, 50);
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen("test");
        // Index documents
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("field", i).field("text", "document " + i).endObject())
                .get();
        }
        refresh("test");
        indexRandomForConcurrentSearch("test");
        Set<String> retrievedIds = new HashSet<>();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setSize(scrollSize)
            .setScroll(TimeValue.timeValueMinutes(2))
            .addSort("field", SortOrder.ASC)
            .get();

        try {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) numDocs));
            do {
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    // Verify no duplicate documents
                    assertTrue("Duplicate document id: " + hit.getId(), retrievedIds.add(hit.getId()));
                    // Verify document content is correct _source field
                    assertNotNull(hit.getSourceAsMap());
                    assertEquals(Integer.parseInt(hit.getId()), hit.getSourceAsMap().get("field"));
                }
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).get();
                assertNoFailures(searchResponse);
            } while (searchResponse.getHits().getHits().length > 0);
            // Verify all documents were retrieved
            assertThat(retrievedIds.size(), equalTo(numDocs));
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }

    /**
     * Tests scroll queries across multiple segments with batch sizes that
     * trigger the sequential reader optimization (>= 10 docs).
     */
    public void testScrollAcrossMultipleSegments() throws Exception {
        int docsPerSegment = randomIntBetween(20, 50);
        int numSegments = randomIntBetween(3, 5);
        int totalDocs = docsPerSegment * numSegments;
        int scrollSize = randomIntBetween(10, 50);
        int expectedBatches = (totalDocs + scrollSize - 1) / scrollSize;
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen("test");
        // Index documents in batches to create multiple segments
        for (int seg = 0; seg < numSegments; seg++) {
            for (int i = 0; i < docsPerSegment; i++) {
                int docId = seg * docsPerSegment + i;
                client().prepareIndex("test")
                    .setId(Integer.toString(docId))
                    .setSource(jsonBuilder().startObject().field("field", docId).field("segment", seg).endObject())
                    .get();
            }
            refresh("test");
        }
        indexRandomForConcurrentSearch("test");
        Set<String> retrievedIds = new HashSet<>();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setSize(scrollSize)
            .setScroll(TimeValue.timeValueMinutes(2))
            .addSort("field", SortOrder.ASC)
            .get();
        try {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) totalDocs));
            int batchCount = 0;
            do {
                batchCount++;
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertTrue("Duplicate document id: " + hit.getId(), retrievedIds.add(hit.getId()));
                }
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).get();
                assertNoFailures(searchResponse);
            } while (searchResponse.getHits().getHits().length > 0);
            // Verify all documents retrieved
            assertThat(retrievedIds.size(), equalTo(totalDocs));
            // Verify exact batch count
            assertThat(batchCount, equalTo(expectedBatches));
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }
}
