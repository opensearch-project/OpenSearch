/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.streaming.aggregation;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 3, maxNumDataNodes = 3)
public class SubAggregationIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {

    public SubAggregationIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    static final int NUM_SHARDS = 3;
    static final int MIN_SEGMENTS_PER_SHARD = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(3);

        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", NUM_SHARDS)    // Number of primary shards
            .put("index.number_of_replicas", 0)  // Number of replica shards
            .put("index.search.concurrent_segment_search.mode", "none")
            // Disable segment merging to keep individual segments
            .put("index.merge.policy.max_merged_segment", "1kb") // Keep segments small
            .put("index.merge.policy.segments_per_tier", "20") // Allow many segments per tier
            .put("index.merge.scheduler.max_thread_count", "1") // Limit merge threads
            .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("index").settings(indexSettings);
        createIndexRequest.mapping(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"field1\": { \"type\": \"keyword\" },\n"
                + "    \"field2\": { \"type\": \"integer\" }\n"
                + "  }\n"
                + "}",
            XContentType.JSON
        );
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());
        client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();
        BulkRequest bulkRequest = new BulkRequest();

        // We'll create 3 segments per shard by indexing docs into each segment and forcing a flush
        // Segment 1 - we'll add docs with field2 values in 1-3 range
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 1));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 2));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 3));
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures()); // Verify ingestion was successful
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        // Segment 2 - we'll add docs with field2 values in 11-13 range
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 11));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 12));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 13));
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        // Segment 3 - we'll add docs with field2 values in 21-23 range
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 21));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 22));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 23));
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();
        ensureSearchable("index");

        // Verify that we have the expected number of shards and segments
        IndicesSegmentResponse segmentResponse = client().admin().indices().segments(new IndicesSegmentsRequest("index")).actionGet();
        assertEquals(NUM_SHARDS, segmentResponse.getIndices().get("index").getShards().size());

        // Verify each shard has at least MIN_SEGMENTS_PER_SHARD segments
        segmentResponse.getIndices().get("index").getShards().values().forEach(indexShardSegments -> {
            assertTrue(
                "Expected at least "
                    + MIN_SEGMENTS_PER_SHARD
                    + " segments but found "
                    + indexShardSegments.getShards()[0].getSegments().size(),
                indexShardSegments.getShards()[0].getSegments().size() >= MIN_SEGMENTS_PER_SHARD
            );
        });
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregation() throws Exception {
        // This test validates streaming aggregation with 3 shards, each with at least 3 segments
        TermsAggregationBuilder agg = terms("agg1").field("field1").subAggregation(AggregationBuilders.max("agg2").field("field2"));
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());
        StringTerms agg1 = (StringTerms) resp.getAggregations().asMap().get("agg1");
        List<StringTerms.Bucket> buckets = agg1.getBuckets();
        assertEquals(3, buckets.size());

        // Validate all buckets - each should have 30 documents
        for (StringTerms.Bucket bucket : buckets) {
            assertEquals(30, bucket.getDocCount());
            assertNotNull(bucket.getAggregations().get("agg2"));
        }
        buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));

        StringTerms.Bucket bucket1 = buckets.get(0);
        assertEquals("value1", bucket1.getKeyAsString());
        assertEquals(30, bucket1.getDocCount());
        Max maxAgg1 = (Max) bucket1.getAggregations().get("agg2");
        assertEquals(21.0, maxAgg1.getValue(), 0.001);

        StringTerms.Bucket bucket2 = buckets.get(1);
        assertEquals("value2", bucket2.getKeyAsString());
        assertEquals(30, bucket2.getDocCount());
        Max maxAgg2 = (Max) bucket2.getAggregations().get("agg2");
        assertEquals(22.0, maxAgg2.getValue(), 0.001);

        StringTerms.Bucket bucket3 = buckets.get(2);
        assertEquals("value3", bucket3.getKeyAsString());
        assertEquals(30, bucket3.getDocCount());
        Max maxAgg3 = (Max) bucket3.getAggregations().get("agg2");
        assertEquals(23.0, maxAgg3.getValue(), 0.001);

        for (SearchHit hit : resp.getHits().getHits()) {
            assertNotNull(hit.getSourceAsString());
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationTerm() throws Exception {
        // This test validates streaming aggregation with 3 shards, each with at least 3 segments
        TermsAggregationBuilder agg = terms("agg1").field("field1");
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());
        StringTerms agg1 = (StringTerms) resp.getAggregations().asMap().get("agg1");
        List<StringTerms.Bucket> buckets = agg1.getBuckets();
        assertEquals(3, buckets.size());

        // Validate all buckets - each should have 30 documents
        for (StringTerms.Bucket bucket : buckets) {
            assertEquals(30, bucket.getDocCount());
        }
        buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));

        StringTerms.Bucket bucket1 = buckets.get(0);
        assertEquals("value1", bucket1.getKeyAsString());
        assertEquals(30, bucket1.getDocCount());

        StringTerms.Bucket bucket2 = buckets.get(1);
        assertEquals("value2", bucket2.getKeyAsString());
        assertEquals(30, bucket2.getDocCount());

        StringTerms.Bucket bucket3 = buckets.get(2);
        assertEquals("value3", bucket3.getKeyAsString());
        assertEquals(30, bucket3.getDocCount());

        for (SearchHit hit : resp.getHits().getHits()) {
            assertNotNull(hit.getSourceAsString());
        }
    }
}
