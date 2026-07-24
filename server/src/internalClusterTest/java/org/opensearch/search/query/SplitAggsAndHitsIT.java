/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

public class SplitAggsAndHitsIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test1";
    private static final int NUM_DOCS = 500;
    private static final List<String> CATEGORIES = Arrays.asList("alpha", "beta", "gamma", "delta");

    @Before
    public void setUpIndex() throws Exception {
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build();
        prepareCreate(INDEX).setSettings(indexSettings)
            .setMapping(
                "title",
                "type=text",
                "category",
                "type=keyword",
                "ts",
                "type=long",
                "price",
                "type=double",
                "in_stock",
                "type=boolean"
            )
            .get();

        List<IndexRequestBuilder> docs = new ArrayList<>(NUM_DOCS);
        for (int i = 0; i < NUM_DOCS; i++) {
            String title = (i % 3 == 0) ? "title1 " : "title2";
            docs.add(
                client().prepareIndex(INDEX)
                    .setId(Integer.toString(i))
                    .setSource(
                        "title",
                        title,
                        "category",
                        CATEGORIES.get(i % CATEGORIES.size()),
                        "ts",
                        (long) i,
                        "price",
                        (double) (i % 50),
                        "in_stock",
                        i % 2 == 0
                    )
            );
        }
        indexRandom(true, docs);
    }

    @After
    public void resetSetting() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .putNull(SearchService.SEARCH_SPLIT_AGGS_AND_HITS_SETTING.getKey())
                    .putNull(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey())
            )
            .get();
    }

    public void testBasic() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title1"))
            .size(10)
            .aggregation(AggregationBuilders.terms("by_category").field("category").size(10));
        assertEquivalent(src);
    }

    /** Field-sorted hits with aggregation: still must be semantically equivalent. */
    public void testFieldSortedWithAgg() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .sort("ts", SortOrder.ASC)
            .size(20)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        assertEquivalent(src, true);
    }

    public void testMultipleAggregations() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title2"))
            .size(5)
            .aggregation(AggregationBuilders.terms("by_category").field("category"))
            .aggregation(AggregationBuilders.avg("avg_price").field("price"))
            .aggregation(AggregationBuilders.stats("price_stats").field("price"));
        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);
        assertTermsEquivalent(off, on, "by_category");
        Avg avgOff = off.getAggregations().get("avg_price");
        Avg avgOn = on.getAggregations().get("avg_price");
        assertEquals("avg differs", avgOff.getValue(), avgOn.getValue(), 1e-9);
        Stats statsOff = off.getAggregations().get("price_stats");
        Stats statsOn = on.getAggregations().get("price_stats");
        assertEquals(statsOff.getCount(), statsOn.getCount());
        assertEquals("stats sum differs", statsOff.getSum(), statsOn.getSum(), 1e-9);
        assertEquals("stats avg differs", statsOff.getAvg(), statsOn.getAvg(), 1e-9);
        assertEquals("stats min differs", statsOff.getMin(), statsOn.getMin(), 1e-9);
        assertEquals("stats max differs", statsOff.getMax(), statsOn.getMax(), 1e-9);
    }

    public void testNestedAggregations() {
        AggregationBuilder agg = AggregationBuilders.terms("outer")
            .field("category")
            .subAggregation(AggregationBuilders.terms("inner_in_stock").field("in_stock"))
            .subAggregation(AggregationBuilders.avg("avg_price").field("price"));
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(0).aggregation(agg);

        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);
        Terms outerOff = off.getAggregations().get("outer");
        Terms outerOn = on.getAggregations().get("outer");
        assertEquals(outerOff.getBuckets().size(), outerOn.getBuckets().size());
        for (int i = 0; i < outerOff.getBuckets().size(); i++) {
            Terms.Bucket bo = outerOff.getBuckets().get(i);
            Terms.Bucket bn = outerOn.getBuckets().get(i);
            assertEquals(bo.getKeyAsString(), bn.getKeyAsString());
            assertEquals(bo.getDocCount(), bn.getDocCount());
            Avg avgOff = bo.getAggregations().get("avg_price");
            Avg avgOn = bn.getAggregations().get("avg_price");
            assertEquals("avg in bucket " + bo.getKeyAsString() + " differs", avgOff.getValue(), avgOn.getValue(), 1e-9);
        }
    }

    public void testGlobalAndNonGlobalAggregations() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title2"))
            .size(3)
            .aggregation(AggregationBuilders.terms("by_category").field("category"))
            .aggregation(AggregationBuilders.global("all_docs").subAggregation(AggregationBuilders.avg("global_avg").field("price")));
        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);
        assertTermsEquivalent(off, on, "by_category");
        Global gOff = off.getAggregations().get("all_docs");
        Global gOn = on.getAggregations().get("all_docs");
        Avg avgOff = gOff.getAggregations().get("global_avg");
        Avg avgOn = gOn.getAggregations().get("global_avg");
        assertEquals("global avg differs", avgOff.getValue(), avgOn.getValue(), 1e-9);
        assertEquals("global doc_count differs", gOff.getDocCount(), gOn.getDocCount());
    }

    public void testPostFilterDoesNotBreakAggs() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title1"))
            .postFilter(QueryBuilders.termQuery("category", "alpha"))
            .size(10)
            .aggregation(AggregationBuilders.terms("by_category").field("category").size(10));
        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);

        assertEquals("hit count differs", off.getHits().getHits().length, on.getHits().getHits().length);
        for (SearchHit h : on.getHits()) {
            assertEquals("alpha", h.getSourceAsMap().get("category"));
        }
        // Aggregations are not narrowed by post_filter, so all categories should remain visible.
        assertTermsEquivalent(off, on, "by_category");
    }

    public void testDfsQueryThenFetch() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title2"))
            .size(5)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));

        setSplitEnabled(false);
        SearchResponse off = client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(src).get();
        setSplitEnabled(true);
        SearchResponse on = client().prepareSearch(INDEX).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSource(src).get();
        assertSearchResponse(off);
        assertSearchResponse(on);
        assertTermsEquivalent(off, on, "by_category");
    }

    public void testConcurrentSegmentSearchEquivalence() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL)
            )
            .get();

        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title1"))
            .size(7)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        assertEquivalent(src);
    }

    public void testSizeZeroAggregationOnly() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title2"))
            .size(0)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);
        assertEquals(0, on.getHits().getHits().length);
        assertEquals(0, off.getHits().getHits().length);
        assertTermsEquivalent(off, on, "by_category");
    }

    public void testTrackTotalHitsAccurate() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title1"))
            .size(5)
            .trackTotalHits(true)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);
        assertEquals(off.getHits().getTotalHits().value(), on.getHits().getTotalHits().value());
        assertTermsEquivalent(off, on, "by_category");
    }

    public void testMinScoreFallbackEquivalent() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "title2"))
            .minScore(0.1f)
            .sort("ts", SortOrder.ASC)
            .size(5)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        assertEquivalent(src, true);
    }

    public void testTerminateAfterFallbackEquivalent() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .terminateAfter(50)
            .sort("ts", SortOrder.ASC)
            .size(5)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));
        assertEquivalent(src, true);
    }

    public void testToggleSettingBetweenRequests() {
        SearchSourceBuilder src = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "opensearch"))
            .sort("ts", SortOrder.ASC)
            .size(5)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));

        setSplitEnabled(true);
        SearchResponse first = client().prepareSearch(INDEX).setSource(src).get();
        setSplitEnabled(false);
        SearchResponse second = client().prepareSearch(INDEX).setSource(src).get();
        setSplitEnabled(true);
        SearchResponse third = client().prepareSearch(INDEX).setSource(src).get();
        assertSearchResponse(first);
        assertSearchResponse(second);
        assertSearchResponse(third);
        assertHitsEquivalent(first, second);
        assertHitsEquivalent(second, third);
        assertTermsEquivalent(first, second, "by_category");
        assertTermsEquivalent(second, third, "by_category");
    }

    private void assertEquivalent(SearchSourceBuilder src) {
        assertEquivalent(src, false);
    }

    private void assertEquivalent(SearchSourceBuilder src, boolean assertHits) {
        SearchResponse off = search(src, false);
        SearchResponse on = search(src, true);
        if (assertHits) {
            assertHitsEquivalent(off, on);
        } else if (off.getHits().getTotalHits() != null && on.getHits().getTotalHits() != null) {
            assertEquals("total hits differs", off.getHits().getTotalHits().value(), on.getHits().getTotalHits().value());
        }
        if (off.getAggregations() != null) {
            // Default helper assumes the most common "by_category" terms agg if present.
            if (off.getAggregations().get("by_category") != null) {
                assertTermsEquivalent(off, on, "by_category");
            }
        }
    }

    private SearchResponse search(SearchSourceBuilder src, boolean splitEnabled) {
        setSplitEnabled(splitEnabled);
        SearchResponse resp = client().prepareSearch(INDEX).setSource(src).get();
        assertSearchResponse(resp);
        return resp;
    }

    private void setSplitEnabled(boolean enabled) {
        ClusterUpdateSettingsResponse resp = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(SearchService.SEARCH_SPLIT_AGGS_AND_HITS_SETTING.getKey(), enabled))
            .get();
        assertAcked(resp);
    }

    private static void assertHitsEquivalent(SearchResponse a, SearchResponse b) {
        SearchHit[] ha = a.getHits().getHits();
        SearchHit[] hb = b.getHits().getHits();
        assertEquals("hit count differs", ha.length, hb.length);
        for (int i = 0; i < ha.length; i++) {
            assertEquals("hit[" + i + "] id differs", ha[i].getId(), hb[i].getId());
            // Score-sorted requests must keep the exact same score; field-sorted requests yield NaN.
            if (Float.isNaN(ha[i].getScore()) == false) {
                assertEquals("hit[" + i + "] score differs", ha[i].getScore(), hb[i].getScore(), 1e-6f);
            }
        }
    }

    private static void assertTermsEquivalent(SearchResponse a, SearchResponse b, String aggName) {
        Terms ta = a.getAggregations().get(aggName);
        Terms tb = b.getAggregations().get(aggName);
        assertEquals("bucket count differs", ta.getBuckets().size(), tb.getBuckets().size());
        for (int i = 0; i < ta.getBuckets().size(); i++) {
            Terms.Bucket ba = ta.getBuckets().get(i);
            Terms.Bucket bb = tb.getBuckets().get(i);
            assertEquals("bucket[" + i + "] key differs", ba.getKeyAsString(), bb.getKeyAsString());
            assertEquals("bucket[" + i + "] count differs", ba.getDocCount(), bb.getDocCount());
        }
        assertEquals("doc_count_error_upper_bound differs", ta.getDocCountError(), tb.getDocCountError());
        assertEquals("sum_other_doc_count differs", ta.getSumOfOtherDocCounts(), tb.getSumOfOtherDocCounts());
    }
}
