/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

public class SplitAggsAndHitsIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "split_aggs_hits_idx";

    @After
    public void resetSetting() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().putNull(SearchService.SEARCH_SPLIT_AGGS_AND_HITS_SETTING.getKey()))
            .setTransientSettings(Settings.builder().putNull(SearchService.SEARCH_SPLIT_AGGS_AND_HITS_SETTING.getKey()))
            .get();
    }

    public void testSplitAggsAndHitsEquivalence() throws Exception {
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
        prepareCreate(INDEX).setSettings(indexSettings)
            .setMapping("title", "type=text", "category", "type=keyword", "ts", "type=long")
            .get();

        List<String> categories = List.of("a", "b", "c", "d");
        int numDocs = 1_000;
        for (int i = 0; i < numDocs; i++) {
            String title = (i % 3 == 0 ? "opensearch lucene query " : "opensearch ") + (i % 7 == 0 ? "fast " : "") + String.format(
                Locale.ROOT,
                "doc-%d",
                i
            );
            client().prepareIndex(INDEX)
                .setId(Integer.toString(i))
                .setSource("title", title, "category", categories.get(i % categories.size()), "ts", (long) i)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "opensearch lucene fast"))
            .size(10)
            .aggregation(AggregationBuilders.terms("by_category").field("category").size(10))
            .trackTotalHits(false);

        setSplitEnabled(false);
        SearchResponse off = client().prepareSearch(INDEX).setSource(source).get();
        assertSearchResponse(off);

        setSplitEnabled(true);
        SearchResponse on = client().prepareSearch(INDEX).setSource(source).get();
        assertSearchResponse(on);

        assertHitsEquivalent(off, on);
        assertTermsEquivalent(off, on);
    }

    public void testSplitWhenSortedByField() throws Exception {
        prepareCreate(INDEX).setMapping("category", "type=keyword", "ts", "type=long").get();
        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("category", i % 3 == 0 ? "x" : "y", "ts", (long) i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .sort("ts")
            .size(5)
            .aggregation(AggregationBuilders.terms("by_category").field("category"));

        setSplitEnabled(false);
        SearchResponse off = client().prepareSearch(INDEX).setSource(source).get();
        setSplitEnabled(true);
        SearchResponse on = client().prepareSearch(INDEX).setSource(source).get();

        assertSearchResponse(off);
        assertSearchResponse(on);
        assertHitsEquivalent(off, on);
        assertTermsEquivalent(off, on);
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
            assertEquals("hit[" + i + "] score differs", ha[i].getScore(), hb[i].getScore(), 1e-6f);
        }
    }

    private static void assertTermsEquivalent(SearchResponse a, SearchResponse b) {
        Terms ta = a.getAggregations().get("by_category");
        Terms tb = b.getAggregations().get("by_category");
        List<String> keysA = bucketKeys(ta);
        List<String> keysB = bucketKeys(tb);
        assertEquals("agg buckets differ: " + keysA + " vs " + keysB, keysA, keysB);
        for (int i = 0; i < ta.getBuckets().size(); i++) {
            assertEquals(
                "bucket " + keysA.get(i) + " count differs",
                ta.getBuckets().get(i).getDocCount(),
                tb.getBuckets().get(i).getDocCount()
            );
        }
    }

    private static List<String> bucketKeys(Terms terms) {
        List<String> keys = new ArrayList<>();
        for (Terms.Bucket b : terms.getBuckets()) {
            keys.add(b.getKeyAsString() + "=" + b.getDocCount());
        }
        return keys;
    }
}
