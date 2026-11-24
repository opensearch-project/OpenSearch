/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

/**
 * A {@code terms} aggregation ordered by a cardinality defers collection to global ordinals once they are loaded, eagerly or
 * by an earlier search, and returns the same buckets either way.
 */
public class CardinalityDeferredOrdinalsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final int GROUPS = 200;

    public CardinalityDeferredOrdinalsIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    private static String group(int g) {
        return String.format(Locale.ROOT, "g%03d", g);
    }

    /**
     * Group {@code g} holds {@code g + 1} distinct values, so the top buckets and their cardinalities are known.
     */
    private void indexGroups() {
        // Three refreshes give the shard several segments, so global ordinals need an ordinal map
        for (int batch = 0; batch < 3; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int g = 0; g < GROUPS; g++) {
                for (int v = batch; v <= g; v += 3) {
                    bulk.add(client().prepareIndex("idx").setSource("group", group(g), "eager_value", "v" + v, "value", "v" + v));
                }
            }
            assertNoFailures(bulk.get());
            refresh("idx");
        }
    }

    private int segments() {
        return client().admin()
            .indices()
            .prepareSegments("idx")
            .get()
            .getIndices()
            .get("idx")
            .getShards()
            .values()
            .stream()
            .mapToInt(shard -> shard.getShards()[0].getSegments().size())
            .sum();
    }

    private SearchResponse topGroupsByCardinality(String field, int size) {
        SearchResponse response = client().prepareSearch("idx")
            .setSize(0)
            .setProfile(true)
            .addAggregation(
                terms("groups").field("group")
                    .size(size)
                    // Under concurrent segment search every slice keeps its own top shard_size buckets, which can leave out part of
                    // a top bucket's documents. Keeping every bucket makes the expected results exact.
                    .shardSize(GROUPS + 1)
                    // the profiler wraps aggregators, which breaks ordering by a sub-aggregation under breadth_first
                    .collectMode(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
                    .order(BucketOrder.aggregation("card", false))
                    .subAggregation(cardinality("card").field(field))
            )
            .get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("groups");
        assertEquals(size, terms.getBuckets().size());
        for (int i = 0; i < size; i++) {
            Terms.Bucket bucket = terms.getBuckets().get(i);
            int g = GROUPS - 1 - i;
            assertEquals(group(g), bucket.getKeyAsString());
            assertEquals(g + 1, ((Cardinality) bucket.getAggregations().get("card")).getValue());
        }
        return response;
    }

    /**
     * The {@code deferred_ordinals} debug value of every shard and slice of the cardinality aggregation.
     */
    private static Set<Object> deferredOrdinals(SearchResponse response) {
        Set<Object> values = new HashSet<>();
        for (ProfileShardResult shard : response.getProfileResults().values()) {
            for (ProfileResult terms : shard.getAggregationProfileResults().getProfileResults()) {
                for (ProfileResult card : terms.getProfiledChildren()) {
                    if (card.getLuceneDescription().equals("card")) {
                        values.add(card.getDebugInfo().get("deferred_ordinals"));
                    }
                }
            }
        }
        assertFalse(values.isEmpty());
        return values;
    }

    public void testDefersOnceGlobalOrdinalsAreLoaded() throws Exception {
        assertAcked(
            prepareCreate("idx").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("group", "type=keyword", "eager_value", "type=keyword,eager_global_ordinals=true", "value", "type=keyword")
        );
        indexGroups();
        // A single segment has global ordinals without an ordinal map, which always counts as loaded
        assumeTrue("needs several segments", segments() > 1);
        final int size = randomIntBetween(1, 20);

        // Eager global ordinals are loaded on refresh
        assertEquals(Set.of("used"), deferredOrdinals(topGroupsByCardinality("eager_value", size)));

        // Lazy ones aren't, until something loads them
        assertEquals(Set.of("global_ordinals_not_loaded"), deferredOrdinals(topGroupsByCardinality("value", size)));
        assertSearchResponse(client().prepareSearch("idx").setSize(0).addAggregation(terms("load").field("value")).get());
        assertEquals(Set.of("used"), deferredOrdinals(topGroupsByCardinality("value", size)));

        // New documents need new global ordinals
        client().prepareIndex("idx").setSource("group", "other", "value", "x", "eager_value", "x").get();
        refresh("idx");
        assertEquals(Set.of("global_ordinals_not_loaded"), deferredOrdinals(topGroupsByCardinality("value", size)));
        assertEquals(Set.of("used"), deferredOrdinals(topGroupsByCardinality("eager_value", size)));
    }
}
