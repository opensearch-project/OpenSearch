/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY;
import static org.opensearch.search.SearchService.MAX_AGGREGATION_REWRITE_FILTERS;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class FilterRewriteIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {

    // simulate segment level match all
    private static final QueryBuilder QUERY = QueryBuilders.termQuery("match", true);
    private static final Map<String, Long> expected = new HashMap<>();

    public FilterRewriteIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() },
            // Intra-segment partitioning: exercise the partition-aware filter-rewrite path so the same assertions
            // must hold whether or not a segment is split into doc-id-range partitions (guards #18016).
            new Object[] {
                Settings.builder()
                    .put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true)
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "segment")
                    .build() },
            new Object[] {
                Settings.builder()
                    .put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true)
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "force")
                    .build() },
            new Object[] {
                Settings.builder()
                    .put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true)
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "balanced")
                    .put(CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE.getKey(), 1000)
                    .build() }
        );
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx").get());
        expected.clear();

        final int repeat = randomIntBetween(2, 10);
        final Set<Long> longTerms = new HashSet<>();

        for (int i = 0; i < repeat; i++) {
            final List<IndexRequestBuilder> indexRequests = new ArrayList<>();

            long longTerm;
            do {
                longTerm = randomInt(repeat * 2);
            } while (!longTerms.add(longTerm));
            ZonedDateTime time = ZonedDateTime.of(2024, 1, ((int) longTerm) + 1, 0, 0, 0, 0, ZoneOffset.UTC);
            String dateTerm = DateFormatter.forPattern("yyyy-MM-dd").format(time);

            final int frequency = randomBoolean() ? 1 : randomIntBetween(2, 20);
            for (int j = 0; j < frequency; j++) {
                indexRequests.add(
                    client().prepareIndex("idx")
                        .setSource(jsonBuilder().startObject().field("date", dateTerm).field("match", true).endObject())
                );
            }
            expected.put(dateTerm + "T00:00:00.000Z", (long) frequency);

            indexRandom(true, false, indexRequests);
        }

        ensureSearchable();
    }

    public void testMinDocCountOnDateHistogram() throws Exception {
        final SearchResponse allResponse = client().prepareSearch("idx")
            .setSize(0)
            .setQuery(QUERY)
            .addAggregation(dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.DAY).minDocCount(0))
            .get();

        final Histogram allHisto = allResponse.getAggregations().get("histo");
        Map<String, Long> results = new HashMap<>();
        allHisto.getBuckets().forEach(bucket -> results.put(bucket.getKeyAsString(), bucket.getDocCount()));

        for (Map.Entry<String, Long> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), results.get(entry.getKey()));
        }
    }

    public void testDisableOptimizationGivesSameResults() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setSize(0)
            .addAggregation(dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.DAY).minDocCount(0))
            .get();

        final Histogram allHisto1 = response.getAggregations().get("histo");

        final ClusterUpdateSettingsResponse updateSettingResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(MAX_AGGREGATION_REWRITE_FILTERS.getKey(), 0))
            .get();

        assertEquals(updateSettingResponse.getTransientSettings().get(MAX_AGGREGATION_REWRITE_FILTERS.getKey()), "0");

        response = client().prepareSearch("idx")
            .setSize(0)
            .addAggregation(dateHistogram("histo").field("date").calendarInterval(DateHistogramInterval.DAY).minDocCount(0))
            .get();

        final Histogram allHisto2 = response.getAggregations().get("histo");

        assertEquals(allHisto1, allHisto2);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(MAX_AGGREGATION_REWRITE_FILTERS.getKey()))
            .get();
    }

    /**
     * date_histogram WITH a sub-aggregation exercises the partition-aware filter-rewrite collection path (the
     * primary #18016 fix). Under intra-segment strategies the per-bucket doc counts must still match the
     * expected per-day frequencies exactly, i.e. no per-partition duplication.
     */
    public void testDateHistogramWithSubAggMatchesExpected() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setSize(0)
            .setQuery(QUERY)
            .addAggregation(
                dateHistogram("histo").field("date")
                    .calendarInterval(DateHistogramInterval.DAY)
                    .minDocCount(1)
                    .subAggregation(avg("avg_ts").field("date"))
            )
            .get();

        final Histogram histo = response.getAggregations().get("histo");
        long total = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            Long expectedCount = expected.get(bucket.getKeyAsString());
            assertEquals(expectedCount, (Long) bucket.getDocCount());
            assertNotNull(bucket.getAggregations().get("avg_ts"));
            total += bucket.getDocCount();
        }
        long expectedTotal = expected.values().stream().mapToLong(Long::longValue).sum();
        assertThat(total, equalTo(expectedTotal));
    }

    /**
     * Nested filter-rewrite under filter-rewrite: range(over the day-of-month) -&gt; date_histogram. The inner
     * date_histogram is a leaf with no sub-agg; under intra it is collected through the parent's partition-clamped
     * doc set, so total counts must still equal the indexed total under every strategy.
     */
    public void testNestedRangeDateHistogram() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setSize(0)
            .setQuery(QUERY)
            .addAggregation(
                dateHistogram("histo").field("date")
                    .calendarInterval(DateHistogramInterval.MONTH)
                    .subAggregation(dateHistogram("inner").field("date").calendarInterval(DateHistogramInterval.DAY))
            )
            .get();

        final Histogram histo = response.getAggregations().get("histo");
        long total = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            total += bucket.getDocCount();
        }
        long expectedTotal = expected.values().stream().mapToLong(Long::longValue).sum();
        assertThat(total, equalTo(expectedTotal));
    }

    /**
     * Dedicated large-index test that guarantees the {@code balanced} strategy actually splits a segment (the
     * suite-scope {@code idx} is too small to exceed the min-segment-size threshold). Indexes 5000 docs across
     * 50 distinct days in 2 segments so each segment (~2500 docs) exceeds both the fair-share and the
     * min_segment_size=1000 threshold, forcing balanced/force to partition. Asserts the partition-aware
     * date_histogram + sub-agg counts are exact under every strategy (no #18016 per-partition duplication).
     */
    public void testFilterRewriteWithIntraSegmentPartitioning() throws Exception {
        createIndex("fr_intra_agg", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        try {
            int days = 50;
            int perDay = 100; // 50 * 100 = 5000 docs
            ZonedDateTime base = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            List<IndexRequestBuilder> builders = new ArrayList<>(days * perDay);
            for (int d = 0; d < days; d++) {
                ZonedDateTime time = base.plusDays(d);
                String dateTerm = DateFormatter.forPattern("yyyy-MM-dd").format(time);
                for (int j = 0; j < perDay; j++) {
                    builders.add(
                        client().prepareIndex("fr_intra_agg")
                            .setSource(jsonBuilder().startObject().field("date", dateTerm).field("match", true).endObject())
                    );
                }
            }
            indexBulkWithSegments(builders, 2);
            indexRandomForConcurrentSearch("fr_intra_agg");

            final SearchResponse response = client().prepareSearch("fr_intra_agg")
                .setSize(0)
                .setQuery(QUERY)
                .addAggregation(
                    dateHistogram("histo").field("date")
                        .calendarInterval(DateHistogramInterval.DAY)
                        .minDocCount(1)
                        .subAggregation(avg("avg_ts").field("date"))
                )
                .get();

            final Histogram histo = response.getAggregations().get("histo");
            assertThat(histo.getBuckets().size(), equalTo(days));
            long total = 0;
            for (Histogram.Bucket bucket : histo.getBuckets()) {
                assertThat(bucket.getDocCount(), equalTo((long) perDay));
                assertNotNull(bucket.getAggregations().get("avg_ts"));
                total += bucket.getDocCount();
            }
            assertThat(total, equalTo((long) days * perDay));
        } finally {
            internalCluster().wipeIndices("fr_intra_agg");
        }
    }
}
