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
import static org.opensearch.search.SearchService.MAX_AGGREGATION_REWRITE_FILTERS;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

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
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
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
}
