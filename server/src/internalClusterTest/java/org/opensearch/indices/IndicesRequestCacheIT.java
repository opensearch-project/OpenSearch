/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class IndicesRequestCacheIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public IndicesRequestCacheIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() },
            new Object[] { Settings.builder().put(FeatureFlags.PLUGGABLE_CACHE, "true").build() },
            new Object[] { Settings.builder().put(FeatureFlags.PLUGGABLE_CACHE, "false").build() }
        );
    }

    @Override
    protected boolean useRandomReplicationStrategy() {
        return true;
    }

    // One of the primary purposes of the query cache is to cache aggs results
    public void testCacheAggs() throws Exception {
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("f", "type=date")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(
            true,
            client.prepareIndex(index).setSource("f", "2014-03-10T00:00:00.000Z"),
            client.prepareIndex(index).setSource("f", "2014-05-13T00:00:00.000Z")
        );
        ensureSearchable(index);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        // This is not a random example: serialization with time zones writes shared strings
        // which used to not work well with the query cache because of the handles stream output
        // see #9500
        final SearchResponse r1 = client.prepareSearch(index)
            .setSize(0)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(
                dateHistogram("histo").field("f")
                    .timeZone(ZoneId.of("+01:00"))
                    .minDocCount(0)
                    .dateHistogramInterval(DateHistogramInterval.MONTH)
            )
            .get();
        assertSearchResponse(r1);

        // The cached is actually used
        assertThat(
            client.admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            greaterThan(0L)
        );

        for (int i = 0; i < 10; ++i) {
            final SearchResponse r2 = client.prepareSearch(index)
                .setSize(0)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addAggregation(
                    dateHistogram("histo").field("f")
                        .timeZone(ZoneId.of("+01:00"))
                        .minDocCount(0)
                        .dateHistogramInterval(DateHistogramInterval.MONTH)
                )
                .get();
            assertSearchResponse(r2);
            Histogram h1 = r1.getAggregations().get("histo");
            Histogram h2 = r2.getAggregations().get("histo");
            final List<? extends Bucket> buckets1 = h1.getBuckets();
            final List<? extends Bucket> buckets2 = h2.getBuckets();
            assertEquals(buckets1.size(), buckets2.size());
            for (int j = 0; j < buckets1.size(); ++j) {
                final Bucket b1 = buckets1.get(j);
                final Bucket b2 = buckets2.get(j);
                assertEquals(b1.getKey(), b2.getKey());
                assertEquals(b1.getDocCount(), b2.getDocCount());
            }
        }
    }

    public void testQueryRewrite() throws Exception {
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("s", "type=date")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                        .put("index.number_of_routing_shards", 5)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(
            true,
            client.prepareIndex(index).setId("1").setRouting("1").setSource("s", "2016-03-19"),
            client.prepareIndex(index).setId("2").setRouting("1").setSource("s", "2016-03-20"),
            client.prepareIndex(index).setId("3").setRouting("1").setSource("s", "2016-03-21"),
            client.prepareIndex(index).setId("4").setRouting("2").setSource("s", "2016-03-22"),
            client.prepareIndex(index).setId("5").setRouting("2").setSource("s", "2016-03-23"),
            client.prepareIndex(index).setId("6").setRouting("2").setSource("s", "2016-03-24"),
            client.prepareIndex(index).setId("7").setRouting("3").setSource("s", "2016-03-25"),
            client.prepareIndex(index).setId("8").setRouting("3").setSource("s", "2016-03-26"),
            client.prepareIndex(index).setId("9").setRouting("3").setSource("s", "2016-03-27")
        );
        ensureSearchable(index);
        assertCacheState(client, index, 0, 0);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        assertCacheState(client, index, 0, 0);

        final SearchResponse r1 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-25"))
            // to ensure that query is executed even if it rewrites to match_no_docs
            .addAggregation(new GlobalAggregationBuilder("global"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 5);

        final SearchResponse r2 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-20").lte("2016-03-26"))
            .addAggregation(new GlobalAggregationBuilder("global"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r2);
        assertThat(r2.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 3, 7);

        final SearchResponse r3 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-21").lte("2016-03-27"))
            .addAggregation(new GlobalAggregationBuilder("global"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r3);
        assertThat(r3.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 6, 9);
    }

    public void testQueryRewriteMissingValues() throws Exception {
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("s", "type=date")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(
            true,
            client.prepareIndex(index).setId("1").setSource("s", "2016-03-19"),
            client.prepareIndex(index).setId("2").setSource("s", "2016-03-20"),
            client.prepareIndex(index).setId("3").setSource("s", "2016-03-21"),
            client.prepareIndex(index).setId("4").setSource("s", "2016-03-22"),
            client.prepareIndex(index).setId("5").setSource("s", "2016-03-23"),
            client.prepareIndex(index).setId("6").setSource("s", "2016-03-24"),
            client.prepareIndex(index).setId("7").setSource("other", "value"),
            client.prepareIndex(index).setId("8").setSource("s", "2016-03-26"),
            client.prepareIndex(index).setId("9").setSource("s", "2016-03-27")
        );
        ensureSearchable(index);
        assertCacheState(client, index, 0, 0);

        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        assertCacheState(client, index, 0, 0);

        final SearchResponse r1 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-28"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(8L));
        assertCacheState(client, index, 0, 1);

        final SearchResponse r2 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-28"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r2);
        assertThat(r2.getHits().getTotalHits().value, equalTo(8L));
        assertCacheState(client, index, 1, 1);

        final SearchResponse r3 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-28"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r3);
        assertThat(r3.getHits().getTotalHits().value, equalTo(8L));
        assertCacheState(client, index, 2, 1);
    }

    public void testQueryRewriteDates() throws Exception {
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("d", "type=date")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(
            true,
            client.prepareIndex(index).setId("1").setSource("d", "2014-01-01T00:00:00"),
            client.prepareIndex(index).setId("2").setSource("d", "2014-02-01T00:00:00"),
            client.prepareIndex(index).setId("3").setSource("d", "2014-03-01T00:00:00"),
            client.prepareIndex(index).setId("4").setSource("d", "2014-04-01T00:00:00"),
            client.prepareIndex(index).setId("5").setSource("d", "2014-05-01T00:00:00"),
            client.prepareIndex(index).setId("6").setSource("d", "2014-06-01T00:00:00"),
            client.prepareIndex(index).setId("7").setSource("d", "2014-07-01T00:00:00"),
            client.prepareIndex(index).setId("8").setSource("d", "2014-08-01T00:00:00"),
            client.prepareIndex(index).setId("9").setSource("d", "2014-09-01T00:00:00")
        );
        ensureSearchable(index);
        assertCacheState(client, index, 0, 0);

        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        assertCacheState(client, index, 0, 0);

        final SearchResponse r1 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("d").gte("2013-01-01T00:00:00").lte("now"))
            // to ensure that query is executed even if it rewrites to match_no_docs
            .addAggregation(new GlobalAggregationBuilder("global"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(9L));
        assertCacheState(client, index, 0, 1);

        final SearchResponse r2 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("d").gte("2013-01-01T00:00:00").lte("now"))
            .addAggregation(new GlobalAggregationBuilder("global"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r2);
        assertThat(r2.getHits().getTotalHits().value, equalTo(9L));
        assertCacheState(client, index, 1, 1);

        final SearchResponse r3 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("d").gte("2013-01-01T00:00:00").lte("now"))
            .addAggregation(new GlobalAggregationBuilder("global"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r3);
        assertThat(r3.getHits().getTotalHits().value, equalTo(9L));
        assertCacheState(client, index, 2, 1);
    }

    public void testQueryRewriteDatesWithNow() throws Exception {
        Client client = client();
        Settings settings = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            // Disable index refreshing to avoid cache being invalidated mid-test
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
            .build();
        assertAcked(client.admin().indices().prepareCreate("index-1").setMapping("d", "type=date").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareCreate("index-2").setMapping("d", "type=date").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareCreate("index-3").setMapping("d", "type=date").setSettings(settings).get());
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        DateFormatter formatter = DateFormatter.forPattern("strict_date_optional_time");
        indexRandom(
            true,
            client.prepareIndex("index-1").setId("1").setSource("d", formatter.format(now)),
            client.prepareIndex("index-1").setId("2").setSource("d", formatter.format(now.minusDays(1))),
            client.prepareIndex("index-1").setId("3").setSource("d", formatter.format(now.minusDays(2))),
            client.prepareIndex("index-2").setId("4").setSource("d", formatter.format(now.minusDays(3))),
            client.prepareIndex("index-2").setId("5").setSource("d", formatter.format(now.minusDays(4))),
            client.prepareIndex("index-2").setId("6").setSource("d", formatter.format(now.minusDays(5))),
            client.prepareIndex("index-3").setId("7").setSource("d", formatter.format(now.minusDays(6))),
            client.prepareIndex("index-3").setId("8").setSource("d", formatter.format(now.minusDays(7))),
            client.prepareIndex("index-3").setId("9").setSource("d", formatter.format(now.minusDays(8)))
        );
        ensureSearchable("index-1", "index-2", "index-3");
        assertCacheState(client, "index-1", 0, 0);
        assertCacheState(client, "index-2", 0, 0);
        assertCacheState(client, "index-3", 0, 0);

        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin()
            .indices()
            .prepareForceMerge("index-1", "index-2", "index-3")
            .setFlush(true)
            .get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        refreshAndWaitForReplication();
        ensureSearchable("index-1", "index-2", "index-3");

        assertCacheState(client, "index-1", 0, 0);
        assertCacheState(client, "index-2", 0, 0);
        assertCacheState(client, "index-3", 0, 0);

        final SearchResponse r1 = client.prepareSearch("index-*")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("d").gte("now-7d/d").lte("now"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(8L));
        assertCacheState(client, "index-1", 0, 1);
        assertCacheState(client, "index-2", 0, 1);
        // Because the query will INTERSECT with the 3rd index it will not be
        // rewritten and will still contain `now` so won't be recorded as a
        // cache miss or cache hit since queries containing now can't be cached
        assertCacheState(client, "index-3", 0, 0);

        final SearchResponse r2 = client.prepareSearch("index-*")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("d").gte("now-7d/d").lte("now"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r2);
        assertThat(r2.getHits().getTotalHits().value, equalTo(8L));
        assertCacheState(client, "index-1", 1, 1);
        assertCacheState(client, "index-2", 1, 1);
        assertCacheState(client, "index-3", 0, 0);

        final SearchResponse r3 = client.prepareSearch("index-*")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("d").gte("now-7d/d").lte("now"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r3);
        assertThat(r3.getHits().getTotalHits().value, equalTo(8L));
        assertCacheState(client, "index-1", 2, 1);
        assertCacheState(client, "index-2", 2, 1);
        assertCacheState(client, "index-3", 0, 0);
    }

    public void testCanCache() throws Exception {
        Client client = client();
        Settings settings = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put("index.number_of_routing_shards", 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
            .build();
        String index = "index";
        assertAcked(client.admin().indices().prepareCreate(index).setMapping("s", "type=date").setSettings(settings).get());
        indexRandom(
            true,
            client.prepareIndex(index).setId("1").setRouting("1").setSource("s", "2016-03-19"),
            client.prepareIndex(index).setId("2").setRouting("1").setSource("s", "2016-03-20"),
            client.prepareIndex(index).setId("3").setRouting("1").setSource("s", "2016-03-21"),
            client.prepareIndex(index).setId("4").setRouting("2").setSource("s", "2016-03-22"),
            client.prepareIndex(index).setId("5").setRouting("2").setSource("s", "2016-03-23"),
            client.prepareIndex(index).setId("6").setRouting("2").setSource("s", "2016-03-24"),
            client.prepareIndex(index).setId("7").setRouting("3").setSource("s", "2016-03-25"),
            client.prepareIndex(index).setId("8").setRouting("3").setSource("s", "2016-03-26"),
            client.prepareIndex(index).setId("9").setRouting("3").setSource("s", "2016-03-27")
        );
        ensureSearchable(index);
        assertCacheState(client, index, 0, 0);

        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        assertCacheState(client, index, 0, 0);

        // If size > 0 we should no cache by default
        final SearchResponse r1 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(1)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-19").lte("2016-03-25"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 0);

        // If search type is DFS_QUERY_THEN_FETCH we should not cache
        final SearchResponse r2 = client.prepareSearch(index)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-20").lte("2016-03-26"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r2);
        assertThat(r2.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 0);

        // If search type is DFS_QUERY_THEN_FETCH we should not cache even if
        // the cache flag is explicitly set on the request
        final SearchResponse r3 = client.prepareSearch(index)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setSize(0)
            .setRequestCache(true)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-20").lte("2016-03-26"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r3);
        assertThat(r3.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 0);

        // If the request has an non-filter aggregation containing now we should not cache
        final SearchResponse r5 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setRequestCache(true)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-20").lte("2016-03-26"))
            .addAggregation(dateRange("foo").field("s").addRange("now-10y", "now"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r5);
        assertThat(r5.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 0);

        // If size > 1 and cache flag is set on the request we should cache
        final SearchResponse r6 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(1)
            .setRequestCache(true)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-21").lte("2016-03-27"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r6);
        assertThat(r6.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 2);

        // If the request has a filter aggregation containing now we should cache since it gets rewritten
        final SearchResponse r4 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setRequestCache(true)
            .setQuery(QueryBuilders.rangeQuery("s").gte("2016-03-20").lte("2016-03-26"))
            .addAggregation(filter("foo", QueryBuilders.rangeQuery("s").from("now-10y").to("now")))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r4);
        assertThat(r4.getHits().getTotalHits().value, equalTo(7L));
        assertCacheState(client, index, 0, 4);
    }

    public void testCacheWithFilteredAlias() throws InterruptedException {
        Client client = client();
        Settings settings = Settings.builder()
            .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("created_at", "type=date")
                .setSettings(settings)
                .addAlias(new Alias("last_week").filter(QueryBuilders.rangeQuery("created_at").gte("now-7d/d")))
                .get()
        );
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        client.prepareIndex(index).setId("1").setRouting("1").setSource("created_at", DateTimeFormatter.ISO_LOCAL_DATE.format(now)).get();
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge(index).setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        refreshAndWaitForReplication();

        indexRandomForConcurrentSearch(index);

        assertCacheState(client, index, 0, 0);

        SearchResponse r1 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("created_at").gte("now-7d/d"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(1L));
        assertCacheState(client, index, 0, 1);

        r1 = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("created_at").gte("now-7d/d"))
            .get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(1L));
        assertCacheState(client, index, 1, 1);

        r1 = client.prepareSearch("last_week").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0).get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(1L));
        assertCacheState(client, index, 1, 2);

        r1 = client.prepareSearch("last_week").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0).get();
        OpenSearchAssertions.assertAllSuccessful(r1);
        assertThat(r1.getHits().getTotalHits().value, equalTo(1L));
        assertCacheState(client, index, 2, 2);
    }

    public void testProfileDisableCache() throws Exception {
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(true, client.prepareIndex(index).setSource("k", "hello"));
        ensureSearchable(index);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        int expectedHits = 0;
        int expectedMisses = 0;
        for (int i = 0; i < 5; i++) {
            boolean profile = i % 2 == 0;
            SearchResponse resp = client.prepareSearch(index)
                .setRequestCache(true)
                .setProfile(profile)
                .setQuery(QueryBuilders.termQuery("k", "hello"))
                .get();
            assertSearchResponse(resp);
            OpenSearchAssertions.assertAllSuccessful(resp);
            assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
            if (profile == false) {
                if (i == 1) {
                    expectedMisses++;
                } else {
                    expectedHits++;
                }
            }
            assertCacheState(client, index, expectedHits, expectedMisses);
        }
    }
  
    public void testDeleteAndCreateSameIndexShardOnSameNode() throws Exception {
        String node_1 = internalCluster().startNode(Settings.builder().build());
        Client client = client(node_1);

        logger.info("Starting a node in the cluster");

        assertThat(cluster().size(), equalTo(1));
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("1").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        String indexName = "test";

        logger.info("Creating an index: {} with 2 shards", indexName);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                // Disable index refreshing to avoid cache being invalidated mid-test
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                .build()
        );

        ensureGreen(indexName);

        logger.info("Writing few docs and searching those which will cache items in RequestCache");
        indexRandom(true, client.prepareIndex(indexName).setSource("k", "hello"));
        indexRandom(true, client.prepareIndex(indexName).setSource("y", "hello again"));
        ensureSearchable(indexName);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, indexName);
        SearchResponse resp = client.prepareSearch(indexName).setRequestCache(true).setQuery(QueryBuilders.termQuery("k", "hello")).get();
        assertSearchResponse(resp);
        resp = client.prepareSearch(indexName).setRequestCache(true).setQuery(QueryBuilders.termQuery("y", "hello")).get();

        RequestCacheStats stats = getNodeCacheStats(client);
        assertTrue(stats.getMemorySizeInBytes() > 0);

        logger.info("Disabling allocation");
        Settings newSettings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE.name())
            .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(newSettings).execute().actionGet();

        logger.info("Starting a second node");
        String node_2 = internalCluster().startDataOnlyNode(Settings.builder().build());
        assertThat(cluster().size(), equalTo(2));
        healthResponse = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("Moving the shard:{} from node:{} to node:{}", indexName + "#0", node_1, node_2);
        MoveAllocationCommand cmd = new MoveAllocationCommand(indexName, 0, node_1, node_2);
        internalCluster().client().admin().cluster().prepareReroute().add(cmd).get();
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Index index = state.metadata().index(indexName).getIndex();

        assertBusy(() -> {
            assertFalse(Arrays.stream(shardDirectory(node_1, index, 0)).anyMatch(Files::exists));
            assertEquals(1, Arrays.stream(shardDirectory(node_2, index, 0)).filter(Files::exists).count());
        });

        logger.info("Moving the shard: {} again from node:{} to node:{}", indexName + "#0", node_2, node_1);
        cmd = new MoveAllocationCommand(indexName, 0, node_2, node_1);
        internalCluster().client().admin().cluster().prepareReroute().add(cmd).get();
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertBusy(() -> {
            assertEquals(1, Arrays.stream(shardDirectory(node_1, index, 0)).filter(Files::exists).count());
            assertFalse(Arrays.stream(shardDirectory(node_2, index, 0)).anyMatch(Files::exists));
        });

        logger.info("Clearing the cache for index:{}. And verify the request stats doesn't go negative", indexName);
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(indexName);
        client.admin().indices().clearCache(clearIndicesCacheRequest).actionGet();

        stats = getNodeCacheStats(client(node_1));
        assertTrue(stats.getMemorySizeInBytes() == 0);
        stats = getNodeCacheStats(client(node_2));
        assertTrue(stats.getMemorySizeInBytes() == 0);
    }

    public void testTimedOutQuery() throws Exception {
        // A timed out query should be cached and then invalidated
        Client client = client();
        String index = "index";
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setMapping("k", "type=keyword")
                .setSettings(
                    Settings.builder()
                        .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disable index refreshing to avoid cache being invalidated mid-test
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(-1))
                )
                .get()
        );
        indexRandom(true, client.prepareIndex(index).setSource("k", "hello"));
        ensureSearchable(index);
        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        forceMerge(client, index);

        QueryBuilder timeoutQueryBuilder = new TermQueryBuilder("k", "hello") {
            @Override
            protected Query doToQuery(QueryShardContext context) {
                return new TermQuery(new Term("k", "hello")) {
                    @Override
                    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                        // Create the weight before sleeping. Otherwise, TermStates.build() (in the call to super.createWeight()) will
                        // sometimes throw an exception on timeout, rather than timing out gracefully.
                        Weight result = super.createWeight(searcher, scoreMode, boost);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ignored) {}
                        return result;
                    }
                };
            }
        };

        SearchResponse resp = client.prepareSearch(index)
            .setRequestCache(true)
            .setQuery(timeoutQueryBuilder)
            .setTimeout(TimeValue.ZERO)
            .get();
        assertTrue(resp.isTimedOut());
        RequestCacheStats requestCacheStats = getRequestCacheStats(client, index);
        // The cache should be empty as the timed-out query was invalidated
        assertEquals(0, requestCacheStats.getMemorySizeInBytes());
    }

    private Path[] shardDirectory(String server, Index index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.availableShardPaths(new ShardId(index, shard));
        // the available paths of the shard may be bigger than the 1,
        // it depends on `InternalTestCluster.numDataPaths`.
        return paths;
    }

    private void forceMerge(Client client, String index) {
        ForceMergeResponse forceMergeResponse = client.admin().indices().prepareForceMerge(index).setFlush(true).get();
        OpenSearchAssertions.assertAllSuccessful(forceMergeResponse);
        refreshAndWaitForReplication();
    }

    private static void assertCacheState(Client client, String index, long expectedHits, long expectedMisses) {
        RequestCacheStats requestCacheStats = getRequestCacheStats(client, index);
        // Check the hit count and miss count together so if they are not
        // correct we can see both values
        assertEquals(
            Arrays.asList(expectedHits, expectedMisses, 0L),
            Arrays.asList(requestCacheStats.getHitCount(), requestCacheStats.getMissCount(), requestCacheStats.getEvictions())
        );

    }

    private static RequestCacheStats getRequestCacheStats(Client client, String index) {
        return client.admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache();
    }

    private static RequestCacheStats getNodeCacheStats(Client client) {
        NodesStatsResponse stats = client.admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : stats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                return stat.getIndices().getRequestCache();
            }
        }
        return null;
    }
}