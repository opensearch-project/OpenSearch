/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexService;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.startree.DateHistogramAggregatorTests;
import org.opensearch.search.aggregations.startree.StarTreeFilterTests;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.startree.StarTreeQueryContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.medianAbsoluteDeviation;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * Tests for validating query shapes which can be resolved using star-tree index
 * For valid resolvable (with star-tree) cases, StarTreeQueryContext is created and populated with the SearchContext
 * For non-resolvable (with star-tree) cases, StarTreeQueryContext is null
 */
public class SearchServiceStarTreeTests extends OpenSearchSingleNodeTestCase {

    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String FIELD_NAME = "status";

    /**
     * Test query parsing for non-nested metric aggregations, with/without numeric term query
     */
    public void testQueryParsingForMetricAggregations() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
        setStarTreeIndexSetting("true");

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();
        CreateIndexRequestBuilder builder = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(settings)
            .setMapping(StarTreeFilterTests.getExpandedMapping(1, false));
        createIndex("test", builder);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("test"));
        IndexShard indexShard = indexService.getShard(0);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        // Case 1: No query or aggregations, should not use star tree
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 2: MatchAllQuery present but no aggregations, should not use star tree
        sourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 3: MatchAllQuery and metric aggregations present, should use star tree
        sourceBuilder = new SearchSourceBuilder().size(0).query(new MatchAllQueryBuilder()).aggregation(max("test").field("field"));
        CompositeIndexFieldInfo expectedStarTree = new CompositeIndexFieldInfo(
            "startree",
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE
        );
        Map<String, Long> expectedQueryMap = null;
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        // Case 4: MatchAllQuery and metric aggregations present, but postFilter specified, should not use star tree
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new MatchAllQueryBuilder())
            .aggregation(max("test").field("field"))
            .postFilter(new MatchAllQueryBuilder());
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 5: TermQuery and single metric aggregation, should use star tree, but not initialize query cache
        sourceBuilder = new SearchSourceBuilder().size(0).query(new TermQueryBuilder("sndv", 1)).aggregation(max("test").field("field"));
        expectedQueryMap = Map.of("sndv", 1L);
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        // Case 6: TermQuery and multiple metric aggregations present, should use star tree & initialize cache
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new TermQueryBuilder("sndv", 1))
            .aggregation(max("test").field("field"))
            .aggregation(AggregationBuilders.sum("test2").field("field"));
        expectedQueryMap = Map.of("sndv", 1L);
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, 0), 0);

        // Case 7: No query, metric aggregations present, should use star tree
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(max("test").field("field"));
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, null, -1), -1);

        setStarTreeIndexSetting(null);
    }

    /**
     * Test query parsing for date histogram aggregations, with/without numeric term query
     */
    public void testQueryParsingForDateHistogramAggregations() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
        setStarTreeIndexSetting("true");

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();
        CreateIndexRequestBuilder builder = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(settings)
            .setMapping(DateHistogramAggregatorTests.getExpandedMapping(1, false));
        createIndex("test", builder);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("test"));
        IndexShard indexShard = indexService.getShard(0);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        MaxAggregationBuilder maxAggNoSub = max("max").field(FIELD_NAME);
        MaxAggregationBuilder sumAggNoSub = max("sum").field(FIELD_NAME);
        SumAggregationBuilder sumAggSub = sum("sum").field(FIELD_NAME).subAggregation(maxAggNoSub);
        MedianAbsoluteDeviationAggregationBuilder medianAgg = medianAbsoluteDeviation("median").field(FIELD_NAME);

        // Case 1: No query or aggregations, should not use star tree
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 2: MatchAllQuery present but no aggregations, should not use star tree
        sourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 3: MatchAllQuery and non-nested metric aggregations is nested within date-histogram aggregation, should use star tree
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new MatchAllQueryBuilder()).aggregation(dateHistogramAggregationBuilder);
        CompositeIndexFieldInfo expectedStarTree = new CompositeIndexFieldInfo(
            "startree1",
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE
        );
        Map<String, Long> expectedQueryMap = null;
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        // Case 4: MatchAllQuery and nested-metric aggregations is nested within date-histogram aggregation, should not use star tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(sumAggSub);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new MatchAllQueryBuilder()).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 5: MatchAllQuery and non-startree supported aggregation nested within date-histogram aggregation, should not use star tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(medianAgg);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new MatchAllQueryBuilder()).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 6: NumericTermQuery and date-histogram aggregation present, should use star tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub);
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new TermQueryBuilder(FIELD_NAME, 1))
            .aggregation(dateHistogramAggregationBuilder);
        expectedQueryMap = Map.of(FIELD_NAME, 1L);
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        // Case 7: Date histogram with non calendar interval: rounding is null for DateHistogramFactory - cannot use star-tree
        dateHistogramAggregationBuilder = dateHistogram("non_cal").field(TIMESTAMP_FIELD)
            .fixedInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub);
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 8: Date histogram with no metric aggregation - does not use star-tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD).calendarInterval(DateHistogramInterval.DAY);
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 9: Date histogram with no valid time interval to resolve aggregation - should not use star-tree
        dateHistogramAggregationBuilder = dateHistogram("by_sec").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.SECOND)
            .subAggregation(maxAggNoSub);
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 10: Date histogram nested with multiple non-nested metric aggregations - should use star-tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub)
            .subAggregation(sumAggNoSub);
        expectedQueryMap = null;
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        setStarTreeIndexSetting(null);
    }

    /**
     * Test query parsing for date histogram aggregations on star-tree index when @timestamp field does not exist
     */
    public void testInvalidQueryParsingForDateHistogramAggregations() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
        setStarTreeIndexSetting("true");

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();
        CreateIndexRequestBuilder builder = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(settings)
            .setMapping(StarTreeFilterTests.getExpandedMapping(1, false));
        createIndex("test", builder);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("test"));
        IndexShard indexShard = indexService.getShard(0);
        ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            new SearchRequest().allowPartialSearchResults(true),
            indexShard.shardId(),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        MaxAggregationBuilder maxAggNoSub = max("max").field(FIELD_NAME);
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new MatchAllQueryBuilder())
            .aggregation(dateHistogramAggregationBuilder);
        CompositeIndexFieldInfo expectedStarTree = new CompositeIndexFieldInfo(
            "startree1",
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE
        );
        assertStarTreeContext(request, sourceBuilder, null, -1);

        setStarTreeIndexSetting(null);
    }

    private void setStarTreeIndexSetting(String value) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), value).build())
            .execute();
    }

    private void assertStarTreeContext(
        ShardSearchRequest request,
        SearchSourceBuilder sourceBuilder,
        StarTreeQueryContext expectedContext,
        int expectedCacheUsage
    ) throws IOException {
        request.source(sourceBuilder);
        SearchService searchService = getInstanceFromNode(SearchService.class);
        try (ReaderContext reader = searchService.createOrGetReaderContext(request, false)) {
            SearchContext context = searchService.createContext(reader, request, null, true);
            StarTreeQueryContext actualContext = context.getStarTreeQueryContext();

            if (expectedContext == null) {
                assertThat(context.getStarTreeQueryContext(), nullValue());
            } else {
                assertThat(actualContext, notNullValue());
                assertEquals(expectedContext.getStarTree().getType(), actualContext.getStarTree().getType());
                assertEquals(expectedContext.getStarTree().getField(), actualContext.getStarTree().getField());
                assertEquals(expectedContext.getQueryMap(), actualContext.getQueryMap());
                if (expectedCacheUsage > -1) {
                    assertEquals(expectedCacheUsage, actualContext.getStarTreeValues().length);
                } else {
                    assertNull(actualContext.getStarTreeValues());
                }
            }
            searchService.doStop();
        }
    }
}
