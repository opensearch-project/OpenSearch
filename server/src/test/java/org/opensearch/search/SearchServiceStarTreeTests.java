/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.apache.lucene.util.FixedBitSet;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexService;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.startree.DateHistogramAggregatorTests;
import org.opensearch.search.aggregations.startree.NumericTermsAggregatorTests;
import org.opensearch.search.aggregations.startree.StarTreeFilterTests;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.startree.StarTreeQueryContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.medianAbsoluteDeviation;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();
        CreateIndexRequestBuilder builder = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(settings)
            .setMapping(
                StarTreeFilterTests.getExpandedMapping(
                    1,
                    false,
                    StarTreeFilterTests.DIMENSION_TYPE_MAP,
                    StarTreeFilterTests.METRIC_TYPE_MAP
                )
            );
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

        QueryBuilder baseQuery;
        SearchContext searchContext = createSearchContext(indexService);
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            1,
            Collections.emptySet(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );

        // Case 1: No query or aggregations, should not use star tree
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 2: MatchAllQuery present but no aggregations, should not use star tree
        sourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 3: MatchAllQuery and aggregations present, should use star tree
        baseQuery = new MatchAllQueryBuilder();
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(AggregationBuilders.max("test").field("field"));
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree",
                -1,
                List.of(new NumericDimension(FIELD_NAME)),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 4: MatchAllQuery and metric aggregations present, but postFilter specified, should not use star tree
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new MatchAllQueryBuilder())
            .aggregation(max("test").field("field"))
            .postFilter(new MatchAllQueryBuilder());
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 5: TermQuery and single aggregation, should use star tree, but not initialize query cache
        baseQuery = new TermQueryBuilder("sndv", 1);
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(AggregationBuilders.max("test").field("field"));
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree",
                -1,
                List.of(new OrdinalDimension("sndv")),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 6: TermQuery and multiple aggregations present, should use star tree & initialize cache
        baseQuery = new TermQueryBuilder("sndv", 1);
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(baseQuery)
            .aggregation(AggregationBuilders.max("test").field("field"))
            .aggregation(AggregationBuilders.sum("test2").field("field"));
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree",
                -1,
                List.of(new OrdinalDimension("sndv")),
                List.of(new Metric("field", List.of(MetricStat.MAX, MetricStat.SUM))),
                baseQuery,
                sourceBuilder,
                true
            ),
            0
        );

        // Case 7: No query, metric aggregations present, should use star tree
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.max("test").field("field"));
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree",
                -1,
                List.of(new OrdinalDimension("sndv")),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                null,
                sourceBuilder,
                true
            ),
            -1
        );

        setStarTreeIndexSetting(null);
        searchContext.close();
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
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
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

        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            1,
            Collections.emptySet(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );

        QueryBuilder baseQuery;
        SearchContext searchContext = createSearchContext(indexService);
        // Case 1: No query or aggregations, should not use star tree
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 2: MatchAllQuery present but no aggregations, should not use star tree
        baseQuery = new MatchAllQueryBuilder();
        sourceBuilder = new SearchSourceBuilder().query(baseQuery);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 3: MatchAllQuery and non-nested metric aggregations is nested within date-histogram aggregation, should use star tree
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub);
        baseQuery = new MatchAllQueryBuilder();
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(dateHistogramAggregationBuilder);
        CompositeIndexFieldInfo expectedStarTree = new CompositeIndexFieldInfo(
            "startree1",
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE
        );
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(
                    new DateDimension(
                        TIMESTAMP_FIELD,
                        List.of(new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)),
                        DateFieldMapper.Resolution.MILLISECONDS
                    ),
                    new NumericDimension(FIELD_NAME)
                ),
                List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 4: MatchAllQuery and nested-metric aggregations is nested within date-histogram aggregation, should not use star tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(sumAggSub);
        baseQuery = new MatchAllQueryBuilder();
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 5: MatchAllQuery and non-startree supported aggregation nested within date-histogram aggregation, should not use star tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(medianAgg);
        baseQuery = new MatchAllQueryBuilder();
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 6: NumericTermQuery and date-histogram aggregation present, should use star tree
        dateHistogramAggregationBuilder = dateHistogram("by_day").field(TIMESTAMP_FIELD)
            .calendarInterval(DateHistogramInterval.DAY)
            .subAggregation(maxAggNoSub);
        baseQuery = new TermQueryBuilder(FIELD_NAME, 1L);
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(
                    new DateDimension(
                        TIMESTAMP_FIELD,
                        List.of(new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)),
                        DateFieldMapper.Resolution.MILLISECONDS
                    ),
                    new NumericDimension(FIELD_NAME)
                ),
                List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

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
        baseQuery = null;
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(dateHistogramAggregationBuilder);
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(
                    new DateDimension(
                        TIMESTAMP_FIELD,
                        List.of(new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)),
                        DateFieldMapper.Resolution.MILLISECONDS
                    ),
                    new NumericDimension(FIELD_NAME)
                ),
                List.of(
                    new Metric(TIMESTAMP_FIELD, List.of(MetricStat.SUM, MetricStat.MAX)),
                    new Metric(FIELD_NAME, List.of(MetricStat.MAX, MetricStat.SUM))
                ),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        setStarTreeIndexSetting(null);
    }

    public void testCacheCreationInStarTreeQueryContext() throws IOException {
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            1,
            Collections.emptySet(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(new OrdinalDimension("field")),
                List.of(new Metric("metricField", List.of(MetricStat.SUM, MetricStat.MAX))),
                starTreeFieldConfiguration
            )
        );

        QueryBuilder baseQuery = new MatchAllQueryBuilder();
        SearchContext searchContext = mock(SearchContext.class);
        MapperService mapperService = mock(MapperService.class);
        IndexShard indexShard = mock(IndexShard.class);
        Segment segment = mock(Segment.class);
        SearchContextAggregations searchContextAggregations = mock(SearchContextAggregations.class);
        AggregatorFactories aggregatorFactories = mock(AggregatorFactories.class);

        when(mapperService.getCompositeFieldTypes()).thenReturn(Set.of(compositeDataCubeFieldType));
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(indexShard.segments(false)).thenReturn(List.of(segment, segment));
        when(searchContext.aggregations()).thenReturn(searchContextAggregations);
        when(searchContextAggregations.factories()).thenReturn(aggregatorFactories);
        when(aggregatorFactories.getFactories()).thenReturn(new AggregatorFactory[] { null, null });
        StarTreeQueryContext starTreeQueryContext = new StarTreeQueryContext(searchContext, baseQuery);

        assertEquals(2, starTreeQueryContext.getAllCachedValues().length);

        // Asserting null values are ignored
        when(aggregatorFactories.getFactories()).thenReturn(new AggregatorFactory[] {});
        starTreeQueryContext = new StarTreeQueryContext(searchContext, baseQuery);
        starTreeQueryContext.maybeSetCachedNodeIdsForSegment(-1, null);
        assertNull(starTreeQueryContext.getAllCachedValues());
        assertNull(starTreeQueryContext.maybeGetCachedNodeIdsForSegment(0));

        // Assert correct cached value is returned
        when(aggregatorFactories.getFactories()).thenReturn(new AggregatorFactory[] { null, null });
        starTreeQueryContext = new StarTreeQueryContext(searchContext, baseQuery);
        FixedBitSet cachedValues = new FixedBitSet(22);
        starTreeQueryContext.maybeSetCachedNodeIdsForSegment(0, cachedValues);
        assertEquals(2, starTreeQueryContext.getAllCachedValues().length);
        assertEquals(22, starTreeQueryContext.maybeGetCachedNodeIdsForSegment(0).length());

        starTreeQueryContext = new StarTreeQueryContext(compositeDataCubeFieldType, new MatchAllQueryBuilder(), 2);
        assertEquals(2, starTreeQueryContext.getAllCachedValues().length);

        mapperService.close();
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
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();
        CreateIndexRequestBuilder builder = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(settings)
            .setMapping(
                StarTreeFilterTests.getExpandedMapping(
                    1,
                    false,
                    StarTreeFilterTests.DIMENSION_TYPE_MAP,
                    StarTreeFilterTests.METRIC_TYPE_MAP
                )
            );
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
        assertStarTreeContext(request, sourceBuilder, null, -1);

        setStarTreeIndexSetting(null);
    }

    /**
     * Test query parsing for bucket aggregations, with/without numeric term query
     */
    public void testQueryParsingForBucketAggregations() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
        setStarTreeIndexSetting("true");

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();
        CreateIndexRequestBuilder builder = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(settings)
            .setMapping(NumericTermsAggregatorTests.getExpandedMapping(1, false));
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
        String KEYWORD_FIELD = "clientip";
        String NUMERIC_FIELD = "size";

        MaxAggregationBuilder maxAggNoSub = max("max").field(FIELD_NAME);
        MaxAggregationBuilder sumAggNoSub = max("sum").field(FIELD_NAME);
        SumAggregationBuilder sumAggSub = sum("sum").field(FIELD_NAME).subAggregation(maxAggNoSub);
        MedianAbsoluteDeviationAggregationBuilder medianAgg = medianAbsoluteDeviation("median").field(FIELD_NAME);

        QueryBuilder baseQuery;
        SearchContext searchContext = createSearchContext(indexService);
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            1,
            Collections.emptySet(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );

        // Case 1: MatchAllQuery and non-nested metric aggregations is nested within keyword term aggregation, should use star tree
        TermsAggregationBuilder termsAggregationBuilder = terms("term").field(KEYWORD_FIELD).subAggregation(maxAggNoSub);
        baseQuery = new MatchAllQueryBuilder();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(termsAggregationBuilder);

        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(new NumericDimension(NUMERIC_FIELD), new OrdinalDimension(KEYWORD_FIELD)),
                List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 2: MatchAllQuery and non-nested metric aggregations is nested within numeric term aggregation, should use star tree
        termsAggregationBuilder = terms("term").field(NUMERIC_FIELD).subAggregation(maxAggNoSub);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new MatchAllQueryBuilder()).aggregation(termsAggregationBuilder);
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(new NumericDimension(NUMERIC_FIELD), new OrdinalDimension(KEYWORD_FIELD)),
                List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 3: NumericTermsQuery and non-nested metric aggregations is nested within keyword term aggregation, should use star tree
        termsAggregationBuilder = terms("term").field(KEYWORD_FIELD).subAggregation(maxAggNoSub);
        baseQuery = new TermQueryBuilder(FIELD_NAME, 1);
        sourceBuilder = new SearchSourceBuilder().size(0).query(baseQuery).aggregation(termsAggregationBuilder);
        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(new NumericDimension(NUMERIC_FIELD), new OrdinalDimension(KEYWORD_FIELD), new NumericDimension(FIELD_NAME)),
                List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 4: NumericTermsQuery and multiple non-nested metric aggregations is within numeric term aggregation, should use star tree
        termsAggregationBuilder = terms("term").field(NUMERIC_FIELD).subAggregation(maxAggNoSub).subAggregation(sumAggNoSub);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new TermQueryBuilder(FIELD_NAME, 1)).aggregation(termsAggregationBuilder);

        assertStarTreeContext(
            request,
            sourceBuilder,
            getStarTreeQueryContext(
                searchContext,
                starTreeFieldConfiguration,
                "startree1",
                -1,
                List.of(new NumericDimension(NUMERIC_FIELD), new OrdinalDimension(KEYWORD_FIELD), new NumericDimension(FIELD_NAME)),
                List.of(new Metric(FIELD_NAME, List.of(MetricStat.SUM, MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 5: Nested metric aggregations is nested within numeric term aggregation, should not use star tree
        termsAggregationBuilder = terms("term").field(NUMERIC_FIELD).subAggregation(sumAggSub);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new TermQueryBuilder(FIELD_NAME, 1)).aggregation(termsAggregationBuilder);
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 6: Unsupported aggregations is nested within numeric term aggregation, should not use star tree
        termsAggregationBuilder = terms("term").field(NUMERIC_FIELD).subAggregation(medianAgg);
        sourceBuilder = new SearchSourceBuilder().size(0).query(new TermQueryBuilder(FIELD_NAME, 1)).aggregation(termsAggregationBuilder);
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
            StarTreeQueryContext actualContext = context.getQueryShardContext().getStarTreeQueryContext();

            if (expectedContext == null) {
                assertThat(context.getQueryShardContext().getStarTreeQueryContext(), nullValue());
            } else {
                assertThat(actualContext, notNullValue());
                assertEquals(expectedContext.getStarTree().getType(), actualContext.getStarTree().getType());
                assertEquals(expectedContext.getStarTree().getField(), actualContext.getStarTree().getField());
                assertEquals(
                    expectedContext.getBaseQueryStarTreeFilter().getDimensions(),
                    actualContext.getBaseQueryStarTreeFilter().getDimensions()
                );
                if (expectedCacheUsage > -1) {
                    assertEquals(expectedCacheUsage, actualContext.getAllCachedValues().length);
                } else {
                    assertNull(actualContext.getAllCachedValues());
                }
            }
            searchService.doStop();
        }
    }

    private StarTreeQueryContext getStarTreeQueryContext(
        SearchContext searchContext,
        StarTreeFieldConfiguration starTreeFieldConfiguration,
        String compositeFieldName,
        int cacheSize,
        List<Dimension> dimensions,
        List<Metric> metrics,
        QueryBuilder baseQuery,
        SearchSourceBuilder sourceBuilder,
        boolean assertConsolidation
    ) {
        AggregatorFactories aggregatorFactories = mock(AggregatorFactories.class);
        AggregatorFactory[] aggregatorFactoriesArray = sourceBuilder.aggregations().getAggregatorFactories().stream().map(af -> {
            try {
                return ((ValuesSourceAggregationBuilder<?>) af).build(searchContext.getQueryShardContext(), null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toArray(AggregatorFactory[]::new);
        when(aggregatorFactories.getFactories()).thenReturn(aggregatorFactoriesArray);
        SearchContextAggregations mockAggregations = mock(SearchContextAggregations.class);
        when(mockAggregations.factories()).thenReturn(aggregatorFactories);
        searchContext.aggregations(mockAggregations);
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            compositeFieldName,
            new StarTreeField(compositeFieldName, dimensions, metrics, starTreeFieldConfiguration)
        );
        StarTreeQueryContext starTreeQueryContext = new StarTreeQueryContext(compositeDataCubeFieldType, baseQuery, cacheSize);
        boolean consolidated = starTreeQueryContext.consolidateAllFilters(searchContext);
        if (assertConsolidation) {
            assertTrue(consolidated);
            searchContext.getQueryShardContext().setStarTreeQueryContext(starTreeQueryContext);
        }
        return starTreeQueryContext;
    }
}
