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
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
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
import org.opensearch.search.aggregations.startree.StarTreeQueryTests;
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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchServiceStarTreeTests extends OpenSearchSingleNodeTestCase {

    public void testParseQueryToOriginalOrStarTreeQuery() throws IOException {
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
            .setMapping(StarTreeQueryTests.getExpandedMapping(1, false));
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
                List.of(),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                baseQuery,
                sourceBuilder,
                true
            ),
            -1
        );

        // Case 4: MatchAllQuery and aggregations present, but postFilter specified, should not use star tree
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new MatchAllQueryBuilder())
            .aggregation(AggregationBuilders.max("test").field("field"))
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

    private void setStarTreeIndexSetting(String value) throws IOException {
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
                assertEquals(expectedContext.getBaseQueryStarTreeFilter(), actualContext.getBaseQueryStarTreeFilter());
                if (expectedCacheUsage > -1) {
                    assertEquals(expectedCacheUsage, actualContext.getStarTreeValues().length);
                } else {
                    assertNull(actualContext.getStarTreeValues());
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
        }
        return starTreeQueryContext;
    }
}
