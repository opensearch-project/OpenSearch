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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

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

        // Case 3: MatchAllQuery and aggregations present, should use star tree
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new MatchAllQueryBuilder())
            .aggregation(AggregationBuilders.max("test").field("field"));
        CompositeIndexFieldInfo expectedStarTree = new CompositeIndexFieldInfo(
            "startree",
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE
        );
        Map<String, Long> expectedQueryMap = null;
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        // Case 4: MatchAllQuery and aggregations present, but postFilter specified, should not use star tree
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new MatchAllQueryBuilder())
            .aggregation(AggregationBuilders.max("test").field("field"))
            .postFilter(new MatchAllQueryBuilder());
        assertStarTreeContext(request, sourceBuilder, null, -1);

        // Case 5: TermQuery and single aggregation, should use star tree, but not initialize query cache
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new TermQueryBuilder("sndv", 1))
            .aggregation(AggregationBuilders.max("test").field("field"));
        expectedQueryMap = Map.of("sndv", 1L);
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, -1), -1);

        // Case 6: TermQuery and multiple aggregations present, should use star tree & initialize cache
        sourceBuilder = new SearchSourceBuilder().size(0)
            .query(new TermQueryBuilder("sndv", 1))
            .aggregation(AggregationBuilders.max("test").field("field"))
            .aggregation(AggregationBuilders.sum("test2").field("field"));
        expectedQueryMap = Map.of("sndv", 1L);
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, expectedQueryMap, 0), 0);

        // Case 7: No query, metric aggregations present, should use star tree
        sourceBuilder = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.max("test").field("field"));
        assertStarTreeContext(request, sourceBuilder, new StarTreeQueryContext(expectedStarTree, null, -1), -1);

        setStarTreeIndexSetting(null);
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
