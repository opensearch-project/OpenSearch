/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximateMatchAllQueryTests extends OpenSearchTestCase {

    public void testCanApproximate() throws IOException {
        ApproximateMatchAllQuery approximateMatchAllQuery = new ApproximateMatchAllQuery();
        // Fail on null searchContext
        assertFalse(approximateMatchAllQuery.canApproximate(null));

        ShardSearchRequest[] shardSearchRequest = new ShardSearchRequest[1];

        MapperService mockMapper = mock(MapperService.class);
        String sortfield = "myfield";
        MappedFieldType myFieldType = new NumberFieldMapper.NumberFieldType(sortfield, NumberFieldMapper.NumberType.LONG);
        when(mockMapper.fieldType(sortfield)).thenReturn(myFieldType);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        QueryShardContext queryShardContext = new QueryShardContext(
            0,
            new IndexSettings(indexMetadata, settings),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            mockMapper,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        TestSearchContext searchContext = new TestSearchContext(queryShardContext) {
            @Override
            public ShardSearchRequest request() {
                return shardSearchRequest[0];
            }
        };

        // Fail if aggregations are present
        searchContext.aggregations(new SearchContextAggregations(new AggregatorFactories.Builder().build(null, null), null));
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));
        searchContext.aggregations(null);

        // Fail on missing ShardSearchRequest
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        // Fail if source is null or empty
        shardSearchRequest[0] = new ShardSearchRequest(null, System.currentTimeMillis(), null);
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        // Fail if source does not have a sort.
        SearchSourceBuilder source = new SearchSourceBuilder();
        shardSearchRequest[0].source(source);
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));

        source.sort(sortfield, SortOrder.ASC);
        assertTrue(approximateMatchAllQuery.canApproximate(searchContext));
        assertTrue(approximateMatchAllQuery.rewrite(null) instanceof ApproximatePointRangeQuery);

        // But not if the sort field makes a decision about missing data
        source.sorts().clear();
        source.sort(new FieldSortBuilder(sortfield).missing("foo"));
        assertFalse(approximateMatchAllQuery.canApproximate(searchContext));
        assertThrows(IllegalStateException.class, () -> approximateMatchAllQuery.rewrite(null));
    }

    public void testCannotApproximateWithTrackTotalHits() {
        ApproximateMatchAllQuery approximateMatchAllQuery = new ApproximateMatchAllQuery();

        ShardSearchRequest[] shardSearchRequest = new ShardSearchRequest[1];

        MapperService mockMapper = mock(MapperService.class);
        String sortfield = "myfield";
        MappedFieldType myFieldType = new NumberFieldMapper.NumberFieldType(sortfield, NumberFieldMapper.NumberType.LONG);
        when(mockMapper.fieldType(sortfield)).thenReturn(myFieldType);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(settings).build();
        QueryShardContext queryShardContext = new QueryShardContext(
            0,
            new IndexSettings(indexMetadata, settings),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            mockMapper,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        TestSearchContext searchContext = new TestSearchContext(queryShardContext) {
            @Override
            public ShardSearchRequest request() {
                return shardSearchRequest[0];
            }
        };

        SearchSourceBuilder source = new SearchSourceBuilder();
        shardSearchRequest[0] = new ShardSearchRequest(null, System.currentTimeMillis(), null);
        shardSearchRequest[0].source(source);
        source.sort(sortfield, SortOrder.ASC);

        assertTrue(approximateMatchAllQuery.canApproximate(searchContext));

        searchContext.trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        assertFalse("Should not approximate when track_total_hits is accurate", approximateMatchAllQuery.canApproximate(searchContext));

        searchContext.trackTotalHitsUpTo(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        assertTrue("Should approximate when track_total_hits is not accurate", approximateMatchAllQuery.canApproximate(searchContext));
    }

}
