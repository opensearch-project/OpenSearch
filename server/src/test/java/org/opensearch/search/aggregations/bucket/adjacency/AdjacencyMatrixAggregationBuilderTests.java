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

package org.opensearch.search.aggregations.bucket.adjacency;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdjacencyMatrixAggregationBuilderTests extends OpenSearchTestCase {

    public void testFilterSizeLimitation() throws Exception {
        // filter size grater than max size should throw an exception
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        Settings settings = Settings.builder()
            .put("index.max_adjacency_matrix_filters", 2)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(queryShardContext.getIndexSettings()).thenReturn(indexSettings);
        SearchContext context = new TestSearchContext(queryShardContext, indexShard);

        Map<String, QueryBuilder> filters = new HashMap<>(3);
        for (int i = 0; i < 3; i++) {
            QueryBuilder queryBuilder = mock(QueryBuilder.class);
            // return builder itself to skip rewrite
            when(queryBuilder.rewrite(queryShardContext)).thenReturn(queryBuilder);
            filters.put("filter" + i, queryBuilder);
        }
        AdjacencyMatrixAggregationBuilder builder = new AdjacencyMatrixAggregationBuilder("dummy", filters);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> builder.doBuild(context.getQueryShardContext(), null, new AggregatorFactories.Builder())
        );
        assertThat(
            ex.getMessage(),
            equalTo(
                "Number of filters is too large, must be less than or equal to: [2] but was [3]."
                    + "This limit can be set by changing the ["
                    + IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()
                    + "] index level setting."
            )
        );

        // filter size not greater than max size should return an instance of AdjacencyMatrixAggregatorFactory
        Map<String, QueryBuilder> emptyFilters = Collections.emptyMap();

        AdjacencyMatrixAggregationBuilder aggregationBuilder = new AdjacencyMatrixAggregationBuilder("dummy", emptyFilters);
        AggregatorFactory factory = aggregationBuilder.doBuild(context.getQueryShardContext(), null, new AggregatorFactories.Builder());
        assertThat(factory instanceof AdjacencyMatrixAggregatorFactory, is(true));
        assertThat(factory.name(), equalTo("dummy"));
        assertWarnings(
            "[index.max_adjacency_matrix_filters] setting was deprecated in OpenSearch and will be "
                + "removed in a future release! See the breaking changes documentation for the next major version."
        );
    }

    public void testShowOnlyIntersecting() throws Exception {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);

        Map<String, QueryBuilder> filters = new HashMap<>(3);
        for (int i = 0; i < 2; i++) {
            QueryBuilder queryBuilder = mock(QueryBuilder.class);
            // return builder itself to skip rewrite
            when(queryBuilder.rewrite(queryShardContext)).thenReturn(queryBuilder);
            filters.put("filter" + i, queryBuilder);
        }
        AdjacencyMatrixAggregationBuilder builder = new AdjacencyMatrixAggregationBuilder("dummy", filters, true);
        assertTrue(builder.isShowOnlyIntersecting());

        builder = new AdjacencyMatrixAggregationBuilder("dummy", filters, false);
        assertFalse(builder.isShowOnlyIntersecting());
    }
}
