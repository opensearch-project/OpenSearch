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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregator.KeyedFilter;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregation Factory for adjacency_matrix agg
 *
 * @opensearch.internal
 */
public class AdjacencyMatrixAggregatorFactory extends AggregatorFactory {

    private final String[] keys;
    private final Weight[] weights;

    private final boolean showOnlyIntersecting;
    private final String separator;

    public AdjacencyMatrixAggregatorFactory(
        String name,
        List<KeyedFilter> filters,
        boolean showOnlyIntersecting,
        String separator,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
        IndexSearcher contextSearcher = queryShardContext.searcher();
        this.separator = separator;
        weights = new Weight[filters.size()];
        keys = new String[filters.size()];
        for (int i = 0; i < filters.size(); ++i) {
            KeyedFilter keyedFilter = filters.get(i);
            keys[i] = keyedFilter.key();
            Query filter = keyedFilter.filter().toQuery(queryShardContext);
            weights[i] = contextSearcher.createWeight(contextSearcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        this.showOnlyIntersecting = showOnlyIntersecting;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new AdjacencyMatrixAggregator(
            name,
            factories,
            separator,
            keys,
            weights,
            showOnlyIntersecting,
            searchContext,
            parent,
            metadata
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}
