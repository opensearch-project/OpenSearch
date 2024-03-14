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

package org.opensearch.search.aggregations;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.search.aggregations.support.AggregationUsageService.OTHER_SUBTYPE;

/**
 * Base factory to instantiate an internal aggregator
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class AggregatorFactory {
    protected final String name;
    protected final AggregatorFactory parent;
    protected final AggregatorFactories factories;
    protected final Map<String, Object> metadata;

    protected final QueryShardContext queryShardContext;

    /**
     * Constructs a new aggregator factory.
     *
     * @param name
     *            The aggregation name
     * @throws IOException
     *             if an error occurs creating the factory
     */
    public AggregatorFactory(
        String name,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        this.name = name;
        this.queryShardContext = queryShardContext;
        this.parent = parent;
        this.factories = subFactoriesBuilder.build(queryShardContext, this);
        this.metadata = metadata;
    }

    public String name() {
        return name;
    }

    public void doValidate() {}

    protected abstract Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;

    /**
     * Creates the aggregator.
     *
     * @param parent The parent aggregator (if this is a top level factory, the
     *               parent will be {@code null})
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    public final Aggregator create(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality) throws IOException {
        return createInternal(searchContext, parent, cardinality, this.metadata);
    }

    public AggregatorFactory getParent() {
        return parent;
    }

    /**
     * Returns the aggregation subtype for nodes usage stats.
     * <p>
     * It should match the types registered by calling {@linkplain org.opensearch.search.aggregations.support.AggregationUsageService}.
     * In other words, it should be ValueSourcesType for the VST aggregations OTHER_SUBTYPE for all other aggregations.
     */
    public String getStatsSubtype() {
        return OTHER_SUBTYPE;
    }

    /**
     * Implementation should override this method and return true if the Aggregator created by the factory works with concurrent segment search execution model
     */
    protected boolean supportsConcurrentSegmentSearch() {
        return false;
    }

    public boolean evaluateChildFactories() {
        return factories.allFactoriesSupportConcurrentSearch();
    }
}
