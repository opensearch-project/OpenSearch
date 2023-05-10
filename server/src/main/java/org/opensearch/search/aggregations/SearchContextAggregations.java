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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;

/**
 * The aggregation context that is part of the search context.
 *
 * @opensearch.internal
 */
public class SearchContextAggregations {
    private final AggregatorFactories factories;
    private final MultiBucketConsumer multiBucketConsumer;

    // top level global aggregators in the request
    private final List<Aggregator> globalAggregators;

    // top level aggregators other than global ones in the request
    private final List<Aggregator> nonGlobalAggregators;

    /**
     * Creates a new aggregation context with the parsed aggregator factories
     */
    public SearchContextAggregations(AggregatorFactories factories, MultiBucketConsumer multiBucketConsumer) {
        this.factories = factories;
        this.multiBucketConsumer = multiBucketConsumer;
        this.globalAggregators = new ArrayList<>();
        this.nonGlobalAggregators = new ArrayList<>();
    }

    public AggregatorFactories factories() {
        return factories;
    }

    public List<Aggregator> getGlobalAggregators() {
        return Collections.unmodifiableList(globalAggregators);
    }

    public List<Aggregator> getNonGlobalAggregators() {
        return Collections.unmodifiableList(nonGlobalAggregators);
    }

    /**
     * Registers all the created non-global aggregators (top level aggregators) for the search execution context. In case of concurrent
     * segment search where multiple slices are created, it will create the {@link Aggregator} collector per slice and register here.
     *
     * @param aggregators The top level non-global aggregators of the search execution.
     */
    public void addNonGlobalAggregators(List<Aggregator> aggregators) {
        this.nonGlobalAggregators.addAll(aggregators);
    }

    /**
     * Registers all the created global aggregators (top level aggregators) for the search execution context.
     *
     * @param aggregators The top level global aggregators of the search execution.
     */
    public void addGlobalAggregators(List<Aggregator> aggregators) {
        this.globalAggregators.addAll(aggregators);
    }

    /**
     * Returns a consumer for multi bucket aggregation that checks the total number of buckets
     * created in the response
     */
    public MultiBucketConsumer multiBucketConsumer() {
        return multiBucketConsumer;
    }

    void resetBucketMultiConsumer() {
        multiBucketConsumer.reset();
    }
}
