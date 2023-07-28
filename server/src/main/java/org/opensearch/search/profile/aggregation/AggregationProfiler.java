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

package org.opensearch.search.profile.aggregation;

import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.profile.AbstractProfiler;

import java.util.HashMap;
import java.util.Map;

/**
 * Main class to profile aggregations
 *
 * @opensearch.internal
 */
public class AggregationProfiler extends AbstractProfiler<AggregationProfileBreakdown, Aggregator> {

    private final Map<Aggregator, AggregationProfileBreakdown> profileBreakdownLookup = new HashMap<>();

    public AggregationProfiler() {
        super(new InternalAggregationProfileTree());
    }

    /**
     * This method does not need to be thread safe for concurrent search use case as well.
     * The {@link AggregationProfileBreakdown} for each Aggregation operator is created in sync path when
     * {@link org.opensearch.search.aggregations.BucketCollector#preCollection()} is called
     * on the Aggregation collector instances during construction.
     */
    @Override
    public AggregationProfileBreakdown getQueryBreakdown(Aggregator agg) {
        AggregationProfileBreakdown aggregationProfileBreakdown = profileBreakdownLookup.get(agg);
        if (aggregationProfileBreakdown == null) {
            aggregationProfileBreakdown = super.getQueryBreakdown(agg);
            profileBreakdownLookup.put(agg, aggregationProfileBreakdown);
        }
        return aggregationProfileBreakdown;
    }
}
