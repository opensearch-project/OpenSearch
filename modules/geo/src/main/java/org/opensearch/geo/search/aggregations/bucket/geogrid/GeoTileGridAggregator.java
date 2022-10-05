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

package org.opensearch.geo.search.aggregations.bucket.geogrid;

import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as geotile longs (for efficiency's sake) but formats results as geotile strings.
 *
 * @opensearch.internal
 */
class GeoTileGridAggregator extends GeoGridAggregator<GeoTileGrid> {

    GeoTileGridAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        int requiredSize,
        int shardSize,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, valuesSource, requiredSize, shardSize, aggregationContext, parent, cardinality, metadata);
    }

    @Override
    protected GeoTileGrid buildAggregation(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata) {
        return new GeoTileGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public GeoTileGrid buildEmptyAggregation() {
        return new GeoTileGrid(name, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    protected BaseGeoGridBucket newEmptyBucket() {
        return new InternalGeoTileGridBucket(0, 0, null);
    }
}
