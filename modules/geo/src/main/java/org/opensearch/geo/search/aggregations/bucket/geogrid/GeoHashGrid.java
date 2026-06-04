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

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Represents a grid of cells where each cell's location is determined by a geohash.
 * All geohashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 *
 * @opensearch.api
 */
public class GeoHashGrid extends BaseGeoGrid<InternalGeoHashGridBucket> {

    GeoHashGrid(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata) {
        super(name, requiredSize, buckets, metadata);
    }

    public GeoHashGrid(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BaseGeoGrid create(List<BaseGeoGridBucket> buckets) {
        return new GeoHashGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public BaseGeoGridBucket createBucket(InternalAggregations aggregations, BaseGeoGridBucket prototype) {
        return new InternalGeoHashGridBucket(prototype.hashAsLong, prototype.docCount, aggregations);
    }

    @Override
    protected BaseGeoGrid create(String name, int requiredSize, List buckets, Map metadata) {
        return new GeoHashGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    protected InternalGeoHashGridBucket createBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        return new InternalGeoHashGridBucket(hashAsLong, docCount, aggregations);
    }

    @Override
    protected Reader<InternalGeoHashGridBucket> getBucketReader() {
        return InternalGeoHashGridBucket::new;
    }

    @Override
    public String getWriteableName() {
        return GeoHashGridAggregationBuilder.NAME;
    }
}
