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

import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.geo.search.aggregations.bucket.geogrid.cells.CellIdSource;
import org.opensearch.geo.search.aggregations.bucket.geogrid.cells.GeoShapeCellIdSource;
import org.opensearch.geo.search.aggregations.bucket.geogrid.util.GeoShapeHashUtil;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.NonCollectingAggregator;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Aggregation Factory for geohash_grid agg
 *
 * @opensearch.internal
 */
class GeoHashGridAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final int precision;
    private final int requiredSize;
    private final int shardSize;
    private final GeoBoundingBox geoBoundingBox;

    GeoHashGridAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int precision,
        int requiredSize,
        int shardSize,
        GeoBoundingBox geoBoundingBox,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.precision = precision;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.geoBoundingBox = geoBoundingBox;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new GeoHashGrid(name, requiredSize, emptyList(), metadata);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(GeoHashGridAggregationBuilder.REGISTRY_KEY, config)
            .build(
                name,
                factories,
                config.getValuesSource(),
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                searchContext,
                parent,
                cardinality,
                metadata
            );
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        // register GeoPoint Aggregation
        builder.register(
            GeoHashGridAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEOPOINT,
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                aggregationContext,
                parent,
                cardinality,
                metadata) -> {
                CellIdSource cellIdSource = new CellIdSource(
                    (ValuesSource.GeoPoint) valuesSource,
                    precision,
                    geoBoundingBox,
                    Geohash::longEncode
                );
                return new GeoHashGridAggregator(
                    name,
                    factories,
                    cellIdSource,
                    requiredSize,
                    shardSize,
                    aggregationContext,
                    parent,
                    cardinality,
                    metadata
                );
            },
            true
        );
        // register GeoShape Aggregation
        builder.register(
            GeoHashGridAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.GEO_SHAPE,
            (
                name,
                factories,
                valuesSource,
                precision,
                geoBoundingBox,
                requiredSize,
                shardSize,
                aggregationContext,
                parent,
                cardinality,
                metadata) -> {
                final GeoShapeCellIdSource cellIdSource = new GeoShapeCellIdSource(
                    (ValuesSource.GeoShape) valuesSource,
                    precision,
                    geoBoundingBox,
                    GeoShapeHashUtil::encodeShape
                );
                return new GeoHashGridAggregator(
                    name,
                    factories,
                    cellIdSource,
                    requiredSize,
                    shardSize,
                    aggregationContext,
                    parent,
                    cardinality,
                    metadata
                );
            },
            true
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}
