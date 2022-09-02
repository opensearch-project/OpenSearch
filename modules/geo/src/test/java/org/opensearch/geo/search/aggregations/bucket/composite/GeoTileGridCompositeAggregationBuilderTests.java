/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket.composite;

import org.opensearch.geo.GeoModulePlugin;
import org.opensearch.geo.tests.common.RandomGeoGenerator;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.BaseAggregationTestCase;
import org.opensearch.search.aggregations.bucket.GeoTileUtils;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class GeoTileGridCompositeAggregationBuilderTests extends BaseAggregationTestCase<CompositeAggregationBuilder> {

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(GeoModulePlugin.class);
    }

    private GeoTileGridValuesSourceBuilder randomGeoTileGridValuesSourceBuilder() {
        GeoTileGridValuesSourceBuilder geoTile = new GeoTileGridValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            geoTile.precision(randomIntBetween(0, GeoTileUtils.MAX_ZOOM));
        }
        if (randomBoolean()) {
            geoTile.geoBoundingBox(RandomGeoGenerator.randomBBox());
        }
        return geoTile;
    }

    @Override
    protected CompositeAggregationBuilder createTestAggregatorBuilder() {
        int numSources = randomIntBetween(1, 10);
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (int i = 0; i < numSources; i++) {
            sources.add(randomGeoTileGridValuesSourceBuilder());
        }
        return new CompositeAggregationBuilder(randomAlphaOfLength(10), sources);
    }
}
