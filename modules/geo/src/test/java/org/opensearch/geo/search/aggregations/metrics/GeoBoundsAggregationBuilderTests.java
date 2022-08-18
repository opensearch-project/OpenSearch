/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.metrics;

import org.opensearch.geo.GeoModulePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.BaseAggregationTestCase;

import java.util.Collection;
import java.util.Collections;

public class GeoBoundsAggregationBuilderTests extends BaseAggregationTestCase<GeoBoundsAggregationBuilder> {

    /**
     * This registers the GeoShape mapper with the Tests so that it can be used for testing the aggregation builders
     *
     * @return A Collection containing {@link GeoModulePlugin}
     */
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(GeoModulePlugin.class);
    }

    @Override
    protected GeoBoundsAggregationBuilder createTestAggregatorBuilder() {
        GeoBoundsAggregationBuilder factory = new GeoBoundsAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        String field = randomAlphaOfLengthBetween(3, 20);
        factory.field(field);
        if (randomBoolean()) {
            factory.wrapLongitude(randomBoolean());
        }
        if (randomBoolean()) {
            factory.missing("0,0");
        }
        return factory;
    }

}
