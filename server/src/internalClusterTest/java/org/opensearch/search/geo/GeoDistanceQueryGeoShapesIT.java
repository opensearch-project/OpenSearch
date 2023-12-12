/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.geo;

import org.opensearch.core.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;

/** Tests geo_distance queries on geo_shape field types */
public class GeoDistanceQueryGeoShapesIT extends AbstractGeoDistanceIT {

    @Before
    public void setupTestIndex() throws IOException {
        indexSetup();
    }

    @Override
    public XContentBuilder addGeoMapping(XContentBuilder parentMapping) throws IOException {
        parentMapping = parentMapping.startObject("location").field("type", "geo_shape");
        if (randomBoolean()) {
            parentMapping.field("strategy", "recursive");
        }
        return parentMapping.endObject();
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/2515")
    @Override
    public void testDistanceScript() {
        // no-op; todo script support for distance calculation on shapes cannot be added until GeoShapeDocValues is added
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/2515")
    @Override
    public void testGeoDistanceAggregation() {
        // no-op; todo aggregation support for distance calculation on shapes cannot be added until GeoShapeDocValues is added
    }
}
