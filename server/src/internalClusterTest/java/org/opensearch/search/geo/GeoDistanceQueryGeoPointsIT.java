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

/** Tests geo_distance queries on geo_point field types */
public class GeoDistanceQueryGeoPointsIT extends AbstractGeoDistanceIT {

    @Before
    public void setupTestIndex() throws IOException {
        indexSetup();
    }

    @Override
    public XContentBuilder addGeoMapping(XContentBuilder parentMapping) throws IOException {
        return parentMapping.startObject("location").field("type", "geo_point").endObject();
    }
}
