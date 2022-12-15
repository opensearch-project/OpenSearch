/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket;

import org.opensearch.Version;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geo.GeoModulePluginIntegTestCase;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.test.VersionUtils;

import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * This is the base class for all the Bucket Aggregation related integration tests. Use this class to add common
 * methods which can be used across different bucket aggregations. If there is any common code that can be used
 * across other integration test too then this is not the class. Use {@link GeoModulePluginIntegTestCase}
 */
public abstract class AbstractBucketAggregationIntegTest extends GeoModulePluginIntegTestCase {

    protected static final int MAX_PRECISION_FOR_GEO_SHAPES_AGG_TESTING = 4;

    protected static final int NUM_DOCS = 100;

    protected static final String GEO_SHAPE_INDEX_NAME = "geoshape_index";

    // We don't want to generate this BB at random as it may lead to generation of very large or very small BB.
    // Hence, we are making the parameters of this BB static for simplicity.
    protected static final Rectangle BOUNDING_RECTANGLE_FOR_GEO_SHAPES_AGG = new Rectangle(-4.1, 20.9, 21.9, -3.1);

    protected static final String AGG_NAME = "geohashgrid";

    protected static final String GEO_SHAPE_FIELD_NAME = "location_geo_shape";

    protected static final String GEO_POINT_FIELD_NAME = "location";

    protected static final String KEYWORD_FIELD_NAME = "city";

    protected final Version version = VersionUtils.randomIndexCompatibleVersion(random());

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    protected IndexRequestBuilder indexGeoShape(final String index, final Geometry geometry) throws Exception {
        XContentBuilder source = jsonBuilder().startObject();
        source = source.field(GEO_SHAPE_FIELD_NAME, WKT.toWKT(geometry));
        source = source.endObject();
        return client().prepareIndex(index).setSource(source);
    }

    protected IndexRequestBuilder indexCity(final String index, final String name, final List<String> latLon) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().field(KEYWORD_FIELD_NAME, name);
        if (latLon != null) {
            source = source.field(GEO_POINT_FIELD_NAME, latLon);
        }
        source = source.endObject();
        return client().prepareIndex(index).setSource(source);
    }

    protected IndexRequestBuilder indexCity(final String index, final String name, final String latLon) throws Exception {
        return indexCity(index, name, List.of(latLon));
    }

}
