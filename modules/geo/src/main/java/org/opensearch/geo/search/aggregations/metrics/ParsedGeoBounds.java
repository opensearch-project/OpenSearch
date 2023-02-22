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

package org.opensearch.geo.search.aggregations.metrics;

import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.ParsedAggregation;

import java.io.IOException;

import static org.opensearch.common.geo.GeoBoundingBox.BOTTOM_RIGHT_FIELD;
import static org.opensearch.common.geo.GeoBoundingBox.BOUNDS_FIELD;
import static org.opensearch.common.geo.GeoBoundingBox.LAT_FIELD;
import static org.opensearch.common.geo.GeoBoundingBox.LON_FIELD;
import static org.opensearch.common.geo.GeoBoundingBox.TOP_LEFT_FIELD;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A geo bounds agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedGeoBounds extends ParsedAggregation implements GeoBounds {

    // A top of Double.NEGATIVE_INFINITY yields an empty xContent, so the bounding box is null
    @Nullable
    private GeoBoundingBox geoBoundingBox;

    @Override
    public String getType() {
        return GeoBoundsAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (geoBoundingBox != null) {
            geoBoundingBox.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    @Nullable
    public GeoPoint topLeft() {
        return geoBoundingBox != null ? geoBoundingBox.topLeft() : null;
    }

    @Override
    @Nullable
    public GeoPoint bottomRight() {
        return geoBoundingBox != null ? geoBoundingBox.bottomRight() : null;
    }

    private static final ObjectParser<ParsedGeoBounds, Void> PARSER = new ObjectParser<>(
        ParsedGeoBounds.class.getSimpleName(),
        true,
        ParsedGeoBounds::new
    );

    private static final ConstructingObjectParser<Tuple<GeoPoint, GeoPoint>, Void> BOUNDS_PARSER = new ConstructingObjectParser<>(
        ParsedGeoBounds.class.getSimpleName() + "_BOUNDS",
        true,
        args -> new Tuple<>((GeoPoint) args[0], (GeoPoint) args[1])
    );

    private static final ObjectParser<GeoPoint, Void> GEO_POINT_PARSER = new ObjectParser<>(
        ParsedGeoBounds.class.getSimpleName() + "_POINT",
        true,
        GeoPoint::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject(
            (agg, bbox) -> { agg.geoBoundingBox = new GeoBoundingBox(bbox.v1(), bbox.v2()); },
            BOUNDS_PARSER,
            BOUNDS_FIELD
        );

        BOUNDS_PARSER.declareObject(constructorArg(), GEO_POINT_PARSER, TOP_LEFT_FIELD);
        BOUNDS_PARSER.declareObject(constructorArg(), GEO_POINT_PARSER, BOTTOM_RIGHT_FIELD);

        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLat, LAT_FIELD);
        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLon, LON_FIELD);
    }

    public static ParsedGeoBounds fromXContent(XContentParser parser, final String name) {
        ParsedGeoBounds geoBounds = PARSER.apply(parser, null);
        geoBounds.setName(name);
        return geoBounds;
    }

}
