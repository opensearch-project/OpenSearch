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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.search.aggregations.metrics.InternalGeoCentroid.Fields;

import java.io.IOException;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 *
 * @opensearch.internal
 */
public class ParsedGeoCentroid extends ParsedAggregation implements GeoCentroid {
    private GeoPoint centroid;
    private long count;

    @Override
    public GeoPoint centroid() {
        return centroid;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public String getType() {
        return GeoCentroidAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(Fields.CENTROID.getPreferredName());
            {
                builder.field(Fields.CENTROID_LAT.getPreferredName(), centroid.lat());
                builder.field(Fields.CENTROID_LON.getPreferredName(), centroid.lon());
            }
            builder.endObject();
        }
        builder.field(Fields.COUNT.getPreferredName(), count);
        return builder;
    }

    private static final ObjectParser<ParsedGeoCentroid, Void> PARSER = new ObjectParser<>(
        ParsedGeoCentroid.class.getSimpleName(),
        true,
        ParsedGeoCentroid::new
    );

    private static final ObjectParser<GeoPoint, Void> GEO_POINT_PARSER = new ObjectParser<>(
        ParsedGeoCentroid.class.getSimpleName() + "_POINT",
        true,
        GeoPoint::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareObject((agg, centroid) -> agg.centroid = centroid, GEO_POINT_PARSER, Fields.CENTROID);
        PARSER.declareLong((agg, count) -> agg.count = count, Fields.COUNT);

        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLat, Fields.CENTROID_LAT);
        GEO_POINT_PARSER.declareDouble(GeoPoint::resetLon, Fields.CENTROID_LON);
    }

    public static ParsedGeoCentroid fromXContent(XContentParser parser, final String name) {
        ParsedGeoCentroid geoCentroid = PARSER.apply(parser, null);
        geoCentroid.setName(name);
        return geoCentroid;
    }
}
