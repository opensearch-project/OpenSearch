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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.geo.parsers;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.MapXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.AbstractGeometryFieldMapper;
import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;

import java.io.IOException;
import java.util.Collections;

/**
 * first point of entry for a shape parser
 *
 * @opensearch.internal
 */
public interface ShapeParser {
    ParseField FIELD_TYPE = new ParseField("type");
    ParseField FIELD_COORDINATES = new ParseField("coordinates");
    ParseField FIELD_GEOMETRIES = new ParseField("geometries");
    ParseField FIELD_ORIENTATION = new ParseField("orientation");

    /**
     * Create a new {@link ShapeBuilder} from {@link XContent}
     * @param parser parser to read the GeoShape from
     * @param geometryMapper document field mapper reference required for spatial parameters relevant
     *                     to the shape construction process (e.g., orientation)
     *                     todo: refactor to place build specific parameters in the SpatialContext
     * @return {@link ShapeBuilder} read from the parser or null
     *          if the parsers current token has been <code>null</code>
     * @throws IOException if the input could not be read
     */
    static ShapeBuilder parse(XContentParser parser, AbstractGeometryFieldMapper geometryMapper) throws IOException {
        AbstractShapeGeometryFieldMapper shapeMapper = null;
        if (geometryMapper != null) {
            if (geometryMapper instanceof AbstractShapeGeometryFieldMapper == false) {
                throw new IllegalArgumentException("geometry must be a shape type");
            }
            shapeMapper = (AbstractShapeGeometryFieldMapper) geometryMapper;
        }
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return GeoJsonParser.parse(parser, shapeMapper);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return GeoWKTParser.parse(parser, shapeMapper);
        }
        throw new OpenSearchParseException("shape must be an object consisting of type and coordinates");
    }

    /**
     * Create a new {@link ShapeBuilder} from {@link XContent}
     * @param parser parser to read the GeoShape from
     * @return {@link ShapeBuilder} read from the parser or null
     *          if the parsers current token has been <code>null</code>
     * @throws IOException if the input could not be read
     */
    static ShapeBuilder parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    static ShapeBuilder parse(Object value) throws IOException {
        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("value", value),
                null
            )
        ) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parse(parser);
        }
    }
}
