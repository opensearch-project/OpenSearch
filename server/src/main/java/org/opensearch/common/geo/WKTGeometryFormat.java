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

package org.opensearch.common.geo;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

/**
 * Well Known Text Format
 *
 * @opensearch.internal
 */
public class WKTGeometryFormat implements GeometryFormat<Geometry> {
    public static final String NAME = "wkt";

    private final WellKnownText wellKnownTextParser;

    public WKTGeometryFormat(WellKnownText wellKnownTextParser) {
        this.wellKnownTextParser = wellKnownTextParser;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Geometry fromXContent(XContentParser parser) throws IOException, ParseException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        return wellKnownTextParser.fromWKT(parser.text());
    }

    @Override
    public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (geometry != null) {
            return builder.value(wellKnownTextParser.toWKT(geometry));
        } else {
            return builder.nullValue();
        }
    }

    @Override
    public String toXContentAsObject(Geometry geometry) {
        return wellKnownTextParser.toWKT(geometry);
    }
}
