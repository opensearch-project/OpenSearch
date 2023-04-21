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

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geo.GeometryTestUtils;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.utils.GeographyValidator;
import org.opensearch.test.AbstractXContentTestCase;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.geo.GeometryTestUtils.randomCircle;
import static org.opensearch.geo.GeometryTestUtils.randomGeometryCollection;
import static org.opensearch.geo.GeometryTestUtils.randomLine;
import static org.opensearch.geo.GeometryTestUtils.randomMultiLine;
import static org.opensearch.geo.GeometryTestUtils.randomMultiPoint;
import static org.opensearch.geo.GeometryTestUtils.randomMultiPolygon;
import static org.opensearch.geo.GeometryTestUtils.randomPoint;
import static org.opensearch.geo.GeometryTestUtils.randomPolygon;
import static org.hamcrest.Matchers.equalTo;

public class GeoJsonSerializationTests extends OpenSearchTestCase {

    private static class GeometryWrapper implements ToXContentObject {

        private final Geometry geometry;
        private static final GeoJson PARSER = new GeoJson(true, false, new GeographyValidator(true));

        GeometryWrapper(Geometry geometry) {
            this.geometry = geometry;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return GeoJson.toXContent(geometry, builder, params);
        }

        public static GeometryWrapper fromXContent(XContentParser parser) throws IOException {
            parser.nextToken();
            return new GeometryWrapper(PARSER.fromXContent(parser));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GeometryWrapper that = (GeometryWrapper) o;
            return Objects.equals(geometry, that.geometry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(geometry);
        }
    }

    private void xContentTest(Supplier<Geometry> instanceSupplier) throws IOException {
        AbstractXContentTestCase.xContentTester(
            this::createParser,
            () -> new GeometryWrapper(instanceSupplier.get()),
            (geometryWrapper, xContentBuilder) -> {
                geometryWrapper.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            },
            GeometryWrapper::fromXContent
        ).supportsUnknownFields(true).test();
    }

    public void testPoint() throws IOException {
        xContentTest(() -> randomPoint(randomBoolean()));
    }

    public void testMultiPoint() throws IOException {
        xContentTest(() -> randomMultiPoint(randomBoolean()));
    }

    public void testLineString() throws IOException {
        xContentTest(() -> randomLine(randomBoolean()));
    }

    public void testMultiLineString() throws IOException {
        xContentTest(() -> randomMultiLine(randomBoolean()));
    }

    public void testPolygon() throws IOException {
        xContentTest(() -> randomPolygon(randomBoolean()));
    }

    public void testMultiPolygon() throws IOException {
        xContentTest(() -> randomMultiPolygon(randomBoolean()));
    }

    public void testEnvelope() throws IOException {
        xContentTest(GeometryTestUtils::randomRectangle);
    }

    public void testGeometryCollection() throws IOException {
        xContentTest(() -> randomGeometryCollection(randomBoolean()));
    }

    public void testCircle() throws IOException {
        xContentTest(() -> randomCircle(randomBoolean()));
    }

    public void testToMap() throws IOException {
        for (int i = 0; i < 10; i++) {
            Geometry geometry = GeometryTestUtils.randomGeometry(randomBoolean());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            GeoJson.toXContent(geometry, builder, ToXContent.EMPTY_PARAMS);
            StreamInput input = BytesReference.bytes(builder).streamInput();

            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, input)
            ) {
                Map<String, Object> map = GeoJson.toMap(geometry);
                assertThat(parser.map(), equalTo(map));
            }
        }
    }
}
