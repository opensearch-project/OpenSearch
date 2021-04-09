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

import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.ShapeType;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.geo.GeometryTestUtils.randomGeometry;
import static org.opensearch.index.query.LegacyGeoShapeQueryProcessor.geometryToShapeBuilder;

public class GeometryIOTests extends OpenSearchTestCase {

    public void testRandomSerialization() throws Exception {
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            boolean hasAlt = randomBoolean();
            Geometry geometry = randomGeometry(hasAlt);
            if (shapeSupported(geometry) && randomBoolean()) {
                // Shape builder conversion doesn't support altitude
                ShapeBuilder<?, ?, ?> shapeBuilder = geometryToShapeBuilder(geometry);
                if (randomBoolean()) {
                    Geometry actual = shapeBuilder.buildGeometry();
                    assertEquals(geometry, actual);
                }
                if (randomBoolean()) {
                    // Test ShapeBuilder -> Geometry Serialization
                    try (BytesStreamOutput out = new BytesStreamOutput()) {
                        out.writeNamedWriteable(shapeBuilder);
                        try (StreamInput in = out.bytes().streamInput()) {
                            Geometry actual = GeometryIO.readGeometry(in);
                            assertEquals(geometry, actual);
                            assertEquals(0, in.available());
                        }
                    }
                } else {
                    // Test Geometry -> ShapeBuilder Serialization
                    try (BytesStreamOutput out = new BytesStreamOutput()) {
                        GeometryIO.writeGeometry(out, geometry);
                        try (StreamInput in = out.bytes().streamInput()) {
                            try (StreamInput nin = new NamedWriteableAwareStreamInput(in, this.writableRegistry())) {
                                ShapeBuilder<?, ?, ?> actual = nin.readNamedWriteable(ShapeBuilder.class);
                                assertEquals(shapeBuilder, actual);
                                assertEquals(0, in.available());
                            }
                        }
                    }
                }
                // Test Geometry -> Geometry
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    GeometryIO.writeGeometry(out, geometry);
                    ;
                    try (StreamInput in = out.bytes().streamInput()) {
                        Geometry actual = GeometryIO.readGeometry(in);
                        assertEquals(geometry, actual);
                        assertEquals(0, in.available());
                    }
                }

            }
        }
    }

    private boolean shapeSupported(Geometry geometry) {
        if (geometry.hasZ()) {
            return false;
        }

        if (geometry.type() == ShapeType.GEOMETRYCOLLECTION) {
            GeometryCollection<?> collection = (GeometryCollection<?>) geometry;
            for (Geometry g : collection) {
                if (shapeSupported(g) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(GeoShapeType.getShapeWriteables());
    }
}
