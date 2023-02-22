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

import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.geo.GeoModulePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;

public class InternalGeoBoundsTests extends InternalAggregationTestCase<InternalGeoBounds> {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    /**
     * Overriding the method so that tests can get the aggregation specs for namedWriteable.
     *
     * @return GeoPlugin
     */
    @Override
    protected SearchPlugin registerPlugin() {
        return new GeoModulePlugin();
    }

    /**
     * Overriding with the {@link ParsedGeoBounds} so that it can be parsed. We need to do this as {@link GeoModulePlugin}
     * is registering this Aggregation.
     *
     * @return a List of {@link NamedXContentRegistry.Entry}
     */
    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(getDefaultNamedXContents());
        final ContextParser<Object, Aggregation> parser = (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c);
        namedXContents.add(new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(GeoBoundsAggregationBuilder.NAME), parser));
        return namedXContents;
    }

    @Override
    protected InternalGeoBounds createTestInstance(String name, Map<String, Object> metadata) {
        // we occasionally want to test top = Double.NEGATIVE_INFINITY since this triggers empty xContent object
        double top = frequently() ? randomDouble() : Double.NEGATIVE_INFINITY;
        InternalGeoBounds geo = new InternalGeoBounds(
            name,
            top,
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomBoolean(),
            metadata
        );
        return geo;
    }

    @Override
    protected void assertReduced(InternalGeoBounds reduced, List<InternalGeoBounds> inputs) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        for (InternalGeoBounds bounds : inputs) {
            if (bounds.top > top) {
                top = bounds.top;
            }
            if (bounds.bottom < bottom) {
                bottom = bounds.bottom;
            }
            if (bounds.posLeft < posLeft) {
                posLeft = bounds.posLeft;
            }
            if (bounds.posRight > posRight) {
                posRight = bounds.posRight;
            }
            if (bounds.negLeft < negLeft) {
                negLeft = bounds.negLeft;
            }
            if (bounds.negRight > negRight) {
                negRight = bounds.negRight;
            }
        }
        assertValueClose(reduced.top, top);
        assertValueClose(reduced.bottom, bottom);
        assertValueClose(reduced.posLeft, posLeft);
        assertValueClose(reduced.posRight, posRight);
        assertValueClose(reduced.negLeft, negLeft);
        assertValueClose(reduced.negRight, negRight);
    }

    private static void assertValueClose(double expected, double actual) {
        if (Double.isInfinite(expected) == false) {
            assertThat(expected, closeTo(actual, GEOHASH_TOLERANCE));
        } else {
            assertTrue(Double.isInfinite(actual));
        }
    }

    @Override
    protected void assertFromXContent(InternalGeoBounds aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedGeoBounds);
        ParsedGeoBounds parsed = (ParsedGeoBounds) parsedAggregation;

        assertEquals(aggregation.topLeft(), parsed.topLeft());
        assertEquals(aggregation.bottomRight(), parsed.bottomRight());
    }

    @Override
    protected InternalGeoBounds mutateInstance(InternalGeoBounds instance) {
        String name = instance.getName();
        double top = instance.top;
        double bottom = instance.bottom;
        double posLeft = instance.posLeft;
        double posRight = instance.posRight;
        double negLeft = instance.negLeft;
        double negRight = instance.negRight;
        boolean wrapLongitude = instance.wrapLongitude;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 8)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(top)) {
                    top += between(1, 20);
                } else {
                    top = randomDouble();
                }
                break;
            case 2:
                bottom += between(1, 20);
                break;
            case 3:
                posLeft += between(1, 20);
                break;
            case 4:
                posRight += between(1, 20);
                break;
            case 5:
                negLeft += between(1, 20);
                break;
            case 6:
                negRight += between(1, 20);
                break;
            case 7:
                wrapLongitude = wrapLongitude == false;
                break;
            case 8:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, metadata);
    }
}
