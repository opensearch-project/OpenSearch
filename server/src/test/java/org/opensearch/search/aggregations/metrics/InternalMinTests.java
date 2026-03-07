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

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalMinTests extends InternalAggregationTestCase<InternalMin> {
    @Override
    protected InternalMin createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDouble() : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalMin(name, value, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalMin reduced, List<InternalMin> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMin::value).min().getAsDouble(), reduced.value(), 0);
    }

    @Override
    protected void assertFromXContent(InternalMin min, ParsedAggregation parsedAggregation) {
        ParsedMin parsed = ((ParsedMin) parsedAggregation);
        if (Double.isInfinite(min.getValue()) == false) {
            assertEquals(min.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(min.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.getValue(), Double.POSITIVE_INFINITY, 0);
        }
    }

    @Override
    protected InternalMin mutateInstance(InternalMin instance) {
        String name = instance.getName();
        double value = instance.getValue();
        DocValueFormat formatter = instance.format;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(value)) {
                    value += between(1, 100);
                } else {
                    value = between(1, 100);
                }
                break;
            case 2:
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
        return new InternalMin(name, value, formatter, metadata);
    }

    public void testGetFormatReturnsCorrectFormat() {
        // Test with RAW format
        InternalMin minWithRawFormat = new InternalMin("test_min", 10.5, DocValueFormat.RAW, null);
        assertEquals(DocValueFormat.RAW, minWithRawFormat.getFormat());

        // Test with custom decimal format
        DocValueFormat customFormat = randomNumericDocValueFormat();
        InternalMin minWithCustomFormat = new InternalMin("test_min", 10.5, customFormat, null);
        assertEquals(customFormat, minWithCustomFormat.getFormat());
    }

    public void testGetFormatWithDifferentFormats() {
        // Test that getFormat() returns the same format that was passed in constructor
        DocValueFormat[] formats = new DocValueFormat[] {
            DocValueFormat.RAW,
            DocValueFormat.BOOLEAN,
            DocValueFormat.GEOHASH,
            DocValueFormat.IP
        };

        for (DocValueFormat format : formats) {
            InternalMin min = new InternalMin("test_min", randomDouble(), format, null);
            assertSame(format, min.getFormat());
        }
    }

    public void testGetFormatNotNull() {
        // Ensure getFormat() never returns null even when constructed with default format
        InternalMin min = new InternalMin("test_min", randomDouble(), DocValueFormat.RAW, null);
        assertNotNull(min.getFormat());
    }
}
