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

import org.opensearch.Version;
import org.opensearch.common.SetOnce;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.rest.action.search.RestSearchAction.TYPED_KEYS_PARAM;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class InternalSumTests extends InternalAggregationTestCase<InternalSum> {

    @Override
    protected InternalSum createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDouble() : randomFrom(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        long count = frequently() ? randomNonNegativeLong() % 100000 : 0;
        DocValueFormat formatter = randomFrom(new DocValueFormat.Decimal("###.##"), DocValueFormat.RAW);
        return new InternalSum(name, value, count, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalSum reduced, List<InternalSum> inputs) {
        double expectedSum = inputs.stream().mapToDouble(InternalSum::getValue).sum();
        long expectedCount = inputs.stream().mapToLong(InternalSum::getCount).sum();
        assertEquals(expectedSum, reduced.getValue(), 0.0001d);
        assertEquals(expectedCount, reduced.getCount());
    }

    public void testSummationAccuracy() {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifySummationOfDoubles(values, 13.5, 0d);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifySummationOfDoubles(values, sum, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifySummationOfDoubles(double[] values, double expected, double delta) {
        List<InternalAggregation> aggregations = new ArrayList<>(values.length);
        for (double value : values) {
            aggregations.add(new InternalSum("dummy1", value, 1L, null, null));
        }
        InternalSum internalSum = new InternalSum("dummy", 0, 0L, null, null);
        InternalSum reduced = internalSum.reduce(aggregations, null);
        assertEquals(expected, reduced.value(), delta);
    }

    @Override
    protected void assertFromXContent(InternalSum sum, ParsedAggregation parsedAggregation) {
        ParsedSum parsed = ((ParsedSum) parsedAggregation);
        if (sum.getCount() != 0) {
            assertEquals(sum.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(sum.getValueAsString(), parsed.getValueAsString());
        } else {
            // When count == 0, InternalSum renders "value": null.
            // ParsedSum should parse null as the POSITIVE_INFINITY sentinel.
            assertEquals(Double.POSITIVE_INFINITY, parsed.getValue(), 0d);
        }
    }

    public void testVersionedSerializationPreV360() throws IOException {
        InternalSum original = new InternalSum("test_sum", 42.0, 5L, DocValueFormat.RAW, null);
        InternalSum deserialized = copyInstance(original, Version.V_3_5_0);

        // Sum value is preserved across versions
        assertEquals(original.getValue(), deserialized.getValue(), 0d);
        // Pre-3.6 streams do not carry count; deserializer defaults to 1
        assertEquals(1L, deserialized.getCount());
    }

    public void testVersionedSerializationPreV360ZeroCount() throws IOException {
        // A new-format sum with count=0 (null semantics) sent to an old node and back
        InternalSum original = new InternalSum("test_sum", 0.0, 0L, DocValueFormat.RAW, null);
        InternalSum deserialized = copyInstance(original, Version.V_3_5_0);

        assertEquals(original.getValue(), deserialized.getValue(), 0d);
        // Legacy fallback: count becomes 1, so null semantics are lost on old nodes
        assertEquals(1L, deserialized.getCount());
    }

    public void testVersionedSerializationCurrentVersion() throws IOException {
        InternalSum original = new InternalSum("test_sum", 42.0, 5L, DocValueFormat.RAW, null);
        InternalSum deserialized = copyInstance(original, Version.CURRENT);

        assertEquals(original.getValue(), deserialized.getValue(), 0d);
        assertEquals(original.getCount(), deserialized.getCount());
    }

    public void testVersionedSerializationCurrentVersionZeroCount() throws IOException {
        InternalSum original = new InternalSum("test_sum", 0.0, 0L, DocValueFormat.RAW, null);
        InternalSum deserialized = copyInstance(original, Version.CURRENT);

        assertEquals(original.getValue(), deserialized.getValue(), 0d);
        assertEquals(0L, deserialized.getCount());
    }

    public void testParsedSumNullValueXContentRoundTrip() throws IOException {
        // InternalSum with count=0 should render "value": null in xcontent.
        // ParsedSum should parse that null back as the POSITIVE_INFINITY sentinel
        // and re-render it as null, completing a lossless round-trip.
        InternalSum emptySum = new InternalSum("test_null", 0.0, 0L, DocValueFormat.RAW, null);

        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(TYPED_KEYS_PARAM, "true"));
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();

        // Serialize InternalSum to bytes
        BytesReference originalBytes = toXContent(emptySum, xContentType, params, humanReadable);

        // Parse bytes into ParsedSum
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());

            SetOnce<Aggregation> parsedAgg = new SetOnce<>();
            XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class, parsedAgg::set);
            ParsedSum parsed = (ParsedSum) parsedAgg.get();

            // Verify the sentinel value is POSITIVE_INFINITY (not NEGATIVE_INFINITY)
            assertEquals(Double.POSITIVE_INFINITY, parsed.getValue(), 0d);

            // Re-serialize and verify round-trip consistency
            BytesReference parsedBytes = toXContent(parsed, xContentType, params, humanReadable);
            assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
        }
    }

    public void testParsedSumPositiveInfinityRoundTrip() throws IOException {
        // InternalSum with count > 0 and value = POSITIVE_INFINITY is a legitimate result
        // (e.g. summing many Double.MAX_VALUE values). ParsedSum must not confuse it with
        // the null sentinel and must round-trip it correctly as Infinity, not null.
        InternalSum infSum = new InternalSum("test_inf", Double.POSITIVE_INFINITY, 5L, DocValueFormat.RAW, null);

        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(TYPED_KEYS_PARAM, "true"));
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();

        BytesReference originalBytes = toXContent(infSum, xContentType, params, humanReadable);

        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());

            SetOnce<Aggregation> parsedAgg = new SetOnce<>();
            XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class, parsedAgg::set);
            ParsedSum parsed = (ParsedSum) parsedAgg.get();

            assertEquals(Double.POSITIVE_INFINITY, parsed.getValue(), 0d);

            BytesReference parsedBytes = toXContent(parsed, xContentType, params, humanReadable);
            assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
        }
    }

    @Override
    protected InternalSum mutateInstance(InternalSum instance) {
        String name = instance.getName();
        double value = instance.getValue();
        long count = instance.getCount();
        DocValueFormat formatter = instance.format;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
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
                count += between(1, 100);
                break;
            case 3:
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
        return new InternalSum(name, value, count, formatter, metadata);
    }
}
