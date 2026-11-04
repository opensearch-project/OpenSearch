/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import static org.hamcrest.Matchers.containsString;

public class BoxplotTests extends AbstractNumericMetricTestCase<BoxplotAggregationBuilder> {

    @Override
    protected BoxplotAggregationBuilder doCreateTestAggregatorFactory() {
        BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            builder.compression(randomDoubleBetween(1, 1000, true));
        }

        if (randomBoolean()) {
            builder.missing("MISSING");
        }

        if (randomBoolean()) {
            builder.format("###.00");
        }

        return builder;
    }

    public void testCompressionMustBePositive() {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("b");

        IllegalArgumentException zero = expectThrows(IllegalArgumentException.class, () -> builder.compression(0));
        assertThat(zero.getMessage(), containsString("[compression] must be greater than 0"));

        IllegalArgumentException negative = expectThrows(IllegalArgumentException.class, () -> builder.compression(-1));
        assertEquals("[compression] must be greater than 0. Found [-1.0] in [b]", negative.getMessage());

        IllegalArgumentException nan = expectThrows(IllegalArgumentException.class, () -> builder.compression(Double.NaN));
        assertEquals("[compression] must be greater than 0. Found [NaN] in [b]", nan.getMessage());

        IllegalArgumentException negativeInfinity = expectThrows(
            IllegalArgumentException.class,
            () -> builder.compression(Double.NEGATIVE_INFINITY)
        );
        assertEquals("[compression] must be greater than 0. Found [-Infinity] in [b]", negativeInfinity.getMessage());

        // a rejected value leaves the builder untouched
        assertEquals(100.0, builder.compression(), 0d);
    }

    public void testCompressionUpperBound() {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("b");

        assertEquals(10_000.0, builder.compression(10_000).compression(), 0d);
        assertEquals(BoxplotAggregationBuilder.MAX_COMPRESSION, builder.compression(), 0d);

        IllegalArgumentException tooLarge = expectThrows(IllegalArgumentException.class, () -> builder.compression(10_001));
        assertEquals("[compression] must be at most 10000.0. Found [10001.0] in [b]", tooLarge.getMessage());

        IllegalArgumentException infinity = expectThrows(
            IllegalArgumentException.class,
            () -> builder.compression(Double.POSITIVE_INFINITY)
        );
        assertEquals("[compression] must be at most 10000.0. Found [Infinity] in [b]", infinity.getMessage());

        // a rejected value leaves the builder untouched
        assertEquals(10_000.0, builder.compression(), 0d);
    }

    public void testDefaultCompression() {
        assertEquals(100.0, new BoxplotAggregationBuilder("b").compression(), 0d);
    }

    public void testCompressionIsKept() {
        final double compression = randomDoubleBetween(1, 1000, true);

        assertEquals(compression, new BoxplotAggregationBuilder("b").compression(compression).compression(), 0d);
    }
}
