/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class InternalDslPercentilesTests extends OpenSearchTestCase {

    public void testKeyedRendering() throws Exception {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0, 99.0 },
            new double[] { 899.0, 1299.0 },
            true,
            DocValueFormat.RAW
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{\"values\":{\"50.0\":899.0,\"99.0\":1299.0}}", builder.toString());
        }
    }

    public void testKeyedRenderingWithMissingValue() throws Exception {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0, 99.0 },
            new double[] { 899.0, Double.NaN },
            true,
            DocValueFormat.RAW
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{\"values\":{\"50.0\":899.0,\"99.0\":null}}", builder.toString());
        }
    }

    public void testNonKeyedRendering() throws Exception {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0 },
            new double[] { 899.0 },
            false,
            DocValueFormat.RAW
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{\"values\":[{\"key\":50.0,\"value\":899.0}]}", builder.toString());
        }
    }

    public void testKeyedRenderingWithFormat() throws Exception {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0, 99.0 },
            new double[] { 899.0, 1299.5 },
            true,
            new DocValueFormat.Decimal("0.00")
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals(
                "{\"values\":{\"50.0\":899.0,\"50.0_as_string\":\"899.00\",\"99.0\":1299.5,\"99.0_as_string\":\"1299.50\"}}",
                builder.toString()
            );
        }
    }

    public void testNonKeyedRenderingWithFormat() throws Exception {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0 },
            new double[] { 899.0 },
            false,
            new DocValueFormat.Decimal("0.00")
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{\"values\":[{\"key\":50.0,\"value\":899.0,\"value_as_string\":\"899.00\"}]}", builder.toString());
        }
    }

    public void testFormatSkipsAsStringForEmptyValue() throws Exception {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0 },
            new double[] { Double.NaN },
            true,
            new DocValueFormat.Decimal("0.00")
        );

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertEquals("{\"values\":{\"50.0\":null}}", builder.toString());
        }
    }

    public void testPercentileAsStringUsesFormat() {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0 },
            new double[] { 899.0 },
            true,
            new DocValueFormat.Decimal("0.00")
        );
        assertEquals("899.00", agg.percentileAsString(50.0));
    }

    public void testTypeNameMatchesLegacyTDigest() {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0 },
            new double[] { 1.0 },
            true,
            DocValueFormat.RAW
        );
        assertEquals("tdigest_percentiles", agg.getWriteableName());
        assertEquals("tdigest_percentiles", agg.getType());
    }

    public void testAccessors() {
        InternalDslPercentiles agg = new InternalDslPercentiles(
            "pcts",
            new double[] { 50.0, 99.0 },
            new double[] { 899.0, 1299.0 },
            true,
            DocValueFormat.RAW
        );

        assertEquals(899.0, agg.percentile(50.0), 0.001);
        assertEquals(899.0, agg.value("50.0"), 0.001);
        expectThrows(IllegalArgumentException.class, () -> agg.percentile(75.0));

        List<Percentile> entries = new ArrayList<>();
        agg.iterator().forEachRemaining(entries::add);
        assertEquals(2, entries.size());
        assertEquals(50.0, entries.get(0).getPercent(), 0.001);
        assertEquals(1299.0, entries.get(1).getValue(), 0.001);
    }

    public void testLengthMismatchRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new InternalDslPercentiles("pcts", new double[] { 50.0, 99.0 }, new double[] { 1.0 }, true, DocValueFormat.RAW)
        );
    }
}
