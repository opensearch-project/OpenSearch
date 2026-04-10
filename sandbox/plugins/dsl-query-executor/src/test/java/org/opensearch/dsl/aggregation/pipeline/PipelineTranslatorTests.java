/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class PipelineTranslatorTests extends OpenSearchTestCase {

    public void testResolveFormatWithNoFormat() {
        var builder = new AvgBucketPipelineAggregationBuilder("avg", "path>metric");
        DocValueFormat format = PipelineTranslator.resolveFormat(builder);

        assertSame(DocValueFormat.RAW, format);
    }

    public void testResolveFormatWithDecimalFormat() {
        var builder = new SumBucketPipelineAggregationBuilder("sum", "path>metric");
        builder.format("#,##0.00");
        DocValueFormat format = PipelineTranslator.resolveFormat(builder);

        assertTrue(format instanceof DocValueFormat.Decimal);
    }
}
