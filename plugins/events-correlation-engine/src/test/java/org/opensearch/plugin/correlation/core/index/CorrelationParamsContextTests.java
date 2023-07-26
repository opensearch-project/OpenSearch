/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Assert;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.correlation.core.index.CorrelationParamsContext.PARAMETERS;
import static org.opensearch.plugin.correlation.core.index.CorrelationParamsContext.VECTOR_SIMILARITY_FUNCTION;

/**
 * Unit tests for CorrelationsParamsContext
 */
public class CorrelationParamsContextTests extends OpenSearchTestCase {

    /**
     * Test reading from and writing to streams
     */
    public void testStreams() throws IOException {
        int efConstruction = 321;
        int m = 12;

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("m", m);
        parameters.put("ef_construction", efConstruction);

        CorrelationParamsContext context = new CorrelationParamsContext(VectorSimilarityFunction.EUCLIDEAN, parameters);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        context.writeTo(streamOutput);

        CorrelationParamsContext copy = new CorrelationParamsContext(streamOutput.bytes().streamInput());
        Assert.assertEquals(context.getSimilarityFunction(), copy.getSimilarityFunction());
        Assert.assertEquals(context.getParameters(), copy.getParameters());
    }

    /**
     * test get vector similarity function
     */
    public void testVectorSimilarityFunction() {
        int efConstruction = 321;
        int m = 12;

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("m", m);
        parameters.put("ef_construction", efConstruction);

        CorrelationParamsContext context = new CorrelationParamsContext(VectorSimilarityFunction.EUCLIDEAN, parameters);
        Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN, context.getSimilarityFunction());
    }

    /**
     * test get parameters
     */
    public void testParameters() {
        int efConstruction = 321;
        int m = 12;

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("m", m);
        parameters.put("ef_construction", efConstruction);

        CorrelationParamsContext context = new CorrelationParamsContext(VectorSimilarityFunction.EUCLIDEAN, parameters);
        Assert.assertEquals(parameters, context.getParameters());
    }

    /**
     * test parse method with invalid input
     * @throws IOException IOException
     */
    public void testParse_Invalid() throws IOException {
        // Invalid input type
        Integer invalidIn = 12;
        expectThrows(MapperParsingException.class, () -> CorrelationParamsContext.parse(invalidIn));

        // Invalid vector similarity function
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(CorrelationParamsContext.VECTOR_SIMILARITY_FUNCTION, 0)
            .endObject();

        final Map<String, Object> in2 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> CorrelationParamsContext.parse(in2));

        // Invalid parameters
        xContentBuilder = XContentFactory.jsonBuilder().startObject().field(PARAMETERS, 0).endObject();

        final Map<String, Object> in4 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> CorrelationParamsContext.parse(in4));
    }

    /**
     * test parse with null parameters
     * @throws IOException IOException
     */
    public void testParse_NullParameters() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(VECTOR_SIMILARITY_FUNCTION, VectorSimilarityFunction.EUCLIDEAN)
            .field(PARAMETERS, (String) null)
            .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        Assert.assertThrows(MapperParsingException.class, () -> { CorrelationParamsContext.parse(in); });
    }

    /**
     * test parse method
     * @throws IOException IOException
     */
    public void testParse_Valid() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(VECTOR_SIMILARITY_FUNCTION, VectorSimilarityFunction.EUCLIDEAN)
            .startObject(PARAMETERS)
            .field("m", 2)
            .field("ef_construction", 128)
            .endObject()
            .endObject();

        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        CorrelationParamsContext context = CorrelationParamsContext.parse(in);
        Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN, context.getSimilarityFunction());
        Assert.assertEquals(Map.of("m", 2, "ef_construction", 128), context.getParameters());
    }

    /**
     * test toXContent method
     * @throws IOException IOException
     */
    public void testToXContent() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(VECTOR_SIMILARITY_FUNCTION, VectorSimilarityFunction.EUCLIDEAN)
            .startObject(PARAMETERS)
            .field("m", 2)
            .field("ef_construction", 128)
            .endObject()
            .endObject();

        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        CorrelationParamsContext context = CorrelationParamsContext.parse(in);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = context.toXContent(builder, ToXContent.EMPTY_PARAMS);

        Map<String, Object> out = xContentBuilderToMap(builder);
        Assert.assertEquals(VectorSimilarityFunction.EUCLIDEAN.name(), out.get(VECTOR_SIMILARITY_FUNCTION));
    }

    private Map<String, Object> xContentBuilderToMap(XContentBuilder xContentBuilder) {
        return XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2();
    }
}
