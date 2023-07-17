/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Defines vector similarity function, m and ef_construction hyper parameters field mappings for correlation_vector type.
 *
 * @opensearch.internal
 */
public class CorrelationParamsContext implements ToXContentFragment, Writeable {

    /**
     * Vector Similarity Function field
     */
    public static final String VECTOR_SIMILARITY_FUNCTION = "similarityFunction";
    /**
     * Parameters field to define m and ef_construction
     */
    public static final String PARAMETERS = "parameters";

    private final VectorSimilarityFunction similarityFunction;
    private final Map<String, Object> parameters;

    /**
     * Parameterized ctor for CorrelationParamsContext
     * @param similarityFunction Vector Similarity Function
     * @param parameters Parameters to define m and ef_construction
     */
    public CorrelationParamsContext(VectorSimilarityFunction similarityFunction, Map<String, Object> parameters) {
        this.similarityFunction = similarityFunction;
        this.parameters = parameters;
    }

    /**
     * Parameterized ctor for CorrelationParamsContext
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public CorrelationParamsContext(StreamInput sin) throws IOException {
        this.similarityFunction = VectorSimilarityFunction.valueOf(sin.readString());
        if (sin.available() > 0) {
            this.parameters = sin.readMap();
        } else {
            this.parameters = null;
        }
    }

    /**
     * Parse into CorrelationParamsContext
     * @param in Object
     * @return CorrelationParamsContext
     */
    public static CorrelationParamsContext parse(Object in) {
        if (!(in instanceof Map<?, ?>)) {
            throw new MapperParsingException("Unable to parse CorrelationParamsContext");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> contextMap = (Map<String, Object>) in;
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
        Map<String, Object> parameters = new HashMap<>();

        if (contextMap.containsKey(VECTOR_SIMILARITY_FUNCTION)) {
            Object value = contextMap.get(VECTOR_SIMILARITY_FUNCTION);

            if (value != null && !(value instanceof String)) {
                throw new MapperParsingException(String.format(Locale.getDefault(), "%s must be a string", VECTOR_SIMILARITY_FUNCTION));
            }

            try {
                similarityFunction = VectorSimilarityFunction.valueOf((String) value);
            } catch (IllegalArgumentException ex) {
                throw new MapperParsingException(String.format(Locale.getDefault(), "Invalid %s: %s", VECTOR_SIMILARITY_FUNCTION, value));
            }
        }
        if (contextMap.containsKey(PARAMETERS)) {
            Object value = contextMap.get(PARAMETERS);
            if (!(value instanceof Map)) {
                throw new MapperParsingException("Unable to parse parameters for Correlation context");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> valueMap = (Map<String, Object>) value;
            parameters.putAll(valueMap);
        }
        return new CorrelationParamsContext(similarityFunction, parameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VECTOR_SIMILARITY_FUNCTION, similarityFunction.name());
        if (params == null) {
            builder.field(PARAMETERS, (String) null);
        } else {
            builder.startObject(PARAMETERS);
            for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
                builder.field(parameter.getKey(), parameter.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(similarityFunction.name());
        if (this.parameters != null) {
            out.writeMap(parameters);
        }
    }

    /**
     * get Vector Similarity Function
     * @return Vector Similarity Function
     */
    public VectorSimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    /**
     * Get Parameters to define m and ef_construction
     * @return Parameters to define m and ef_construction
     */
    public Map<String, Object> getParameters() {
        return parameters;
    }
}
