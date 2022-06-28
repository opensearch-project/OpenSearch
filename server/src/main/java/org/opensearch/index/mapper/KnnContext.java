/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.mapper;

import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Abstracts KNN segment of dense_vector field type
 */
public class KnnContext implements ToXContentFragment, Writeable {

    private final Metric metric;
    private final KnnAlgorithmContext knnAlgorithmContext;
    private static final String KNN_METRIC_NAME = "metric";
    private static final String ALGORITHM = "algorithm";

    KnnContext(final Metric metric, final KnnAlgorithmContext knnAlgorithmContext) {
        this.metric = metric;
        this.knnAlgorithmContext = knnAlgorithmContext;
    }

    /**
     * Parses an Object into a KnnContext.
     *
     * @param in Object containing mapping to be parsed
     * @return KnnContext
     */
    public static KnnContext parse(Object in) {
        if (!(in instanceof Map<?, ?>)) {
            throw new MapperParsingException("Unable to parse mapping into KnnContext. Object not of type \"Map\"");
        }

        Map<String, Object> knnMap = (Map<String, Object>) in;

        Metric metric = Metric.L2;
        KnnAlgorithmContext knnAlgorithmContext = KnnAlgorithmContextFactory.defaultContext(KnnAlgorithmContext.Method.HNSW);

        String key;
        Object value;
        for (Map.Entry<String, Object> methodEntry : knnMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();
            if (KNN_METRIC_NAME.equals(key)) {
                if (value != null && !(value instanceof String)) {
                    throw new MapperParsingException(String.format(Locale.ROOT, "[%s] must be a string", KNN_METRIC_NAME));
                }

                if (value != null) {
                    try {
                        metric = Metric.fromName((String) value);
                    } catch (IllegalArgumentException illegalArgumentException) {
                        throw new MapperParsingException(String.format(Locale.ROOT, "[%s] value [%s] is invalid", key, value));
                    }
                }
            } else if (ALGORITHM.equals(key)) {
                if (value == null) {
                    continue;
                }

                if (!(value instanceof Map)) {
                    throw new MapperParsingException("Unable to parse knn algorithm");
                }
                knnAlgorithmContext = KnnAlgorithmContext.parse(value);
            } else {
                throw new MapperParsingException(String.format(Locale.ROOT, "%s value is invalid", key));
            }
        }
        return new KnnContext(metric, knnAlgorithmContext);
    }

    public Metric getMetric() {
        return metric;
    }

    public KnnAlgorithmContext getKnnAlgorithmContext() {
        return knnAlgorithmContext;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KNN_METRIC_NAME, metric.name());
        builder.startObject(ALGORITHM);
        builder = knnAlgorithmContext.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(metric.name());
        this.knnAlgorithmContext.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KnnContext that = (KnnContext) o;
        return metric == that.metric && Objects.equals(knnAlgorithmContext, that.knnAlgorithmContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric, knnAlgorithmContext.hashCode());
    }
}
