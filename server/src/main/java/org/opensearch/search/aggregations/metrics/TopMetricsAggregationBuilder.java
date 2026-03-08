/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationInitializationException;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Aggregation Builder for top_metrics agg.
 *
 * @opensearch.internal
 */
public class TopMetricsAggregationBuilder extends AbstractAggregationBuilder<TopMetricsAggregationBuilder> {
    public static final String NAME = "top_metrics";

    private static final ParseField METRICS_FIELD = new ParseField("metrics");
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField SORT_FIELD = new ParseField("sort");
    private static final ParseField SIZE_FIELD = new ParseField("size");
    private static final int MAX_SIZE = 10;

    private final List<String> metricFields = new ArrayList<>();
    private final List<SortBuilder<?>> sorts = new ArrayList<>();
    private int size = 1;

    public TopMetricsAggregationBuilder(String name) {
        super(name);
    }

    protected TopMetricsAggregationBuilder(TopMetricsAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        metricFields.addAll(clone.metricFields);
        sorts.addAll(clone.sorts);
        size = clone.size;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TopMetricsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    public TopMetricsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        metricFields.addAll(in.readStringList());
        sorts.addAll((List<SortBuilder<?>>) (List<?>) in.readNamedWriteableList(SortBuilder.class));
        size = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringCollection(metricFields);
        out.writeNamedWriteableList(sorts);
        out.writeVInt(size);
    }

    public TopMetricsAggregationBuilder metricField(String field) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("[field] for [" + name + "] must not be null or empty");
        }
        metricFields.add(field);
        return this;
    }

    public TopMetricsAggregationBuilder sort(SortBuilder<?> sortBuilder) {
        sorts.add(sortBuilder);
        return this;
    }

    public TopMetricsAggregationBuilder size(int size) {
        if (size <= 0 || size > MAX_SIZE) {
            throw new IllegalArgumentException("[size] for [" + name + "] must be between 1 and " + MAX_SIZE + ", found [" + size + "]");
        }
        this.size = size;
        return this;
    }

    List<String> metricFields() {
        return metricFields;
    }

    int size() {
        return size;
    }

    @Override
    public TopMetricsAggregationBuilder subAggregations(Builder subFactories) {
        throw new AggregationInitializationException(
            "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
        );
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected TopMetricsAggregatorFactory doBuild(
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        Builder subfactoriesBuilder
    ) throws IOException {
        if (metricFields.isEmpty()) {
            throw new ParsingException(null, "[" + METRICS_FIELD.getPreferredName() + "] is required for aggregation [" + name + "]");
        }
        if (sorts.isEmpty()) {
            throw new ParsingException(null, "[" + SORT_FIELD.getPreferredName() + "] is required for aggregation [" + name + "]");
        }
        final Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(sorts, queryShardContext);
        return new TopMetricsAggregatorFactory(
            name,
            List.copyOf(metricFields),
            size,
            optionalSort,
            queryShardContext,
            parent,
            subfactoriesBuilder,
            metadata
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (metricFields.size() == 1) {
            builder.startObject(METRICS_FIELD.getPreferredName());
            builder.field(FIELD_FIELD.getPreferredName(), metricFields.get(0));
            builder.endObject();
        } else {
            builder.startArray(METRICS_FIELD.getPreferredName());
            for (String metricField : metricFields) {
                builder.startObject();
                builder.field(FIELD_FIELD.getPreferredName(), metricField);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.startArray(SORT_FIELD.getPreferredName());
        for (SortBuilder<?> sort : sorts) {
            sort.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(SIZE_FIELD.getPreferredName(), size);
        builder.endObject();
        return builder;
    }

    public static TopMetricsAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        TopMetricsAggregationBuilder builder = new TopMetricsAggregationBuilder(aggregationName);
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue;
            }

            if (METRICS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    builder.metricField(parseMetricField(aggregationName, parser));
                } else if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + METRICS_FIELD.getPreferredName() + "] must contain objects"
                            );
                        }
                        builder.metricField(parseMetricField(aggregationName, parser));
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token [" + token + "] for [" + METRICS_FIELD.getPreferredName() + "]"
                    );
                }
                continue;
            }

            if (SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + SORT_FIELD.getPreferredName() + "] must contain objects"
                            );
                        }
                        builder.sorts.addAll(SortBuilder.fromXContent(parser));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    builder.sorts.addAll(SortBuilder.fromXContent(parser));
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token [" + token + "] for [" + SORT_FIELD.getPreferredName() + "]"
                    );
                }
                continue;
            }

            if (SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                builder.size(parser.intValue());
                continue;
            }

            throw new ParsingException(
                parser.getTokenLocation(),
                "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "]"
            );
        }
        return builder;
    }

    private static String parseMetricField(String aggregationName, XContentParser parser) throws IOException {
        String metricField = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler()) && token.isValue()) {
                metricField = parser.text();
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for [" + NAME + "] in [" + aggregationName + "]: [" + currentFieldName + "]"
                );
            }
        }
        if (metricField == null || metricField.isEmpty()) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + FIELD_FIELD.getPreferredName() + "] is required in [" + METRICS_FIELD.getPreferredName() + "]"
            );
        }
        return metricField;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), metricFields, sorts, size);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        TopMetricsAggregationBuilder other = (TopMetricsAggregationBuilder) obj;
        return size == other.size && Objects.equals(metricFields, other.metricFields) && Objects.equals(sorts, other.sorts);
    }
}
