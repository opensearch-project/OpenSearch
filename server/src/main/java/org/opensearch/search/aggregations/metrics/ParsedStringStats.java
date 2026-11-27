/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ObjectParser.ValueType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.search.aggregations.metrics.InternalStringStats.Metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A string_stats agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedStringStats extends ParsedAggregation implements StringStats {

    private static final ObjectParser<ParsedStringStats, Void> PARSER = new ObjectParser<>(
        ParsedStringStats.class.getSimpleName(),
        true,
        ParsedStringStats::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> agg.count = value, new ParseField(Metrics.count.name()));
        // the statistics are null in a response without values, which parses to NaN
        PARSER.declareField(
            (agg, value) -> agg.minLength = value,
            (parser, context) -> parseDouble(parser, Double.NaN),
            new ParseField(Metrics.min_length.name()),
            ValueType.DOUBLE_OR_NULL
        );
        PARSER.declareField(
            (agg, value) -> agg.maxLength = value,
            (parser, context) -> parseDouble(parser, Double.NaN),
            new ParseField(Metrics.max_length.name()),
            ValueType.DOUBLE_OR_NULL
        );
        PARSER.declareField(
            (agg, value) -> agg.avgLength = value,
            (parser, context) -> parseDouble(parser, Double.NaN),
            new ParseField(Metrics.avg_length.name()),
            ValueType.DOUBLE_OR_NULL
        );
        PARSER.declareField(
            (agg, value) -> agg.entropy = value,
            (parser, context) -> parseDouble(parser, Double.NaN),
            new ParseField(Metrics.entropy.name()),
            ValueType.DOUBLE_OR_NULL
        );
        PARSER.declareField((agg, value) -> {
            agg.distributionShown = true;
            agg.distribution = value;
        }, ParsedStringStats::parseDistribution, new ParseField(InternalStringStats.DISTRIBUTION_FIELD), ValueType.OBJECT_OR_NULL);
    }

    private static Map<String, Double> parseDistribution(XContentParser parser, Void context) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return Collections.emptyMap();
        }
        return parser.map(LinkedHashMap::new, XContentParser::doubleValue);
    }

    /**
     * Parses the body of a string_stats aggregation.
     */
    public static ParsedStringStats fromXContent(XContentParser parser, String name) {
        ParsedStringStats parsedStringStats = PARSER.apply(parser, null);
        parsedStringStats.setName(name);
        return parsedStringStats;
    }

    private long count;
    private double minLength = Double.NaN;
    private double maxLength = Double.NaN;
    private double avgLength = Double.NaN;
    private double entropy = Double.NaN;
    private Map<String, Double> distribution = Collections.emptyMap();
    private boolean distributionShown;

    ParsedStringStats() {}

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getMinLength() {
        return minLength;
    }

    @Override
    public double getMaxLength() {
        return maxLength;
    }

    @Override
    public double getAvgLength() {
        return avgLength;
    }

    @Override
    public double getEntropy() {
        return entropy;
    }

    @Override
    public Map<String, Double> getDistribution() {
        return distribution;
    }

    @Override
    public String getType() {
        return InternalStringStats.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Metrics.count.name(), count);
        writeLength(builder, Metrics.min_length, minLength);
        writeLength(builder, Metrics.max_length, maxLength);
        writeStatistic(builder, Metrics.avg_length, avgLength);
        writeStatistic(builder, Metrics.entropy, entropy);
        if (distributionShown) {
            if (count == 0) {
                builder.nullField(InternalStringStats.DISTRIBUTION_FIELD);
            } else {
                builder.startObject(InternalStringStats.DISTRIBUTION_FIELD);
                for (Map.Entry<String, Double> entry : distribution.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue().doubleValue());
                }
                builder.endObject();
            }
        }
        return builder;
    }

    private static void writeStatistic(XContentBuilder builder, Metrics metric, double value) throws IOException {
        if (Double.isNaN(value)) {
            builder.nullField(metric.name());
        } else {
            builder.field(metric.name(), value);
        }
    }

    /** Writes a length the way it is in the response, as a whole number. */
    private static void writeLength(XContentBuilder builder, Metrics metric, double value) throws IOException {
        if (Double.isNaN(value)) {
            builder.nullField(metric.name());
        } else {
            builder.field(metric.name(), (long) value);
        }
    }
}
