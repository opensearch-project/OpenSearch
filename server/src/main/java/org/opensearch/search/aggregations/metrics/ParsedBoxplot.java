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
import org.opensearch.search.aggregations.metrics.InternalBoxplot.Metrics;

import java.io.IOException;
import java.util.Arrays;

/**
 * A boxplot agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedBoxplot extends ParsedAggregation implements Boxplot {

    private static final ObjectParser<ParsedBoxplot, Void> PARSER = new ObjectParser<>(
        ParsedBoxplot.class.getSimpleName(),
        true,
        ParsedBoxplot::new
    );

    static {
        declareAggregationFields(PARSER);
        for (Metrics metric : Metrics.values()) {
            final int index = metric.ordinal();
            PARSER.declareField(
                (agg, value) -> agg.stats[index] = value,
                (parser, context) -> parseDouble(parser, Double.NaN),
                new ParseField(metric.name()),
                ValueType.DOUBLE_OR_NULL
            );
            PARSER.declareString((agg, value) -> agg.statsAsString[index] = value, new ParseField(metric.asStringName()));
        }
    }

    /**
     * Parses the body of a boxplot aggregation.
     */
    public static ParsedBoxplot fromXContent(XContentParser parser, String name) {
        ParsedBoxplot parsedBoxplot = PARSER.apply(parser, null);
        parsedBoxplot.setName(name);
        return parsedBoxplot;
    }

    private final double[] stats = new double[Metrics.values().length];
    private final String[] statsAsString = new String[Metrics.values().length];

    ParsedBoxplot() {
        // NaN is what a response without values parses to
        Arrays.fill(stats, Double.NaN);
    }

    private double stat(Metrics metric) {
        return stats[metric.ordinal()];
    }

    private String statAsString(Metrics metric) {
        final String asString = statsAsString[metric.ordinal()];
        return asString != null ? asString : Double.toString(stat(metric));
    }

    @Override
    public double getMin() {
        return stat(Metrics.min);
    }

    @Override
    public double getMax() {
        return stat(Metrics.max);
    }

    @Override
    public double getQ1() {
        return stat(Metrics.q1);
    }

    @Override
    public double getQ2() {
        return stat(Metrics.q2);
    }

    @Override
    public double getQ3() {
        return stat(Metrics.q3);
    }

    @Override
    public double getLower() {
        return stat(Metrics.lower);
    }

    @Override
    public double getUpper() {
        return stat(Metrics.upper);
    }

    @Override
    public String getMinAsString() {
        return statAsString(Metrics.min);
    }

    @Override
    public String getMaxAsString() {
        return statAsString(Metrics.max);
    }

    @Override
    public String getQ1AsString() {
        return statAsString(Metrics.q1);
    }

    @Override
    public String getQ2AsString() {
        return statAsString(Metrics.q2);
    }

    @Override
    public String getQ3AsString() {
        return statAsString(Metrics.q3);
    }

    @Override
    public String getLowerAsString() {
        return statAsString(Metrics.lower);
    }

    @Override
    public String getUpperAsString() {
        return statAsString(Metrics.upper);
    }

    @Override
    public String getType() {
        return InternalBoxplot.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        for (Metrics metric : Metrics.values()) {
            final double value = stat(metric);
            if (Double.isNaN(value)) {
                builder.nullField(metric.name());
            } else {
                builder.field(metric.name(), value);
                final String asString = statsAsString[metric.ordinal()];
                if (asString != null) {
                    builder.field(metric.asStringName(), asString);
                }
            }
        }
        return builder;
    }
}
