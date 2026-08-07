/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.aggregations.metrics.Percentiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Percentiles response holding engine-computed final values — the server's
 * {@code InternalTDigestPercentiles} wraps a live digest, which this path does not have.
 * Renders legacy-identical JSON (keyed and non-keyed, {@code _as_string} for non-RAW
 * formats) and reports the legacy type name for {@code typed_keys}. Never crosses the
 * wire on this path; wire methods exist only to satisfy {@link InternalAggregation}.
 */
public class InternalDslPercentiles extends InternalNumericMetricsAggregation.MultiValue implements Percentiles {

    /** Matches the legacy tdigest percentiles type for typed_keys response parity. */
    static final String TYPE_NAME = "tdigest_percentiles";

    private final double[] percents;
    private final double[] values;   // NaN = no value (empty result set)
    private final boolean keyed;

    /**
     * Creates a percentiles response.
     *
     * @param name the aggregation name
     * @param percents requested percents, in request order
     * @param values one value per percent ({@code NaN} when execution produced none)
     * @param keyed whether to render the keyed (object) or non-keyed (array) form
     * @param format value format; non-RAW formats add {@code _as_string} companions
     */
    public InternalDslPercentiles(String name, double[] percents, double[] values, boolean keyed, DocValueFormat format) {
        super(name, null);
        if (percents.length != values.length) {
            throw new IllegalArgumentException("percents and values length mismatch");
        }
        this.percents = percents;
        this.values = values;
        this.keyed = keyed;
        this.format = Objects.requireNonNull(format, "format must not be null");
    }

    @Override
    public String getWriteableName() {
        return TYPE_NAME;
    }

    @Override
    public double percentile(double percent) {
        for (int i = 0; i < percents.length; i++) {
            if (percents[i] == percent) {
                return values[i];
            }
        }
        throw new IllegalArgumentException("percent requested [" + percent + "] was not computed");
    }

    @Override
    public String percentileAsString(double percent) {
        return format.format(percentile(percent)).toString();
    }

    @Override
    public double value(String name) {
        return percentile(Double.parseDouble(name));
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iterator<>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < percents.length;
            }

            @Override
            public Percentile next() {
                Percentile p = new Percentile(percents[i], values[i]);
                i++;
                return p;
            }
        };
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.VALUES.getPreferredName());
            for (int i = 0; i < percents.length; i++) {
                String key = String.valueOf(percents[i]);
                builder.field(key, Double.isNaN(values[i]) ? null : values[i]);
                if (format != DocValueFormat.RAW && Double.isNaN(values[i]) == false) {
                    builder.field(key + "_as_string", format.format(values[i]).toString());
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES.getPreferredName());
            for (int i = 0; i < percents.length; i++) {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), percents[i]);
                builder.field(CommonFields.VALUE.getPreferredName(), Double.isNaN(values[i]) ? null : values[i]);
                if (format != DocValueFormat.RAW && Double.isNaN(values[i]) == false) {
                    builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(values[i]).toString());
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(percents);
        out.writeDoubleArray(values);
        out.writeBoolean(keyed);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("percentiles are not reduced on the DSL analytics path");
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        if (path.size() == 1) {
            return value(path.get(0));
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(percents), Arrays.hashCode(values), keyed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalDslPercentiles other = (InternalDslPercentiles) obj;
        return keyed == other.keyed && Arrays.equals(percents, other.percents) && Arrays.equals(values, other.values);
    }
}
