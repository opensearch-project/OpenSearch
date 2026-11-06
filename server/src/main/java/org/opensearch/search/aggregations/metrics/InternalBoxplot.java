/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.tdunning.math.stats.Centroid;

/**
 * Implementation of the boxplot agg. The statistics are approximated from a t-digest of the values, except for
 * the minimum and maximum which the digest tracks exactly.
 *
 * @opensearch.internal
 */
public class InternalBoxplot extends InternalNumericMetricsAggregation.MultiValue implements Boxplot {

    /**
     * The name of the aggregation, which is also its type in requests and responses.
     */
    public static final String NAME = "boxplot";

    private static final double WHISKER_IQR_MULTIPLIER = 1.5;

    /**
     * The statistics of a boxplot, in the order {@link #computeStats(TDigestState)} returns them.
     *
     * @opensearch.internal
     */
    enum Metrics {
        min,
        max,
        q1,
        q2,
        q3,
        lower,
        upper;

        static Metrics resolve(String name) {
            try {
                return Metrics.valueOf(name);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown value [" + name + "] in boxplot aggregation", e);
            }
        }

        /**
         * @return the name of the field holding the formatted value of this statistic
         */
        String asStringName() {
            return name() + "_as_string";
        }
    }

    /**
     * Computes the statistics of the values in the given digest. The minimum and maximum are exact. The quartiles and the whiskers
     * are approximations derived from the digest, so a whisker may not be one of the values.
     *
     * @return {@code [min, max, q1, q2, q3, lower, upper]}, or all {@code NaN} when the digest holds no values
     */
    static double[] computeStats(TDigestState state) {
        final double[] stats = new double[Metrics.values().length];
        if (state.size() == 0) {
            Arrays.fill(stats, Double.NaN);
            return stats;
        }

        final double min = state.getMin();
        final double max = state.getMax();
        final double q1 = state.quantile(0.25);
        final double q2 = state.quantile(0.5);
        final double q3 = state.quantile(0.75);
        final double iqr = q3 - q1;
        final double lowFence = q1 - WHISKER_IQR_MULTIPLIER * iqr;
        final double highFence = q3 + WHISKER_IQR_MULTIPLIER * iqr;

        // The whiskers are the most extreme values inside the fences, so they are the exact extremes unless those are outliers
        double lower = min < lowFence ? lowerWhisker(state, lowFence) : min;
        double upper = max > highFence ? upperWhisker(state, highFence) : max;
        // Keep the whiskers between the extremes and the quartiles, whatever the sketch approximated
        lower = Math.min(Math.max(lower, min), q1);
        upper = Math.max(Math.min(upper, max), q3);

        return new double[] { min, max, q1, q2, q3, lower, upper };
    }

    /**
     * Approximates the smallest value that is not below the low fence. Only centroids are known, not the values themselves. When the
     * centroids on both sides of the fence are single values, no value can lie between them, so the one above the fence is the exact
     * whisker. Otherwise the values near the fence are dense, and the whisker is read from the digest's distribution at the fence.
     */
    private static double lowerWhisker(TDigestState state, double lowFence) {
        Centroid below = null;
        Centroid above = null;
        for (Centroid centroid : state.centroids()) {
            if (centroid.mean() >= lowFence) {
                above = centroid;
                break;
            }
            below = centroid;
        }
        if (isSingleValue(below) && isSingleValue(above)) {
            return above.mean();
        }
        return Math.max(lowFence, state.quantile(state.cdf(lowFence)));
    }

    /**
     * Approximates the largest value that is not above the high fence, mirroring {@link #lowerWhisker(TDigestState, double)}.
     */
    private static double upperWhisker(TDigestState state, double highFence) {
        Centroid below = null;
        Centroid above = null;
        for (Centroid centroid : state.centroids()) {
            if (centroid.mean() > highFence) {
                above = centroid;
                break;
            }
            below = centroid;
        }
        if (isSingleValue(below) && isSingleValue(above)) {
            return below.mean();
        }
        return Math.min(highFence, state.quantile(state.cdf(highFence)));
    }

    private static boolean isSingleValue(Centroid centroid) {
        return centroid != null && centroid.count() == 1;
    }

    private final TDigestState state;
    private final double[] stats;

    InternalBoxplot(String name, Map<String, Object> metadata, DocValueFormat format, TDigestState state) {
        super(name, metadata);
        this.format = Objects.requireNonNull(format);
        this.state = Objects.requireNonNull(state);
        this.stats = computeStats(this.state);
    }

    /**
     * Read from a stream.
     */
    public InternalBoxplot(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        state = TDigestState.read(in);
        stats = computeStats(state);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        TDigestState.write(state, out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        final TDigestState merged = new TDigestState(state.compression());
        for (InternalAggregation aggregation : aggregations) {
            merged.add(((InternalBoxplot) aggregation).state);
        }
        return new InternalBoxplot(name, metadata, format, merged);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean anyResults = state.size() > 0;
        for (Metrics metric : Metrics.values()) {
            if (anyResults) {
                final double value = stat(metric);
                builder.field(metric.name(), value);
                if (format != DocValueFormat.RAW) {
                    builder.field(metric.asStringName(), format.format(value).toString());
                }
            } else {
                builder.nullField(metric.name());
            }
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), state);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalBoxplot other = (InternalBoxplot) obj;
        return Objects.equals(state, other.state);
    }

    TDigestState getState() {
        return state;
    }

    private double stat(Metrics metric) {
        return stats[metric.ordinal()];
    }

    @Override
    public double value(String name) {
        return stat(Metrics.resolve(name));
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
        return valueAsString(Metrics.min.name());
    }

    @Override
    public String getMaxAsString() {
        return valueAsString(Metrics.max.name());
    }

    @Override
    public String getQ1AsString() {
        return valueAsString(Metrics.q1.name());
    }

    @Override
    public String getQ2AsString() {
        return valueAsString(Metrics.q2.name());
    }

    @Override
    public String getQ3AsString() {
        return valueAsString(Metrics.q3.name());
    }

    @Override
    public String getLowerAsString() {
        return valueAsString(Metrics.lower.name());
    }

    @Override
    public String getUpperAsString() {
        return valueAsString(Metrics.upper.name());
    }
}
