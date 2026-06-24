/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency.limit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Randomness;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleUnaryOperator;
import java.util.function.IntUnaryOperator;

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.limit.AbstractLimit;

/**
 * TCP-Vegas-based adaptive concurrency limit with an {@code upDriftFactor} extension.
 * <p>
 * Extends Netflix's Vegas algorithm by scaling the alpha, beta, and threshold values
 * and widening the drift-prevention window. At the default {@code upDriftFactor = 1}
 * the output is mathematically identical to the stock Netflix {@code VegasLimit}.
 * Values {@literal >} 1 make the algorithm more permissive about raising the limit,
 * which helps during bursty workloads where requests cluster in time.
 */
public class OpenSearchVegasLimit extends AbstractLimit {

    private static final Logger LOG = LogManager.getLogger(OpenSearchVegasLimit.class);

    private static final IntUnaryOperator LOG10 = limit -> (int) Math.ceil(Math.log10(limit));

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    /** Builder for {@link OpenSearchVegasLimit}. */
    public static class Builder {
        private int initialLimit = 20;
        private int maxConcurrency = 1000;
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private double smoothing = 1.0;
        private double upDriftFactor = 1.0;

        private IntUnaryOperator alphaFunc = limit -> 3 * LOG10.applyAsInt(limit);
        private IntUnaryOperator betaFunc = limit -> 6 * LOG10.applyAsInt(limit);
        private IntUnaryOperator thresholdFunc = LOG10;
        private DoubleUnaryOperator increaseFunc = limit -> limit + LOG10.applyAsInt((int) limit);
        private DoubleUnaryOperator decreaseFunc = limit -> limit - LOG10.applyAsInt((int) limit);
        private int probeMultiplier = 30;
        private double probeInflightThreshold = 0.5;
        private int increaseHysteresis = 1;
        private int decreaseHysteresis = 1;

        private Builder() {}

        /**
         * Sets the initial concurrency limit.
         *
         * @param initialLimit the initial limit
         */
        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }

        /**
         * Sets the maximum concurrency limit.
         *
         * @param maxConcurrency the maximum limit
         */
        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }

        /**
         * Sets the smoothing factor for limit updates.
         *
         * @param smoothing the smoothing factor
         */
        public Builder smoothing(double smoothing) {
            this.smoothing = smoothing;
            return this;
        }

        /**
         * Scales alpha, beta, and threshold by this factor and widens the drift-prevention
         * window from {@code inflight * 2} to {@code inflight * (upDriftFactor + 1)}.
         * Must be &gt;= 1.0. Default is 1.0 (standard Vegas behaviour).
         * @param upDriftFactor the drift factor, must be &gt;= 1.0
         */
        public Builder upDriftFactor(double upDriftFactor) {
            this.upDriftFactor = upDriftFactor;
            return this;
        }

        /**
         * Sets the probe multiplier controlling probe frequency.
         *
         * @param probeMultiplier the probe multiplier
         */
        public Builder probeMultiplier(int probeMultiplier) {
            this.probeMultiplier = probeMultiplier;
            return this;
        }

        /**
         * Fraction of the current limit below which a probe's RTT is accepted as
         * the new no-load baseline. When {@code inflight > estimatedLimit * probeInflightThreshold},
         * the probe still fires (counter and jitter reset) but the baseline is NOT
         * updated, preventing heavy-load RTT from poisoning {@code rtt_noload}.
         * Range: [0.0, 1.0]. Default 0.5.
         *
         * @param probeInflightThreshold the threshold fraction
         */
        public Builder probeInflightThreshold(double probeInflightThreshold) {
            if (probeInflightThreshold < 0.0 || probeInflightThreshold > 1.0) {
                throw new IllegalArgumentException("probeInflightThreshold must be in [0.0, 1.0] but got " + probeInflightThreshold);
            }
            this.probeInflightThreshold = probeInflightThreshold;
            return this;
        }

        /**
         * Number of consecutive qualifying samples required before increasing the limit.
         * Default 1 (act immediately). Higher values dampen upward thrashing.
         *
         * @param increaseHysteresis the consecutive sample threshold
         */
        public Builder increaseHysteresis(int increaseHysteresis) {
            this.increaseHysteresis = increaseHysteresis;
            return this;
        }

        /**
         * Number of consecutive qualifying samples required before decreasing the limit.
         * Default 1 (act immediately). Higher values dampen downward thrashing.
         *
         * @param decreaseHysteresis the consecutive sample threshold
         */
        public Builder decreaseHysteresis(int decreaseHysteresis) {
            this.decreaseHysteresis = decreaseHysteresis;
            return this;
        }

        /**
         * Sets the metric registry for RTT sampling.
         *
         * @param registry the metric registry
         */
        public Builder metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }

        /** Builds the {@link OpenSearchVegasLimit} instance. */
        public OpenSearchVegasLimit build() {
            if (initialLimit > maxConcurrency) {
                LOG.warn("Initial limit {} exceeded maximum limit {}", initialLimit, maxConcurrency);
            }
            return new OpenSearchVegasLimit(this);
        }
    }

    /** Creates a new builder with default values. */
    public static Builder newBuilder() {
        return new Builder();
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private volatile double estimatedLimit;
    private volatile long rtt_noload = 0;
    private final int maxLimit;
    private final double smoothing;
    private final double upDriftFactor;
    private final IntUnaryOperator alphaFunc;
    private final IntUnaryOperator betaFunc;
    private final IntUnaryOperator thresholdFunc;
    private final DoubleUnaryOperator increaseFunc;
    private final DoubleUnaryOperator decreaseFunc;
    private final SampleListener rttSampleListener;
    private final int probeMultiplier;
    private final double probeInflightThreshold;
    private int probeCount = 0;
    private double probeJitter;
    // Hysteresis: consecutive qualifying samples required before acting on increase/decrease signals.
    private final int increaseHysteresis;
    private final int decreaseHysteresis;
    private int increaseConsecutive = 0;
    private int decreaseConsecutive = 0;

    private OpenSearchVegasLimit(Builder builder) {
        super(builder.initialLimit);
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.smoothing = builder.smoothing;
        this.upDriftFactor = builder.upDriftFactor;
        this.alphaFunc = builder.alphaFunc;
        this.betaFunc = builder.betaFunc;
        this.thresholdFunc = builder.thresholdFunc;
        this.increaseFunc = builder.increaseFunc;
        this.decreaseFunc = builder.decreaseFunc;
        this.probeMultiplier = builder.probeMultiplier;
        this.probeInflightThreshold = builder.probeInflightThreshold;
        this.increaseHysteresis = builder.increaseHysteresis;
        this.decreaseHysteresis = builder.decreaseHysteresis;
        this.rttSampleListener = builder.registry.distribution(MetricIds.MIN_RTT_NAME);
        resetProbeJitter();
    }

    // -------------------------------------------------------------------------
    // Vegas algorithm
    // -------------------------------------------------------------------------

    private void resetProbeJitter() {
        Random rng = Randomness.get();
        probeJitter = 0.5 + rng.nextDouble() * 0.5;
    }

    private boolean shouldProbe() {
        return probeJitter * probeMultiplier * estimatedLimit <= probeCount;
    }

    @Override
    protected int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        if (rtt <= 0) {
            throw new IllegalArgumentException("rtt must be >0 but got " + rtt);
        }

        probeCount++;
        if (this.shouldProbe()) {
            this.resetProbeJitter();
            this.probeCount = 0;
            if (inflight <= estimatedLimit * probeInflightThreshold) {
                LOG.debug(
                    "Probe accepted MinRTT {} ms (inflight={}, limit={})",
                    (double) TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0,
                    inflight,
                    (int) estimatedLimit
                );
                this.rtt_noload = rtt;
            } else {
                LOG.debug(
                    "Probe skipped baseline reset: inflight {} exceeds {}% of limit {}",
                    inflight,
                    (int) (probeInflightThreshold * 100),
                    (int) estimatedLimit
                );
            }
            return (int) this.estimatedLimit;
        } else if (this.rtt_noload != 0L && rtt >= this.rtt_noload) {
            this.rttSampleListener.addSample(this.rtt_noload);
            return this.updateEstimatedLimit(rtt, inflight, didDrop);
        } else {
            LOG.debug("New MinRTT {}", (double) TimeUnit.NANOSECONDS.toMicros(rtt) / (double) 1000.0F);
            this.rtt_noload = rtt;
            return (int) this.estimatedLimit;
        }
    }

    private int updateEstimatedLimit(long rtt, int inflight, boolean didDrop) {
        double estimatedLimit = this.estimatedLimit;
        final int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double) rtt_noload / rtt));
        int alpha = -1, beta = -1, threshold = -1;
        double newLimit;
        if (didDrop) {
            // Hard signal: bypass hysteresis, act immediately.
            increaseConsecutive = 0;
            decreaseConsecutive = 0;
            newLimit = decreaseFunc.applyAsDouble(estimatedLimit);
        } else if (inflight * (upDriftFactor + 1) < estimatedLimit) {
            increaseConsecutive = 0;
            decreaseConsecutive = 0;
            return (int) estimatedLimit;
        } else {
            alpha = (int) upDriftFactor * alphaFunc.applyAsInt((int) estimatedLimit);
            beta = (int) upDriftFactor * betaFunc.applyAsInt((int) estimatedLimit);
            threshold = (int) upDriftFactor * thresholdFunc.applyAsInt((int) estimatedLimit);

            if (queueSize <= threshold || queueSize < alpha) {
                // Increase zone: require increaseHysteresis consecutive qualifying samples.
                decreaseConsecutive = 0;
                if (++increaseConsecutive < increaseHysteresis) return (int) estimatedLimit;
                increaseConsecutive = 0;
                newLimit = queueSize <= threshold ? estimatedLimit + beta : increaseFunc.applyAsDouble(estimatedLimit);
            } else if (queueSize > beta) {
                // Decrease zone: require decreaseHysteresis consecutive qualifying samples.
                increaseConsecutive = 0;
                if (++decreaseConsecutive < decreaseHysteresis) return (int) estimatedLimit;
                decreaseConsecutive = 0;
                newLimit = decreaseFunc.applyAsDouble(estimatedLimit);
            } else {
                increaseConsecutive = 0;
                decreaseConsecutive = 0;
                return (int) estimatedLimit;
            }
        }

        newLimit = Math.max(1, Math.min(maxLimit, newLimit));
        newLimit = (1 - smoothing) * estimatedLimit + smoothing * newLimit;
        if ((int) newLimit != (int) estimatedLimit && LOG.isDebugEnabled()) {
            LOG.debug(
                "New limit={} old limit = {} minRtt={} ms winRtt={} ms queueSize={} alpha={} beta={} threshold={}",
                (int) newLimit,
                (int) estimatedLimit,
                TimeUnit.NANOSECONDS.toMicros(rtt_noload) / 1000.0,
                TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0,
                queueSize,
                alpha,
                beta,
                threshold
            );
        }
        this.estimatedLimit = newLimit;
        return (int) newLimit;
    }

    /**
     * Returns the no-load baseline RTT in the requested time unit.
     *
     * @param units the time unit for the result
     */
    public long getRttNoLoad(TimeUnit units) {
        return units.convert(rtt_noload, TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "OpenSearchVegasLimit [limit="
            + getLimit()
            + ", rtt_noload="
            + TimeUnit.NANOSECONDS.toMicros(rtt_noload) / 1000.0
            + " ms"
            + ", upDriftFactor="
            + upDriftFactor
            + ", probeInflightThreshold="
            + probeInflightThreshold
            + "]";
    }
}
