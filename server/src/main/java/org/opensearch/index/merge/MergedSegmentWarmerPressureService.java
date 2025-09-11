/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;

import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Service that applies throttling rules to determine if merged segment warming should proceed.
 * Evaluates conditions like concurrency limits and applies backpressure when thresholds are exceeded.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class MergedSegmentWarmerPressureService {

    private final Logger logger;

    private final PressureSettings pressureSettings;

    private final List<Rule> throttleRules;

    public MergedSegmentWarmerPressureService(IndexShard indexShard) {
        this.pressureSettings = new PressureSettings(indexShard);
        this.logger = Loggers.getLogger(MergedSegmentWarmerPressureService.class, indexShard.shardId());
        this.throttleRules = List.of(new ConcurrencyLimiterRule(indexShard, pressureSettings));
    }

    public boolean isEnabled() {
        return pressureSettings.isEnabled();
    }

    /**
     * Determines if warming should proceed by evaluating all throttle conditions.
     * Returns false on the first failing predicate and logs the rejection reason.
     *
     * @param stats MergedSegmentWarmerStats snapshot at the time of invocation of warm
     * @return true if all predicates pass, false if any predicate fails
     */
    public boolean shouldWarm(MergedSegmentWarmerStats stats) {
        return throttleRules.stream().allMatch(throttlePredicate -> {
            boolean res = throttlePredicate.test(stats);
            if (res == false && logger.isTraceEnabled()) logger.trace(throttlePredicate.rejectionMessage(stats));
            return res;
        });
    }

    /**
     * Abstract class to check if merged segment warm needs to be throttled.
     *
     * @opensearch.internal
     */
    private static abstract class Rule implements Predicate<MergedSegmentWarmerStats> {

        final PressureSettings pressureSettings;
        final IndexShard indexShard;

        private Rule(IndexShard indexShard, PressureSettings pressureSettings) {
            this.pressureSettings = pressureSettings;
            this.indexShard = indexShard;
        }

        /**
         * Returns the name of the rule.
         *
         * @return the name using class name.
         */
        abstract String name();

        String rejectionMessage(MergedSegmentWarmerStats statsSnapshot) {
            return String.format(Locale.ROOT, "Merged segment warm rejected for shard [%s] by rule: %s | ", indexShard.shardId(), name());
        }
    }

    /**
     * Predicate that limits concurrent segment warming operations to prevent blocking merges.
     * This is important because if all threads are blocked in merge operations and segment warming
     * is slow, we don't want to block new merges from proceeding. The rule ensures there are
     * always enough threads available for merge operations by limiting concurrent warm operations
     * based on a configurable factor of the maximum concurrent merges.
     */
    private static class ConcurrencyLimiterRule extends Rule {
        private final String NAME = "Concurrency limiter rule for merged segment warmer throttling";

        private ConcurrencyLimiterRule(IndexShard indexShard, PressureSettings pressureSettings) {
            super(indexShard, pressureSettings);
        }

        private long calculateMaxAllowedConcurrentWarms(int maxConcurrentMerges) {
            return (long) (pressureSettings.getMaxConcurrentWarmsFactor() * maxConcurrentMerges);
        }

        @Override
        String name() {
            return NAME;
        }

        @Override
        String rejectionMessage(MergedSegmentWarmerStats stats) {
            long maxAllowed = calculateMaxAllowedConcurrentWarms(indexShard.getMaxMergesAllowed());
            return super.rejectionMessage(stats) + String.format(
                Locale.ROOT,
                "Current ongoing warms: %d, Max allowed: %d",
                stats.getOngoingCount(),
                maxAllowed
            );
        }

        @Override
        public boolean test(MergedSegmentWarmerStats statsSnapshot) {
            long onGoingWarms = statsSnapshot.getOngoingCount();
            long maxAllowedWarms = calculateMaxAllowedConcurrentWarms(indexShard.getMaxMergesAllowed());
            return maxAllowedWarms > onGoingWarms;
        }
    }

    /**
     * Settings related to back pressure for MergedSegmentWarmer throttling.
     *
     * @opensearch.internal
     */
    private static class PressureSettings {
        IndexShard indexShard;

        PressureSettings(IndexShard indexShard) {
            this.indexShard = indexShard;
        }

        private IndexSettings indexSettings() {
            return indexShard.indexSettings();
        }

        boolean isEnabled() {
            return indexSettings().isMergedSegmentWarmerPressureEnabled();
        }

        public double getMaxConcurrentWarmsFactor() {
            return indexSettings().getMaxConcurrentMergedSegmentWarmsFactor();
        }
    }
}
