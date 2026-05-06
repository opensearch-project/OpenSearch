/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based test for {@link DatafusionSettings} out-of-range validation rejection.
 * <p>
 * Feature: dynamic-indexed-query-settings, Property 3: Out-of-range validation rejection
 * <p>
 * Validates: Requirements 4.1, 4.2, 4.3
 * <p>
 * For any double value {@code v} where {@code v < 0.0} or {@code v > 1.0}, attempting to
 * parse {@code datafusion.indexed.min_skip_run_selectivity_threshold} with value {@code v}
 * SHALL throw an {@code IllegalArgumentException}. Similarly, for any integer value
 * {@code n < 1}, attempting to parse {@code datafusion.indexed.batch_size} or
 * {@code datafusion.indexed.max_collector_parallelism} with value {@code n} SHALL throw
 * an {@code IllegalArgumentException}.
 */
public class DatafusionSettingsValidationPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    /**
     * **Validates: Requirements 4.1**
     * <p>
     * Property 3: Out-of-range validation rejection — for any integer value less than 1,
     * attempting to parse {@code datafusion.indexed.batch_size} SHALL throw an
     * {@code IllegalArgumentException}.
     */
    public void testBatchSizeBelowMinimumIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            int invalidValue = randomIntBetween(Integer.MIN_VALUE, 0);
            Settings settings = Settings.builder().put("datafusion.indexed.batch_size", invalidValue).build();
            expectThrows(
                IllegalArgumentException.class,
                "batch_size=" + invalidValue + " must be rejected",
                () -> DatafusionSettings.INDEXED_BATCH_SIZE.get(settings)
            );
        }
    }

    /**
     * **Validates: Requirements 4.2**
     * <p>
     * Property 3: Out-of-range validation rejection — for any double value greater than
     * 1.0, attempting to parse {@code datafusion.indexed.min_skip_run_selectivity_threshold}
     * SHALL throw an {@code IllegalArgumentException}.
     */
    public void testSelectivityThresholdAboveMaxIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            double invalidValue = 1.0 + randomDoubleBetween(0.001, 1000.0, true);
            Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", invalidValue).build();
            double finalInvalidValue = invalidValue;
            expectThrows(
                IllegalArgumentException.class,
                "min_skip_run_selectivity_threshold=" + finalInvalidValue + " must be rejected",
                () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings)
            );
        }
    }

    /**
     * **Validates: Requirements 4.2**
     * <p>
     * Property 3: Out-of-range validation rejection — for any double value less than 0.0,
     * attempting to parse {@code datafusion.indexed.min_skip_run_selectivity_threshold}
     * SHALL throw an {@code IllegalArgumentException}.
     */
    public void testSelectivityThresholdBelowMinIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            double invalidValue = -randomDoubleBetween(0.001, 1000.0, true);
            Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", invalidValue).build();
            double finalInvalidValue = invalidValue;
            expectThrows(
                IllegalArgumentException.class,
                "min_skip_run_selectivity_threshold=" + finalInvalidValue + " must be rejected",
                () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings)
            );
        }
    }

    /**
     * **Validates: Requirements 4.3**
     * <p>
     * Property 3: Out-of-range validation rejection — for any integer value less than 1,
     * attempting to parse {@code datafusion.indexed.max_collector_parallelism} SHALL throw
     * an {@code IllegalArgumentException}.
     */
    public void testMaxCollectorParallelismBelowMinimumIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            int invalidValue = randomIntBetween(Integer.MIN_VALUE, 0);
            Settings settings = Settings.builder().put("datafusion.indexed.max_collector_parallelism", invalidValue).build();
            expectThrows(
                IllegalArgumentException.class,
                "max_collector_parallelism=" + invalidValue + " must be rejected",
                () -> DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(settings)
            );
        }
    }
}
