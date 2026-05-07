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

public class DatafusionSettingsValidationPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    public void testBatchSizeBelowMinimumIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            int invalidValue = randomIntBetween(Integer.MIN_VALUE, 0);
            Settings settings = Settings.builder().put("datafusion.indexed.batch_size", invalidValue).build();
            expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_BATCH_SIZE.get(settings));
        }
    }

    public void testSelectivityThresholdAboveMaxIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            double invalidValue = 1.0 + randomDoubleBetween(0.001, 1000.0, true);
            Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", invalidValue).build();
            expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings));
        }
    }

    public void testSelectivityThresholdBelowMinIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            double invalidValue = -randomDoubleBetween(0.001, 1000.0, true);
            Settings settings = Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", invalidValue).build();
            expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(settings));
        }
    }

    public void testMaxCollectorParallelismBelowMinimumIsRejected() {
        for (int i = 0; i < ITERATIONS; i++) {
            int invalidValue = randomIntBetween(Integer.MIN_VALUE, 0);
            Settings settings = Settings.builder().put("datafusion.indexed.max_collector_parallelism", invalidValue).build();
            expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.INDEXED_MAX_COLLECTOR_PARALLELISM.get(settings));
        }
    }
}
