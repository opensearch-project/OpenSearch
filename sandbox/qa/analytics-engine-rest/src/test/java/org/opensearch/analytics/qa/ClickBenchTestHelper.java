/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper constants for the ClickBench dataset.
 * <p>
 * Provisioned via {@link DatasetProvisioner} using resources from {@code datasets/clickbench/}.
 */
public final class ClickBenchTestHelper {

    /** ClickBench dataset descriptor. */
    public static final Dataset DATASET = new Dataset("clickbench", "parquet_hits");

    private ClickBenchTestHelper() {
        // utility class
    }
}
