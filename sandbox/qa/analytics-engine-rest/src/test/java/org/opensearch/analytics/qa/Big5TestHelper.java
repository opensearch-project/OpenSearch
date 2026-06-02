/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper constants for the big5 PPL benchmark dataset.
 * <p>
 * Provisioned via {@link DatasetProvisioner} using resources from {@code datasets/big5/}.
 * Mirrors the upstream
 * <a href="https://github.com/opensearch-project/opensearch-benchmark-workloads/tree/main/big5">big5 workload</a>
 * with a small synthetic dataset and the 46 PPL queries from {@code operations/ppl.json}.
 */
public final class Big5TestHelper {

    /** Big5 dataset descriptor. */
    public static final Dataset DATASET = new Dataset("big5", "big5");

    private Big5TestHelper() {
        // utility class
    }
}
