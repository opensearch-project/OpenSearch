/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Extensive function coverage testing dataset configuration (multi-index).
 */
public final class ExtensiveCoverageTestHelper {

    private ExtensiveCoverageTestHelper() {
        // utility class
    }

    public static final Dataset DATASET = new Dataset(
        "extensive_coverage",
        "test_data",
        "lookup"
    );
}
