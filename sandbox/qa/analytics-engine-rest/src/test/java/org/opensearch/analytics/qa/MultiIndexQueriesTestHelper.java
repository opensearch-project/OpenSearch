/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Additional Commands testing dataset configuration (multi-index).
 * Uses existing indexes from other datasets.
 */
public final class MultiIndexQueriesTestHelper {

    private MultiIndexQueriesTestHelper() {
        // utility class
    }

    public static final Dataset DATASET = new Dataset(
        "multi_index_queries",
        "security_logs",
        "api_metrics",
        "performance_metrics",
        "exception_logs"
    );
}
