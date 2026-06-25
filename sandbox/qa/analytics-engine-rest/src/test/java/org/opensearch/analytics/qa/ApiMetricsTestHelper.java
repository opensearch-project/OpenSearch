/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for API Metrics dataset configuration.
 */
public final class ApiMetricsTestHelper {

    private ApiMetricsTestHelper() {
        // utility class
    }

    public static final Dataset DATASET = new Dataset(
        "api_metrics",
        "api_metrics"
    );
}
