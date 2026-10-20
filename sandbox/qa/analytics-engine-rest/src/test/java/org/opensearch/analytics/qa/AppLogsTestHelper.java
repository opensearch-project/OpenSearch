/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Application log analysis dataset configuration.
 */
public final class AppLogsTestHelper {
    
    private AppLogsTestHelper() {
        // utility class
    }
    
    public static final Dataset DATASET = new Dataset(
        "app_logs",
        "app_logs"
    );
}
