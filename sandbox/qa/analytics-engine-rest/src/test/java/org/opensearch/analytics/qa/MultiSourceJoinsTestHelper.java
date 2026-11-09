/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Complex Redesigned (multi-index) testing dataset configuration.
 */
public final class MultiSourceJoinsTestHelper {
    
    private MultiSourceJoinsTestHelper() {
        // utility class
    }
    
    public static final Dataset DATASET = new Dataset(
        "multi_source_joins",
        "app_monitor",
        "event_processor",
        "monitor_tracking",
        "tax_withholding",
        "voice_verification"
    );
}
