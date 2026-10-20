/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Complex Joins testing dataset configuration (multi-index).
 * Uses existing indexes from other datasets.
 */
public final class ComplexJoinsTestHelper {
    
    private ComplexJoinsTestHelper() {
        // utility class
    }
    
    public static final Dataset DATASET = new Dataset(
        "complex_joins",
        "security_logs",
        "app_monitor",
        "kubernetes_logs",
        "monitor_tracking",
        "performance_metrics",
        "voice_verification"
    );
}
