/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Security Logs testing dataset configuration.
 */
public final class SecurityLogsTestHelper {
    
    private SecurityLogsTestHelper() {
        // utility class
    }
    
    public static final Dataset DATASET = new Dataset(
        "security_logs",
        "security_logs"
    );
}
