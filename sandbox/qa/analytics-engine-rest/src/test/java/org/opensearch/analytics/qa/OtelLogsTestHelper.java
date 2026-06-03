/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for OTel logs dataset configuration.
 */
public final class OtelLogsTestHelper {

    private OtelLogsTestHelper() {
        // utility class
    }

    public static final Dataset DATASET = new Dataset(
        "otel_logs",
        "otel_logs"
    );
}
