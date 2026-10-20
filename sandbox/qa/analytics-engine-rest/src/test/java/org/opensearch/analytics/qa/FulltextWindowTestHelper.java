/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Full-text search with window functions testing dataset configuration (multi-index).
 */
public final class FulltextWindowTestHelper {
    
    private FulltextWindowTestHelper() {
        // utility class
    }
    
    public static final Dataset DATASET = new Dataset(
        "fulltext_window",
        "app_logs",
        "metrics",
        "transactions"
    );
}
