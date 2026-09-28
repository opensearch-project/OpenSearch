/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Complex Splunk Redesign testing dataset configuration (multi-index).
 */
public final class LookupTableQueriesTestHelper {
    
    private LookupTableQueriesTestHelper() {
        // utility class
    }
    
    public static final Dataset DATASET = new Dataset(
        "lookup_table_queries",
        "app_logs_splunk",
        "view_lookup"
    );
}
