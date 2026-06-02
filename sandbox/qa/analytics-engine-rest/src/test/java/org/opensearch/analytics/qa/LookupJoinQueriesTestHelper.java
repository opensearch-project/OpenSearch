/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Advanced Commands testing dataset configuration.
 */
public final class LookupJoinQueriesTestHelper {

    private LookupJoinQueriesTestHelper() {
        // utility class
    }

    public static final Dataset DATASET = new Dataset(
        "lookup_join_queries",
        "sales_data",
        "user_lookup"
    );
}
