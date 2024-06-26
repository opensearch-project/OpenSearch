/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query_group.tracker;

/**
 * This interface is mainly for tracking the QueryGroup level resource usages
 */
public interface QueryGroupResourceUsageTracker {
    /**
     * updates the current resource usage of QueryGroups
     */
    public void updateQueryGroupsResourceUsage();
}
