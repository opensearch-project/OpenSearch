/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.tracker;

import org.opensearch.search.querygroup.QueryGroupLevelResourceUsageView;

import java.util.Map;

/**
 * This interface is mainly for tracking the resourceLimitGroup level resource usages
 */
public interface QueryGroupUsageTracker {
    /**
     * updates the current resource usage of resourceLimitGroups
     */

    Map<String, QueryGroupLevelResourceUsageView> constructQueryGroupLevelUsageViews();
}
