/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.wlm.tracker;

import org.opensearch.search.wlm.QueryGroupLevelResourceUsageView;

import java.util.Map;

/**
 * This interface is mainly for tracking the queryGroup level resource usages
 */
public interface QueryGroupUsageTracker {
    /**
     * updates the current resource usage of queryGroup
     */

    Map<String, QueryGroupLevelResourceUsageView> constructQueryGroupLevelUsageViews();
}
