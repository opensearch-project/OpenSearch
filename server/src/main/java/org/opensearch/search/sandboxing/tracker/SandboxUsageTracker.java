/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.tracker;

import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;

import java.util.Map;

/**
 * This interface is mainly for tracking the resourceLimitGroup level resource usages
 */
public interface SandboxUsageTracker {
    /**
     * updates the current resource usage of resourceLimitGroups
     */

    Map<String, SandboxLevelResourceUsageView> constructSandboxLevelUsageViews();
}
