/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.wlm.WorkloadManagementSettings;

/**
 * Utility class to provide utility methods at query group level
 */
public abstract class ResourceUsageUtil {
    private WorkloadManagementSettings settings;

    public WorkloadManagementSettings getSettings() {
        return settings;
    }

    /**
     * WorkloadManagementSettings setter
     * @param settings
     */
    public void setSettings(WorkloadManagementSettings settings) {
        this.settings = settings;
    }

    /**
     * Determines whether {@link QueryGroup} is breaching its threshold for the resource
     * @param queryGroup
     * @param currentUsage
     * @return whether the query group is breaching threshold for this resource
     */
    public boolean isBreachingThresholdFor(QueryGroup queryGroup, double currentUsage) {
        return getExcessUsage(queryGroup, currentUsage) > 0;
    }

    /**
     * returns the value by which the resource usage is beyond the configured limit for the query group
     * @param queryGroup instance
     * @return the overshooting limit for the resource
     */
    public double getExcessUsage(QueryGroup queryGroup, double currentUsage) {
        return currentUsage - getNormalisedThreshold(queryGroup);
    }

    /**
     * normalises configured value with respect to node level cancellation thresholds
     * @param queryGroup instance
     * @return normalised value with respect to node level cancellation thresholds
     */
    protected abstract double getNormalisedThreshold(QueryGroup queryGroup);
}
