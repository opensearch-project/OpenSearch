/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

public class ResourceUsageStatsReference {
    private String resourceUsageStats;

    public ResourceUsageStatsReference(String stats) {
        this.resourceUsageStats = stats;
    }

    public String getResourceUsageStats() {
        return resourceUsageStats;
    }

    public void setResourceUsageStats(String stats) {
        this.resourceUsageStats = new String(stats);
    }

    @Override
    public String toString() {
        return this.resourceUsageStats;
    }

}
