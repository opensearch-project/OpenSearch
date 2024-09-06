/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.wlm.tracker.CpuUsageCalculator;
import org.opensearch.wlm.tracker.MemoryUsageCalculator;
import org.opensearch.wlm.tracker.QueryGroupUsage;
import org.opensearch.wlm.tracker.ResourceUsageCalculator;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * Enum to hold the resource type
 *
 * @opensearch.api
 */
@PublicApi(since = "2.17.0")
public enum ResourceType {
    CPU("cpu", true, CpuUsageCalculator.INSTANCE, new QueryGroupUsage() {
        @Override
        protected double getNormalisedThreshold(QueryGroup queryGroup) {
            return queryGroup.getResourceLimits().get(ResourceType.CPU) * getSettings().getNodeLevelCpuCancellationThreshold();
        }
    }),
    MEMORY("memory", true, MemoryUsageCalculator.INSTANCE, new QueryGroupUsage() {
        @Override
        protected double getNormalisedThreshold(QueryGroup queryGroup) {
            return queryGroup.getResourceLimits().get(ResourceType.MEMORY) * getSettings().getNodeLevelMemoryCancellationThreshold();
        }
    });

    private final String name;
    private final boolean statsEnabled;
    private final ResourceUsageCalculator resourceUsageCalculator;
    private final QueryGroupUsage queryGroupUsage;
    private static List<ResourceType> sortedValues = List.of(CPU, MEMORY);

    ResourceType(String name, boolean statsEnabled, ResourceUsageCalculator resourceUsageCalculator, QueryGroupUsage queryGroupUsage) {
        this.name = name;
        this.statsEnabled = statsEnabled;
        this.resourceUsageCalculator = resourceUsageCalculator;
        this.queryGroupUsage = queryGroupUsage;
    }

    /**
     * The string match here is case-sensitive
     * @param s name matching the resource type name
     * @return a {@link ResourceType}
     */
    public static ResourceType fromName(String s) {
        for (ResourceType resourceType : values()) {
            if (resourceType.getName().equals(s)) {
                return resourceType;
            }
        }
        throw new IllegalArgumentException("Unknown resource type: [" + s + "]");
    }

    public static void writeTo(StreamOutput out, ResourceType resourceType) throws IOException {
        out.writeString(resourceType.getName());
    }

    public String getName() {
        return name;
    }

    public boolean hasStatsEnabled() {
        return statsEnabled;
    }

    public double calculateQueryGroupUsage(List<QueryGroupTask> tasks, Supplier<Long> nanoTimeSupplier) {
        return resourceUsageCalculator.calculateResourceUsage(tasks, nanoTimeSupplier);
    }

    public double calculateTaskUsage(QueryGroupTask task, Supplier<Long> nanoTimeSupplier) {
        return resourceUsageCalculator.calculateTaskResourceUsage(task, nanoTimeSupplier);
    }

    public boolean isBreachingThreshold(QueryGroup queryGroup, double currentUsage) {
        return getExcessUsage(queryGroup, currentUsage) > 0;
    }

    public double getExcessUsage(QueryGroup queryGroup, double currentUsage) {
        return queryGroupUsage.getExcessUsage(queryGroup, currentUsage);
    }

    public void setWorkloadManagementSettings(WorkloadManagementSettings settings) {
        queryGroupUsage.setSettings(settings);
    }

    public static List<ResourceType> getSortedValues() {
        return sortedValues;
    }
}
