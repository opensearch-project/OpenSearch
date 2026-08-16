/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.wlm.tracker.CpuUsageCalculator;
import org.opensearch.wlm.tracker.MemoryUsageCalculator;
import org.opensearch.wlm.tracker.NativeMemoryUsageCalculator;
import org.opensearch.wlm.tracker.ResourceUsageCalculator;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Enum to hold the resource type
 *
 * @opensearch.api
 */
@PublicApi(since = "2.17.0")
public enum ResourceType {
    CPU("cpu", true, CpuUsageCalculator.INSTANCE, WorkloadManagementSettings::getNodeLevelCpuCancellationThreshold),
    MEMORY("memory", true, MemoryUsageCalculator.INSTANCE, WorkloadManagementSettings::getNodeLevelMemoryCancellationThreshold),
    // NATIVE_MEMORY is off-heap memory reported by native backends (e.g. DataFusion). It is
    // NOT tracked per workload group — {@code statsEnabled=false} keeps WorkloadGroupState
    // from allocating state slots for it, and its node-level WLM threshold is pinned at 1.0
    // so WLM cancellation never fires on this resource. The real consumer is search
    // backpressure, which pulls the duress signal via NodeDuressTrackers#isResourceInDuress.
    NATIVE_MEMORY("native_memory", false, NativeMemoryUsageCalculator.INSTANCE, settings -> 1.0);

    private final String name;
    private final boolean statsEnabled;
    private final ResourceUsageCalculator resourceUsageCalculator;
    private final Function<WorkloadManagementSettings, Double> nodeLevelThresholdSupplier;
    // sortedValues intentionally excludes NATIVE_MEMORY — WLM XContent output and related
    // consumers stayed at "CPU, MEMORY" before this enum grew, and we keep that stable.
    private static List<ResourceType> sortedValues = List.of(CPU, MEMORY);

    ResourceType(
        String name,
        boolean statsEnabled,
        ResourceUsageCalculator resourceUsageCalculator,
        Function<WorkloadManagementSettings, Double> nodeLevelThresholdSupplier
    ) {
        this.name = name;
        this.statsEnabled = statsEnabled;
        this.resourceUsageCalculator = resourceUsageCalculator;
        this.nodeLevelThresholdSupplier = nodeLevelThresholdSupplier;
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

    public ResourceUsageCalculator getResourceUsageCalculator() {
        return resourceUsageCalculator;
    }

    public double getNodeLevelThreshold(WorkloadManagementSettings settings) {
        return nodeLevelThresholdSupplier.apply(settings);
    }

    public static List<ResourceType> getSortedValues() {
        return sortedValues;
    }
}
