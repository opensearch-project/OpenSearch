/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.wlm.WorkloadGroupTask;

import java.util.List;

/**
 * Placeholder {@link ResourceUsageCalculator} for {@link org.opensearch.wlm.ResourceType#NATIVE_MEMORY}.
 *
 * <p>Workload Management does not account native (off-heap) memory per workload group — that
 * live usage is reported by native backends (e.g. DataFusion) via the search backpressure
 * {@code NativeMemoryUsageTracker}. The enum entry exists so {@link
 * org.opensearch.search.backpressure.trackers.NodeDuressTrackers} can route native-memory
 * duress through the same {@code ResourceType} keyed map as CPU and heap; this calculator
 * simply returns zero for any task or task list so WLM surfaces that accidentally iterate it
 * behave as "no usage".
 */
public final class NativeMemoryUsageCalculator extends ResourceUsageCalculator {
    public static final NativeMemoryUsageCalculator INSTANCE = new NativeMemoryUsageCalculator();

    private NativeMemoryUsageCalculator() {}

    @Override
    public double calculateResourceUsage(List<WorkloadGroupTask> tasks) {
        return 0.0;
    }

    @Override
    public double calculateTaskResourceUsage(WorkloadGroupTask task) {
        return 0.0;
    }
}
