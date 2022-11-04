/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

/**
 * Defines the type of TaskResourceUsageTracker.
 */
public enum TaskResourceUsageTrackerType {
    CPU_USAGE_TRACKER("cpu_usage_tracker"),
    HEAP_USAGE_TRACKER("heap_usage_tracker"),
    ELAPSED_TIME_TRACKER("elapsed_time_tracker");

    private final String name;

    TaskResourceUsageTrackerType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static TaskResourceUsageTrackerType fromName(String name) {
        switch (name) {
            case "cpu_usage_tracker":
                return CPU_USAGE_TRACKER;
            case "heap_usage_tracker":
                return HEAP_USAGE_TRACKER;
            case "elapsed_time_tracker":
                return ELAPSED_TIME_TRACKER;
        }

        throw new IllegalArgumentException("Invalid TaskResourceUsageTrackerType: " + name);
    }
}
