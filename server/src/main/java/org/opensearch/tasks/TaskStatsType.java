/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

public enum TaskStatsType {
    WORKER_STATS("worker_stats", false),
    // Used for indicating certain operator resource consumption for each worker
    OPERATOR_STATS("operator_stats", true);

    private final String statsType;
    private final boolean onlyForAnalysis;

    TaskStatsType(String statsType, boolean onlyForAnalysis) {
        this.statsType = statsType;
        this.onlyForAnalysis = onlyForAnalysis;
    }

    public boolean isOnlyForAnalysis() {
        return onlyForAnalysis;
    }

    @Override
    public String toString() {
        return statsType;
    }
}
