/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

public class TaskStatsInfo {
    private final TaskStats stats;
    private final boolean absolute;
    private long startValue;
    private long endValue;

    public TaskStatsInfo(TaskStats stats, long value, boolean absolute) {
        this.stats = stats;
        this.absolute = absolute;
        if (absolute) {
            this.endValue = value;
        } else {
            this.startValue = value;
        }
    }

    public long getTotalValue() {
        if (endValue != 0 && endValue > startValue) {
            return endValue - startValue;
        }
        return 0L;
    }

    public void setEndValue(long value) {
        endValue = value;
    }

    public TaskStats getStats() {
        return stats;
    }

    public boolean isAbsolute() {
        return absolute;
    }

    @Override
    public String toString() {
        return String.valueOf(getTotalValue());
    }
}
