/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

public class TaskStatsInfo {
    private long startValue;
    private long endValue;
    private int count;

    public TaskStatsInfo(long startValue) {
        this.startValue = startValue;
    }

    public long getTotalValue() {
        if (endValue != 0 && endValue > startValue) {
            return endValue - startValue;
        }
        return 0L;
    }

    public int getCount() {
        return count;
    }

    public void setEndValue(long value) {
        endValue = value;
        count++;
    }
}
