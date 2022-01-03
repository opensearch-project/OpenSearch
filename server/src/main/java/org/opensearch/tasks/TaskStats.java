/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

public enum TaskStats {
    MEMORY("memory_in_bytes"),
    CPU("cpu_time_in_nanos");

    private final String statsName;

    TaskStats(String statsName) {
        this.statsName = statsName;
    }

    @Override
    public String toString() {
        return statsName;
    }
}
