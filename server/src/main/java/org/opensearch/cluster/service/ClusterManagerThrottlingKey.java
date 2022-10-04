/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

/**
 * Class to store the throttling key for the tasks of cluster manager
 */
public class ClusterManagerThrottlingKey {
    private String taskThrottlingKey;
    private boolean throttlingEnabled;

    /**
     * Class for throttling key of tasks
     *
     * @param taskThrottlingKey - throttling key for task
     * @param throttlingEnabled - if throttling is enabled or not i.e. data node is performing retry over throttling exception or not.
     */
    public ClusterManagerThrottlingKey(String taskThrottlingKey, boolean throttlingEnabled) {
        this.taskThrottlingKey = taskThrottlingKey;
        this.throttlingEnabled = throttlingEnabled;
    }

    public String getTaskThrottlingKey() {
        return taskThrottlingKey;
    }

    public boolean isThrottlingEnabled() {
        return throttlingEnabled;
    }
}
