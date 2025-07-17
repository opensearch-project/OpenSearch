/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

/**
 * Thread Utilities.
 *
 * @opensearch.internal
 */
public class ThreadUtils {

    private ThreadUtils() {}

    public static final String CLUSTER_UPDATE_THREAD_NAME = "clusterApplierService#updateTask";
    public static final String CLUSTER_MANAGER_UPDATE_THREAD_NAME = "clusterManagerService#updateTask";
    public static final String HTTP_SERVER_WORKER_THREAD_NAME_PREFIX = "http_server_worker";
    public static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = "transport_worker";

    public static boolean assertNotScheduleThread(String reason) {
        assert Thread.currentThread().getName().contains("scheduler") == false : "Expected current thread ["
            + Thread.currentThread()
            + "] to not be the scheduler thread. Reason: ["
            + reason
            + "]";
        return true;
    }

    /** asserts that the current thread is <b>NOT</b> the cluster state update thread */
    public static boolean assertNotClusterStateUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME) == false : "Expected current thread ["
            + Thread.currentThread()
            + "] to not be the cluster state update thread. Reason: ["
            + reason
            + "]";
        return true;
    }

    public static boolean assertNotClusterManagerUpdateThread(String reason) {
        assert isClusterManagerUpdateThread() == false : "Expected current thread ["
            + Thread.currentThread()
            + "] to not be the cluster-manager service thread. Reason: ["
            + reason
            + "]";
        return true;
    }

    public static boolean isClusterManagerUpdateThread() {
        return Thread.currentThread().getName().contains(CLUSTER_MANAGER_UPDATE_THREAD_NAME);
    }
}
