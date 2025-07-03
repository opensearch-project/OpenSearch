/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;

/**
 * A local implementation of {@link ClusterService} that assumes we have no cluster manager.
 * This is used in clusterless mode.
 */
public class LocalClusterService extends ClusterService {
    private static class DummyClusterManagerService extends ClusterManagerService {
        private static final ClusterManagerThrottlingStats EMPTY_THROTTLING_STATS = new ClusterManagerThrottlingStats();

        public DummyClusterManagerService(Settings settings, ClusterSettings clusterSettings) {
            super(settings, clusterSettings, null, null);
        }

        @Override
        public synchronized void setClusterStatePublisher(ClusterStatePublisher publisher) {}

        @Override
        public ClusterManagerThrottlingStats getThrottlingStats() {
            return EMPTY_THROTTLING_STATS;
        }
    }

    public LocalClusterService(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        super(
            settings,
            clusterSettings,
            new DummyClusterManagerService(settings, clusterSettings),
            new ClusterApplierService(Node.NODE_NAME_SETTING.get(settings), settings, clusterSettings, threadPool, clusterManagerMetrics)
        );
    }

    @Override
    protected synchronized void doStart() {
        getClusterApplierService().start();
    }

    @Override
    protected synchronized void doStop() {
        getClusterApplierService().stop();
    }

    @Override
    protected synchronized void doClose() {
        getClusterApplierService().close();
    }

    @Override
    public ClusterManagerTaskThrottler.ThrottlingKey registerClusterManagerTask(ClusterManagerTask task, boolean throttlingEnabled) {
        return null;
    }

    @Override
    public <T> void submitStateUpdateTasks(
        final String source,
        final Map<T, ClusterStateTaskListener> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    ) {
        throw new UnsupportedOperationException("Cannot submit cluster state update tasks when cluster manager service is not available");
    }
}
