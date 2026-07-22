/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.GatewayService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Service that monitors cluster rolling upgrade progress when safe rollback mode is enabled.
 * Once all nodes in the cluster have been upgraded to Version.CURRENT, safe rollback mode is
 * automatically completed.
 *
 * @opensearch.internal
 */
@InternalApi
public class SafeRollbackService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SafeRollbackService.class);

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;

    public SafeRollbackService(ClusterService clusterService, Client client, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return;
        }
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        boolean safeRollbackEnabled = Metadata.SETTING_SAFE_ROLLBACK_ENABLED_SETTING.get(state.metadata().settings());
        if (safeRollbackEnabled == false) {
            return;
        }

        Version minNodeVersion = state.nodes().getMinNodeVersion();
        if (minNodeVersion != null && minNodeVersion.before(Version.CURRENT) == false) {
            logger.info("All nodes in the cluster have upgraded to version {}. Safe rollback mode complete.", Version.CURRENT);
            autoDisableSafeRollback();
        }
    }

    private void autoDisableSafeRollback() {
        if (threadPool.getThreadContext().isSystemContext() == false) {
            threadPool.generic().execute(this::autoDisableSafeRollback);
            return;
        }
        Settings persistentSettings = Settings.builder().put(Metadata.SETTING_SAFE_ROLLBACK_ENABLED_SETTING.getKey(), false).build();
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(persistentSettings);
        client.admin().cluster().updateSettings(request, new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse response) {
                if (response.isAcknowledged()) {
                    logger.info("Successfully automatically disabled cluster.upgrade.safe_rollback_enabled.");
                } else {
                    logger.warn("Cluster update settings for disabling safe rollback was not acknowledged.");
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to automatically disable cluster.upgrade.safe_rollback_enabled", e);
            }
        });
    }
}
