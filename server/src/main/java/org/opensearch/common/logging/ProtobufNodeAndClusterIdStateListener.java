/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ProtobufClusterState;
import org.opensearch.cluster.ProtobufClusterStateObserver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;

/**
 * The {@link ProtobufNodeAndClusterIdStateListener} listens to cluster state changes and ONLY when receives the first update
 * it sets the clusterUUID and nodeID in log4j pattern converter {@link NodeAndClusterIdConverter}.
 * Once the first update is received, it will automatically be de-registered from subsequent updates.
 *
 * @opensearch.internal
 */
public class ProtobufNodeAndClusterIdStateListener implements ProtobufClusterStateObserver.Listener {
    private static final Logger logger = LogManager.getLogger(ProtobufNodeAndClusterIdStateListener.class);

    private ProtobufNodeAndClusterIdStateListener() {}

    /**
     * Subscribes for the first cluster state update where nodeId and clusterId is present
     * and sets these values in {@link NodeAndClusterIdConverter}.
     */
    public static void getAndSetNodeIdAndClusterId(ClusterService clusterService, ThreadContext threadContext) {
        ProtobufClusterState clusterState = clusterService.protobufState();
        ProtobufClusterStateObserver observer = new ProtobufClusterStateObserver(clusterState, clusterService, null, logger, threadContext);

        observer.waitForNextChange(
            new ProtobufNodeAndClusterIdStateListener(),
            ProtobufNodeAndClusterIdStateListener::isNodeAndClusterIdPresent
        );
    }

    private static boolean isNodeAndClusterIdPresent(ProtobufClusterState clusterState) {
        return getNodeId(clusterState) != null && getClusterUUID(clusterState) != null;
    }

    private static String getClusterUUID(ProtobufClusterState state) {
        return state.getMetadata().clusterUUID();
    }

    private static String getNodeId(ProtobufClusterState state) {
        return state.getNodes().getLocalNodeId();
    }

    @Override
    public void onNewClusterState(ProtobufClusterState state) {
        String nodeId = getNodeId(state);
        String clusterUUID = getClusterUUID(state);

        logger.debug("Received cluster state update. Setting nodeId=[{}] and clusterUuid=[{}]", nodeId, clusterUUID);
        NodeAndClusterIdConverter.setNodeIdAndClusterId(nodeId, clusterUUID);
    }

    @Override
    public void onClusterServiceClose() {}

    @Override
    public void onTimeout(TimeValue timeout) {}
}
