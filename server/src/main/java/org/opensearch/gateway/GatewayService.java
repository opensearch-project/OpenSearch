/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.discovery.Discovery;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * The Gateway Service provider
 *
 * @opensearch.internal
 */
public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(GatewayService.class);

    public static final Setting<Integer> EXPECTED_NODES_SETTING = Setting.intSetting(
        "gateway.expected_nodes",
        -1,
        -1,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING = Setting.intSetting(
        "gateway.expected_data_nodes",
        -1,
        -1,
        Property.NodeScope
    );
    public static final Setting<Integer> EXPECTED_MASTER_NODES_SETTING = Setting.intSetting(
        "gateway.expected_master_nodes",
        -1,
        -1,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING = Setting.positiveTimeSetting(
        "gateway.recover_after_time",
        TimeValue.timeValueMillis(0),
        Property.NodeScope
    );
    public static final Setting<Integer> RECOVER_AFTER_NODES_SETTING = Setting.intSetting(
        "gateway.recover_after_nodes",
        -1,
        -1,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING = Setting.intSetting(
        "gateway.recover_after_data_nodes",
        -1,
        -1,
        Property.NodeScope
    );
    public static final Setting<Integer> RECOVER_AFTER_MASTER_NODES_SETTING = Setting.intSetting(
        "gateway.recover_after_master_nodes",
        0,
        0,
        Property.NodeScope,
        Property.Deprecated
    );

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(
        1,
        "state not recovered / initialized",
        true,
        true,
        false,
        RestStatus.SERVICE_UNAVAILABLE,
        ClusterBlockLevel.ALL
    );

    static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);

    private final ThreadPool threadPool;

    private final AllocationService allocationService;

    private final ClusterService clusterService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterNodes;
    private final int expectedNodes;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final int recoverAfterClusterManagerNodes;
    private final int expectedClusterManagerNodes;

    private final Runnable recoveryRunnable;

    private final AtomicBoolean recoveryInProgress = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(
        final Settings settings,
        final AllocationService allocationService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final TransportNodesListGatewayMetaState listGatewayMetaState,
        final Discovery discovery
    ) {
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        this.expectedNodes = EXPECTED_NODES_SETTING.get(settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(settings);
        this.expectedClusterManagerNodes = EXPECTED_MASTER_NODES_SETTING.get(settings);

        if (RECOVER_AFTER_TIME_SETTING.exists(settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(settings);
        } else if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedClusterManagerNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }
        this.recoverAfterNodes = RECOVER_AFTER_NODES_SETTING.get(settings);
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        // default the recover after cluster-manager nodes to the minimum cluster-manager nodes in the discovery
        if (RECOVER_AFTER_MASTER_NODES_SETTING.exists(settings)) {
            recoverAfterClusterManagerNodes = RECOVER_AFTER_MASTER_NODES_SETTING.get(settings);
        } else {
            recoverAfterClusterManagerNodes = -1;
        }

        if (discovery instanceof Coordinator) {
            recoveryRunnable = () -> clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask());
        } else {
            final Gateway gateway = new Gateway(settings, clusterService, listGatewayMetaState);
            recoveryRunnable = () -> gateway.performStateRecovery(new GatewayRecoveryListener());
        }
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        final ClusterState state = event.state();

        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            // not our job to recover
            return;
        }
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            return;
        }

        final DiscoveryNodes nodes = state.nodes();
        if (state.nodes().getClusterManagerNodeId() == null) {
            logger.debug("not recovering from gateway, no cluster-manager elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.getClusterManagerAndDataNodes().size()) < recoverAfterNodes) {
            logger.debug(
                "not recovering from gateway, nodes_size (data+master) [{}] < recover_after_nodes [{}]",
                nodes.getClusterManagerAndDataNodes().size(),
                recoverAfterNodes
            );
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            logger.debug(
                "not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(),
                recoverAfterDataNodes
            );
        } else if (recoverAfterClusterManagerNodes != -1 && nodes.getClusterManagerNodes().size() < recoverAfterClusterManagerNodes) {
            logger.debug(
                "not recovering from gateway, nodes_size (master) [{}] < recover_after_master_nodes [{}]",
                nodes.getClusterManagerNodes().size(),
                recoverAfterClusterManagerNodes
            );
        } else {
            boolean enforceRecoverAfterTime;
            String reason;
            if (expectedNodes == -1 && expectedClusterManagerNodes == -1 && expectedDataNodes == -1) {
                // no expected is set, honor the setting if they are there
                enforceRecoverAfterTime = true;
                reason = "recover_after_time was set to [" + recoverAfterTime + "]";
            } else {
                // one of the expected is set, see if all of them meet the need, and ignore the timeout in this case
                enforceRecoverAfterTime = false;
                reason = "";
                if (expectedNodes != -1 && (nodes.getClusterManagerAndDataNodes().size() < expectedNodes)) { // does not meet the
                                                                                                             // expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting ["
                        + expectedNodes
                        + "] nodes, but only have ["
                        + nodes.getClusterManagerAndDataNodes().size()
                        + "]";
                } else if (expectedDataNodes != -1 && (nodes.getDataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.getDataNodes().size() + "]";
                } else if (expectedClusterManagerNodes != -1 && (nodes.getClusterManagerNodes().size() < expectedClusterManagerNodes)) {
                    // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting ["
                        + expectedClusterManagerNodes
                        + "] cluster-manager nodes, but only have ["
                        + nodes.getClusterManagerNodes().size()
                        + "]";
                }
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(final boolean enforceRecoverAfterTime, final String reason) {
        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("delayed state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        if (recoveryInProgress.compareAndSet(false, true)) {
                            logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                            recoveryRunnable.run();
                        }
                    }
                }, recoverAfterTime, ThreadPool.Names.GENERIC);
            }
        } else {
            if (recoveryInProgress.compareAndSet(false, true)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(final Exception e) {
                        logger.warn("state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        logger.debug("performing state recovery...");
                        recoveryRunnable.run();
                    }
                });
            }
        }
    }

    private void resetRecoveredFlags() {
        recoveryInProgress.set(false);
        scheduledRecovery.set(false);
    }

    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(final ClusterState currentState) {
            if (currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                logger.debug("cluster is already recovered");
                return currentState;
            }

            final ClusterState newState = Function.<ClusterState>identity()
                .andThen(ClusterStateUpdaters::updateRoutingTable)
                .andThen(ClusterStateUpdaters::removeStateNotRecoveredBlock)
                .apply(currentState);

            return allocationService.reroute(newState, "state recovered");
        }

        @Override
        public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
            logger.info("recovered [{}] indices into cluster_state", newState.metadata().indices().size());
            // reset flag even though state recovery completed, to ensure that if we subsequently become leader again based on a
            // not-recovered state, that we again do another state recovery.
            resetRecoveredFlags();
        }

        @Override
        public void onNoLongerClusterManager(String source) {
            logger.debug("stepped down as cluster-manager before recovering state [{}]", source);
            resetRecoveredFlags();
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.info(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
            resetRecoveredFlags();
        }
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask() {
                @Override
                public ClusterState execute(final ClusterState currentState) {
                    final ClusterState updatedState = ClusterStateUpdaters.mixCurrentStateAndRecoveredState(currentState, recoveredState);
                    return super.execute(ClusterStateUpdaters.recoverClusterBlocks(updatedState));
                }
            });
        }

        @Override
        public void onFailure(final String msg) {
            logger.info("state recovery failed: {}", msg);
            resetRecoveredFlags();
        }

    }

    // used for testing
    TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

}
