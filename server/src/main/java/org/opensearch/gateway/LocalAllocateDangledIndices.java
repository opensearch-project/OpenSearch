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
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

/**
 * Allocates dangled indices
 *
 * @opensearch.internal
 */
public class LocalAllocateDangledIndices {

    private static final Logger logger = LogManager.getLogger(LocalAllocateDangledIndices.class);

    public static final String ACTION_NAME = "internal:gateway/local/allocate_dangled";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    @Inject
    public LocalAllocateDangledIndices(
        TransportService transportService,
        ClusterService clusterService,
        AllocationService allocationService,
        MetadataIndexUpgradeService metadataIndexUpgradeService
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        transportService.registerRequestHandler(
            ACTION_NAME,
            ThreadPool.Names.SAME,
            AllocateDangledRequest::new,
            new AllocateDangledRequestHandler()
        );
    }

    public void allocateDangled(Collection<IndexMetadata> indices, ActionListener<AllocateDangledResponse> listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode clusterManagerNode = clusterState.nodes().getClusterManagerNode();
        if (clusterManagerNode == null) {
            listener.onFailure(new ClusterManagerNotDiscoveredException("no cluster-manager to send allocate dangled request"));
            return;
        }
        AllocateDangledRequest request = new AllocateDangledRequest(clusterService.localNode(), indices.toArray(new IndexMetadata[0]));
        transportService.sendRequest(
            clusterManagerNode,
            ACTION_NAME,
            request,
            new ActionListenerResponseHandler<>(listener, AllocateDangledResponse::new, ThreadPool.Names.SAME)
        );
    }

    class AllocateDangledRequestHandler implements TransportRequestHandler<AllocateDangledRequest> {
        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel, Task task) throws Exception {
            String[] indexNames = new String[request.indices.length];
            for (int i = 0; i < request.indices.length; i++) {
                indexNames[i] = request.indices[i].getIndex().getName();
            }
            clusterService.submitStateUpdateTask("allocation dangled indices " + Arrays.toString(indexNames), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.blocks().disableStatePersistence()) {
                        return currentState;
                    }
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    final Version minIndexCompatibilityVersion = currentState.getNodes()
                        .getMaxNodeVersion()
                        .minimumIndexCompatibilityVersion();
                    boolean importNeeded = false;
                    StringBuilder sb = new StringBuilder();
                    for (IndexMetadata indexMetadata : request.indices) {
                        if (indexMetadata.getCreationVersion().before(minIndexCompatibilityVersion)) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}]"
                                    + " since it's created version [{}] is not supported by at least one node in the cluster minVersion [{}]",
                                indexMetadata.getIndex(),
                                request.fromNode,
                                indexMetadata.getCreationVersion(),
                                minIndexCompatibilityVersion
                            );
                            continue;
                        }
                        if (currentState.nodes().getMinNodeVersion().before(indexMetadata.getCreationVersion())) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}]"
                                    + " since its created version [{}] is later than the oldest versioned node in the cluster [{}]",
                                indexMetadata.getIndex(),
                                request.fromNode,
                                indexMetadata.getCreationVersion(),
                                currentState.getNodes().getClusterManagerNode().getVersion()
                            );
                            continue;
                        }
                        if (currentState.metadata().hasIndex(indexMetadata.getIndex().getName())) {
                            continue;
                        }
                        if (currentState.metadata().hasAlias(indexMetadata.getIndex().getName())) {
                            logger.warn(
                                "ignoring dangled index [{}] on node [{}] due to an existing alias with the same name",
                                indexMetadata.getIndex(),
                                request.fromNode
                            );
                            continue;
                        }
                        importNeeded = true;

                        IndexMetadata upgradedIndexMetadata;
                        try {
                            // The dangled index might be from an older version, we need to make sure it's compatible
                            // with the current version and upgrade it if needed.
                            upgradedIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(
                                indexMetadata,
                                minIndexCompatibilityVersion
                            );
                            upgradedIndexMetadata = IndexMetadata.builder(upgradedIndexMetadata)
                                .settings(
                                    Settings.builder()
                                        .put(upgradedIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())
                                )
                                .build();
                        } catch (Exception ex) {
                            // upgrade failed - adding index as closed
                            logger.warn(
                                () -> new ParameterizedMessage(
                                    "found dangled index [{}] on node [{}]. This index cannot be "
                                        + "upgraded to the latest version, adding as closed",
                                    indexMetadata.getIndex(),
                                    request.fromNode
                                ),
                                ex
                            );
                            upgradedIndexMetadata = IndexMetadata.builder(indexMetadata)
                                .state(IndexMetadata.State.CLOSE)
                                .version(indexMetadata.getVersion() + 1)
                                .build();
                        }
                        metadata.put(upgradedIndexMetadata, false);
                        blocks.addBlocks(upgradedIndexMetadata);
                        if (upgradedIndexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                            routingTableBuilder.addAsFromDangling(upgradedIndexMetadata);
                        }
                        sb.append("[")
                            .append(upgradedIndexMetadata.getIndex())
                            .append("/")
                            .append(upgradedIndexMetadata.getState())
                            .append("]");
                    }
                    if (!importNeeded) {
                        return currentState;
                    }
                    logger.info("auto importing dangled indices {} from [{}]", sb.toString(), request.fromNode);

                    RoutingTable routingTable = routingTableBuilder.build();
                    ClusterState updatedState = ClusterState.builder(currentState)
                        .metadata(metadata)
                        .blocks(blocks)
                        .routingTable(routingTable)
                        .build();

                    // now, reroute
                    return allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(routingTable).build(),
                        "dangling indices allocated"
                    );
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed send response for allocating dangled", inner);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        channel.sendResponse(new AllocateDangledResponse(true));
                    } catch (IOException e) {
                        logger.warn("failed send response for allocating dangled", e);
                    }
                }
            });
        }
    }

    /**
     * The request.
     *
     * @opensearch.internal
     */

    public static class AllocateDangledRequest extends TransportRequest {

        DiscoveryNode fromNode;
        IndexMetadata[] indices;

        public AllocateDangledRequest(StreamInput in) throws IOException {
            super(in);
            fromNode = new DiscoveryNode(in);
            indices = in.readArray(IndexMetadata::readFrom, IndexMetadata[]::new);
        }

        AllocateDangledRequest(DiscoveryNode fromNode, IndexMetadata[] indices) {
            this.fromNode = fromNode;
            this.indices = indices;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fromNode.writeTo(out);
            out.writeArray(indices);
        }
    }

    /**
     * The response.
     *
     * @opensearch.internal
     */
    public static class AllocateDangledResponse extends TransportResponse {

        private boolean ack;

        public AllocateDangledResponse(StreamInput in) throws IOException {
            super(in);
            ack = in.readBoolean();
        }

        AllocateDangledResponse(boolean ack) {
            this.ack = ack;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(ack);
        }
    }
}
