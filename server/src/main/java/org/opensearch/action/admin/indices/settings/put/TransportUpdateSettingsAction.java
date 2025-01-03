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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.settings.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.indices.scaleToZero.PreScaleSyncAction;
import org.opensearch.action.admin.indices.scaleToZero.PreScaleSyncRequest;
import org.opensearch.action.admin.indices.scaleToZero.PreScaleSyncResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataUpdateSettingsService;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Transport action for updating index settings
 *
 * @opensearch.internal
 */
public class TransportUpdateSettingsAction extends TransportClusterManagerNodeAction<UpdateSettingsRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateSettingsAction.class);

    private final static Set<String> ALLOWLIST_REMOTE_SNAPSHOT_SETTINGS = Set.of(
        "index.max_result_window",
        "index.max_inner_result_window",
        "index.max_rescore_window",
        "index.max_docvalue_fields_search",
        "index.max_script_fields",
        "index.max_terms_count",
        "index.max_regex_length",
        "index.highlight.max_analyzed_offset",
        "index.number_of_replicas"
    );

    private final static String[] ALLOWLIST_REMOTE_SNAPSHOT_SETTINGS_PREFIXES = { "index.search.slowlog", "index.routing.allocation" };

    private final MetadataUpdateSettingsService updateSettingsService;

    private final AllocationService allocationService;

    private final Client client;

    @Inject
    public TransportUpdateSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataUpdateSettingsService updateSettingsService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        Client client
    ) {
        super(
            UpdateSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateSettingsRequest::new,
            indexNameExpressionResolver
        );
        this.updateSettingsService = updateSettingsService;
        this.allocationService = allocationService;
        this.client = client;
    }

    @Override
    protected String executor() {
        // we go async right away....
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        if (request.settings().size() == 1 &&  // we have to allow resetting these settings otherwise users can't unblock an index
            ClusterBlocks.INDEX_DATA_READ_ONLY_BLOCK_SETTINGS.stream()
                .anyMatch(booleanSetting -> booleanSetting.exists(request.settings()))) {
            return null;
        }

        final Index[] requestIndices = indexNameExpressionResolver.concreteIndices(state, request);
        boolean allowSearchableSnapshotSettingsUpdate = true;
        // check if all indices in the request are remote snapshot
        for (Index index : requestIndices) {
            if (state.blocks().indexBlocked(ClusterBlockLevel.METADATA_WRITE, index.getName())) {
                allowSearchableSnapshotSettingsUpdate = allowSearchableSnapshotSettingsUpdate
                    && state.getMetadata().getIndexSafe(index).isRemoteSnapshot();
            }
        }
        // check if all settings in the request are in the allow list
        if (allowSearchableSnapshotSettingsUpdate) {
            for (String setting : request.settings().keySet()) {
                allowSearchableSnapshotSettingsUpdate = allowSearchableSnapshotSettingsUpdate
                    && (ALLOWLIST_REMOTE_SNAPSHOT_SETTINGS.contains(setting)
                        || Stream.of(ALLOWLIST_REMOTE_SNAPSHOT_SETTINGS_PREFIXES).anyMatch(setting::startsWith));
            }
        }

        final String[] requestIndexNames = Arrays.stream(requestIndices).map(Index::getName).toArray(String[]::new);
        return allowSearchableSnapshotSettingsUpdate
            ? null
            : state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, requestIndexNames);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final UpdateSettingsRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        UpdateSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpdateSettingsClusterStateUpdateRequest().indices(
            concreteIndices
        )
            .settings(request.settings())
            .setPreserveExisting(request.isPreserveExisting())
            .ackTimeout(request.timeout())
            .masterNodeTimeout(request.clusterManagerNodeTimeout());

        updateSettingsService.updateSettings(clusterStateUpdateRequest, new ActionListener<>() {

            /**
             * Handles the response from the initial settings update during scale-down operations.
             * This is a critical part of the two-phase scale-down process:
             * <p>
             * 1. First Phase (Settings Update):
             * - Updates the remove_indexing_shards setting across all nodes
             * - Waits for acknowledgment from the cluster
             * - Allows nodes to prepare for scale-down operations
             * <p>
             * 2. Second Phase (Routing Update):
             * - Only triggered after successful settings propagation
             * - Updates routing tables to remove primary and replica shards
             * - Ensures data consistency before shard removal
             * <p>
             * This two-phase approach prevents data loss by ensuring all nodes are prepared
             * before any shards are removed from the routing table. It's essential for maintaining
             * cluster stability during scale-down operations.
             *
             * @param response The cluster state update response from the settings update
             */

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                if (response.isAcknowledged() && request.settings().hasValue(IndexMetadata.SETTING_REMOVE_INDEXING_SHARDS)) {
                    boolean removeIndexingShards = request.settings().getAsBoolean(IndexMetadata.SETTING_REMOVE_INDEXING_SHARDS, false);
                    UpdateSettingsClusterStateUpdateRequest updateRequest = new UpdateSettingsClusterStateUpdateRequest()
                        .indices(concreteIndices)
                        .ackTimeout(request.timeout())
                        .masterNodeTimeout(request.clusterManagerNodeTimeout());

                    clusterService.submitStateUpdateTask(
                        "update-routing-table-after-settings",
                        new AckedClusterStateUpdateTask<>(Priority.URGENT, updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
                            @Override
                            public void onResponse(ClusterStateUpdateResponse response) {
                                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        }) {
                            @Override
                            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                                return new ClusterStateUpdateResponse(acknowledged);
                            }

                            @Override
                            public ClusterState execute(ClusterState currentState) {
                                String[] indexNames = Arrays.stream(concreteIndices)
                                    .map(Index::getName)
                                    .toArray(String[]::new);

                                if (removeIndexingShards) {
                                    return updateRoutingTableForRemoveIndexShards(indexNames, currentState);
                                } else {
                                    return updateRoutingTableForRestoreIndexShards(indexNames, currentState);
                                }
                            }
                        }
                    );
                } else {
                    listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
                }
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to update settings on indices [{}]", (Object) concreteIndices), t);
                listener.onFailure(t);
            }
        });
    }


    /**
     * Updates routing table and metadata for scaling down indices to zero indexing shards.
     * First performs sync operation and then updates routing tables.
     */
    public ClusterState updateRoutingTableForRemoveIndexShards(String[] actualIndices, ClusterState currentState) {
        // Perform sync for all indices first
        List<Exception> exceptions = new CopyOnWriteArrayList<>();


        exceptions.add(new RuntimeException("Test failure"));

        for (String indexName : actualIndices) {
            PreScaleSyncRequest request = new PreScaleSyncRequest(indexName);
            client.execute(PreScaleSyncAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(PreScaleSyncResponse response) {
                    if (response.hasFailures()) {
                        exceptions.add(
                            new IllegalStateException(
                                "Pre-scale sync failed for index " + indexName + ": " + response.getFailureReason()
                            )
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    exceptions.add(e);
                }
            });
        }
        if (!exceptions.isEmpty()) {
            logger.info("Failed to scale down the primaries, try again {} ", exceptions);
            throw new IllegalArgumentException("Failed to scale down the primaries, try again");
        }

        try {

            // All syncs successful, proceed with routing table update
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

            for (String indexName : actualIndices) {
                Index index = currentState.metadata().index(indexName).getIndex();
                IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                    .removeIndexingShards(true);
                metadataBuilder.put(indexMetadataBuilder);
                routingTableBuilder.updateRemoveIndexShards(true, new String[] { indexName });
            }

            return ClusterState.builder(currentState)
                .metadata(metadataBuilder)
                .routingTable(routingTableBuilder.build())
                .build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ClusterState updateRoutingTableForRestoreIndexShards(String[] actualIndices, ClusterState currentState) {

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

        // Update metadata and routing for each index
        for (String indexName : actualIndices) {
            Index index = currentState.metadata().index(indexName).getIndex();
            IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                .removeIndexingShards(false);
            metadataBuilder.put(indexMetadataBuilder);
            routingTableBuilder.updateRemoveIndexShards(false, actualIndices);
        }
        // Create intermediate state with updated metadata and routing
        ClusterState tempState = ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        // Force a reroute to properly allocate the restored shards
        return ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .routingTable(allocationService.reroute(tempState, "restore indexing shards").routingTable())
            .build();
    }
}

