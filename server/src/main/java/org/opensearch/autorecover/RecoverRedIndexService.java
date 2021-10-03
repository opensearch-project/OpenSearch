/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autorecover;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.master.TransportMasterNodeAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RecoverRedIndexService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(RecoverRedIndexService.class);

    public static final String UPDATE_INDEX_STATUS_ACTION_NAME = "internal:cluster/recoverredindex/update_index_status";

    public final UpdateIndexStatusAction updateIndexStatusHandler;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final Client client;

    public RecoverRedIndexService( TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, Client client) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = transportService.getThreadPool();
        this.client = client;

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updateIndexStatusHandler =
            new UpdateIndexStatusAction(transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);

    }


    @Override
    protected void doStart() {
        assert this.updateIndexStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_INDEX_STATUS_ACTION_NAME) != null;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
    }

    public void updateIndicesStatus(List<UpdateIndexCleanStatusRequest> requestList,
                                    ActionListener<UpdateIndexCleanStatusResponse> listener) {

        clusterService.submitStateUpdateTask("update_indices_clean_to_restore_status", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                for (UpdateIndexCleanStatusRequest request : requestList) {
                    IndexMetadata indexMetadata = currentState.metadata().index(request.getIndexName());
                    Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());

                    indexSettings.put(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(),
                        request.isIndexCleanToRestoreFromSnapshot())
                        .put(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.getKey(),
                            request.getIndexCleanToRestoreFromSnapshotUpdateTime());

                    final IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                    builder.settings(indexSettings);
                    builder.settingsVersion(1 + builder.settingsVersion());
                    metadataBuilder.put(builder);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder).build();
                return updatedState;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(UpdateIndexCleanStatusResponse.INSTANCE);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to update cluster state [{}]", source);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Updates following settings from index list after snapshot process
     * 1. INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT
     * 2. INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME
     */
    public void updateSnapshotIndicesStatus(Map<Index, Boolean> indicesSucessfullySnapshottedStatusMap, long snapshotStartTime,
                                            ActionListener<Void> listener) {
        List<UpdateIndexCleanStatusRequest> requestList = new ArrayList<>();
        for (Map.Entry<Index,Boolean> indexEntry : indicesSucessfullySnapshottedStatusMap.entrySet()) {

            // Update Setting only for indices for which snapshot was successful
            if (indexEntry.getValue()) {
                Settings indexSettings = clusterService.state().metadata().index(indexEntry.getKey().getName()).getSettings();

                if (IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.exists(indexSettings)){
                    Boolean indexCleanToRestoreFromSnapshot  = IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.get(indexSettings);
                    long indexCleanToRestoreFromSnapshotTime  = IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.get(
                        indexSettings);

                    if (!indexCleanToRestoreFromSnapshot) {
                        if(indexCleanToRestoreFromSnapshotTime < snapshotStartTime) {

                            // Index write happened before snapshot was started for the index, hence updating setting
                            UpdateIndexCleanStatusRequest updateIndexCleanStatusRequest = new UpdateIndexCleanStatusRequest(
                                indexEntry.getValue(), threadPool.absoluteTimeInMillis(), indexEntry.getKey().getName());
                            requestList.add(updateIndexCleanStatusRequest);
                        }
                    }
                }
            } else {
                UpdateIndexCleanStatusRequest updateIndexCleanStatusRequest = new UpdateIndexCleanStatusRequest(indexEntry.getValue(),
                    threadPool.absoluteTimeInMillis(), indexEntry.getKey().getName());
                requestList.add(updateIndexCleanStatusRequest);
            }
        }
        updateIndicesStatus(requestList,
            new ActionListener<UpdateIndexCleanStatusResponse>() {
                @Override
                public void onResponse(UpdateIndexCleanStatusResponse updateIndexCleanStatusResponse) {
                    logger.info("snapshot updated index setting [{}] for indices with status [{}]",
                        IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(), requestList);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to update index setting [{}] for indices with status [{}]",
                        IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(), requestList), e);
                    listener.onFailure(e);
                }
            });
    }

    public class UpdateIndexStatusAction
        extends TransportMasterNodeAction<UpdateIndexCleanStatusRequest, UpdateIndexCleanStatusResponse> {
        UpdateIndexStatusAction(TransportService transportService, ClusterService clusterService,
                                ThreadPool threadPool, ActionFilters actionFilters,
                                IndexNameExpressionResolver indexNameExpressionResolver) {
            super(UPDATE_INDEX_STATUS_ACTION_NAME, false, transportService, clusterService, threadPool,
                actionFilters, UpdateIndexCleanStatusRequest::new, indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexCleanStatusResponse read(StreamInput in) throws IOException {
            return UpdateIndexCleanStatusResponse.INSTANCE;
        }

        @Override
        protected void masterOperation(UpdateIndexCleanStatusRequest request, ClusterState state,
                                       ActionListener<UpdateIndexCleanStatusResponse> listener) throws Exception {
            List<UpdateIndexCleanStatusRequest> requestList = new ArrayList<UpdateIndexCleanStatusRequest>();
            requestList.add(request);
            updateIndicesStatus(requestList, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexCleanStatusRequest request, ClusterState state) {
            return null;
        }
    }
}
