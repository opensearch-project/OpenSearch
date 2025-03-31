/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.resume;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.streamingingestion.IngestionStateShardFailure;
import org.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataStreamingIngestionStateService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

/**
 * Transport action to resume ingestion.
 *
 * @opensearch.experimental
 */
public class TransportResumeIngestionAction extends TransportClusterManagerNodeAction<ResumeIngestionRequest, ResumeIngestionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportResumeIngestionAction.class);

    private final MetadataStreamingIngestionStateService ingestionStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportResumeIngestionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataStreamingIngestionStateService ingestionStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            ResumeIngestionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ResumeIngestionRequest::new,
            indexNameExpressionResolver
        );
        this.ingestionStateService = ingestionStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ResumeIngestionResponse read(StreamInput in) throws IOException {
        return new ResumeIngestionResponse(in);
    }

    @Override
    protected void doExecute(Task task, ResumeIngestionRequest request, ActionListener<ResumeIngestionResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(ResumeIngestionRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final ResumeIngestionRequest request,
        final ClusterState state,
        final ActionListener<ResumeIngestionResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final ResumeIngestionRequest request,
        final ClusterState state,
        final ActionListener<ResumeIngestionResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new ResumeIngestionResponse(true, false, new IngestionStateShardFailure[0], ""));
            return;
        }

        String[] indices = Arrays.stream(concreteIndices).map(Index::getName).toArray(String[]::new);
        UpdateIngestionStateRequest updateIngestionStateRequest = new UpdateIngestionStateRequest(indices, new int[0]);
        updateIngestionStateRequest.timeout(request.clusterManagerNodeTimeout());
        updateIngestionStateRequest.setIngestionPaused(false);

        ingestionStateService.updateIngestionPollerState(
            "resume-ingestion",
            concreteIndices,
            updateIngestionStateRequest,
            new ActionListener<>() {

                @Override
                public void onResponse(UpdateIngestionStateResponse updateIngestionStateResponse) {
                    boolean shardsAcked = updateIngestionStateResponse.isAcknowledged()
                        && updateIngestionStateResponse.getFailedShards() == 0;
                    ResumeIngestionResponse response = new ResumeIngestionResponse(
                        true,
                        shardsAcked,
                        updateIngestionStateResponse.getShardFailureList(),
                        updateIngestionStateResponse.getErrorMessage()
                    );
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Error resuming ingestion", e);
                    listener.onFailure(e);
                }
            }
        );
    }
}
