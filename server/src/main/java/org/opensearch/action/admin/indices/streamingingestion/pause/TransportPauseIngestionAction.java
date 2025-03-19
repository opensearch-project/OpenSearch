/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import java.util.Collections;

/**
 * Pause ingestion action.
 *
 * @opensearch.experimental
 */
public class TransportPauseIngestionAction extends TransportClusterManagerNodeAction<PauseIngestionRequest, PauseIngestionResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPauseIngestionAction.class);

    private final MetadataStreamingIngestionStateService ingestionStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportPauseIngestionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataStreamingIngestionStateService ingestionStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            PauseIngestionAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PauseIngestionRequest::new,
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
    protected PauseIngestionResponse read(StreamInput in) throws IOException {
        return new PauseIngestionResponse(in);
    }

    @Override
    protected void doExecute(Task task, PauseIngestionRequest request, ActionListener<PauseIngestionResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PauseIngestionRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final PauseIngestionRequest request,
        final ClusterState state,
        final ActionListener<PauseIngestionResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final PauseIngestionRequest request,
        final ClusterState state,
        final ActionListener<PauseIngestionResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new PauseIngestionResponse(true, Collections.emptyList()));
            return;
        }

        PauseIngestionClusterStateUpdateRequest pauseRequest = new PauseIngestionClusterStateUpdateRequest(task.getId()).ackTimeout(
            request.timeout()
        ).clusterManagerNodeTimeout(request.clusterManagerNodeTimeout()).indices(concreteIndices);

        ingestionStateService.pauseIngestion(pauseRequest, ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
            logger.debug(() -> new ParameterizedMessage("failed to pause ingestion on indices [{}]", (Object) concreteIndices), t);
            delegatedListener.onFailure(t);
        }));
    }
}
