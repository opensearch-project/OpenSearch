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
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.List;

/**
 * Service responsible for submitting metadata updates, mainly ingestion pause/resume state change updates.
 *
 * @opensearch.experimental
 */
public class MetadataStreamingIngestionStateService {
    private static final Logger logger = LogManager.getLogger(MetadataStreamingIngestionStateService.class);

    private final ClusterService clusterService;

    @Inject
    public MetadataStreamingIngestionStateService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Publishes cluster state change request to pause ingestion.
     * TODO: support waiting for node state updates.
     */
    public void pauseIngestion(
        final PauseIngestionClusterStateUpdateRequest request,
        final ActionListener<PauseIngestionResponse> listener
    ) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask("pause-ingestion", new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updateIngestionPausedState(concreteIndices, currentState, true);
            }

            @Override
            protected PauseIngestionResponse newResponse(boolean acknowledged) {
                List<PauseIngestionResponse.IndexResult> results = new ArrayList<>();
                for (Index index : concreteIndices) {
                    results.add(new PauseIngestionResponse.IndexResult(index.getName(), ""));
                }

                return new PauseIngestionResponse(acknowledged, results);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(new OpenSearchException("pause ingestion failed", e));
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        });
    }

    /**
     * Publishes cluster state change request to resume ingestion.
     */
    public void resumeIngestion(
        final ResumeIngestionClusterStateUpdateRequest request,
        final ActionListener<ResumeIngestionResponse> listener
    ) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask("resume-ingestion", new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updateIngestionPausedState(concreteIndices, currentState, false);
            }

            @Override
            protected ResumeIngestionResponse newResponse(boolean acknowledged) {
                List<ResumeIngestionResponse.IndexResult> results = new ArrayList<>();
                for (Index index : concreteIndices) {
                    results.add(new ResumeIngestionResponse.IndexResult(index.getName(), ""));
                }

                return new ResumeIngestionResponse(acknowledged, results);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(new OpenSearchException("resume ingestion failed", e));
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }
        });
    }

    static ClusterState updateIngestionPausedState(final Index[] indices, final ClusterState currentState, boolean ingestionPaused) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());

        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);

            if (indexMetadata.useIngestionSource() == false) {
                logger.debug("Pause/resume request will be ignored for index {} as streaming ingestion is not enabled", index);
            }

            if (indexMetadata.isIngestionPaused() != ingestionPaused) {
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).ingestionPaused(ingestionPaused);
                metadata.put(updatedMetadata);
            } else {
                logger.debug(
                    "Received request for ingestionPaused:{} for index {}. The state is already ingestionPaused:{}",
                    ingestionPaused,
                    index,
                    ingestionPaused
                );
            }
        }

        return ClusterState.builder(currentState).metadata(metadata).build();
    }
}
