/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.opensearch.cluster.service.ClusterManagerTask.PUT_INDEX_FIELD_DOMAINS;

/**
 * Service responsible for publishing index-level field-domain metadata.
 *
 * Field-domain metadata describes values that may exist for a field in one concrete index. Trusted producers use this
 * service to merge validated metadata into {@link IndexMetadata#getCustomData(String)} on the cluster-manager node.
 */
public class MetadataIndexFieldDomainService {
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ClusterManagerTaskThrottler.ThrottlingKey putIndexFieldDomainsTaskKey;

    @Inject
    public MetadataIndexFieldDomainService(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        putIndexFieldDomainsTaskKey = clusterService.registerClusterManagerTask(PUT_INDEX_FIELD_DOMAINS, true);
    }

    /**
     * Publishes field-domain metadata through a cluster state update.
     */
    public void putFieldDomains(
        final PutIndexFieldDomainsClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "put-index-field-domains [" + request.targetIndex() + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, wrapPreservingContext(listener, threadPool.getThreadContext())) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return putIndexFieldDomainsTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyFieldDomains(currentState, request);
                }
            }
        );
    }

    /**
     * Applies the metadata merge to the supplied cluster state.
     */
    static ClusterState applyFieldDomains(ClusterState currentState, PutIndexFieldDomainsClusterStateUpdateRequest request) {
        Objects.requireNonNull(currentState, "currentState must not be null");
        Objects.requireNonNull(request, "request must not be null");

        IndexMetadata currentIndexMetadata = currentState.metadata()
            .getIndexSafe(Objects.requireNonNull(request.targetIndex(), "targetIndex must not be null"));

        Map<String, String> encodedFieldDomains = request.fieldDomainCustomData();
        if (encodedFieldDomains == null || encodedFieldDomains.isEmpty()) {
            throw new IllegalArgumentException("field domain metadata is required");
        }

        IndexMetadata updatedIndexMetadata = IndexFieldDomainMetadata.getInstance()
            .putFieldDomains(currentIndexMetadata, encodedFieldDomains);
        if (Objects.equals(
            currentIndexMetadata.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY),
            updatedIndexMetadata.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY)
        )) {
            return currentState;
        }

        Metadata.Builder metadata = Metadata.builder(currentState.metadata()).put(updatedIndexMetadata, true);
        return ClusterState.builder(currentState).metadata(metadata).build();
    }
}
