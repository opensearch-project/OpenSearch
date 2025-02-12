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

package org.opensearch.action.admin.indices.dangling.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.opensearch.action.admin.indices.dangling.list.NodeListDanglingIndicesResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the deletion of a dangling index. When handling a {@link DeleteDanglingIndexAction},
 * this class first checks that such a dangling index exists. It then submits a cluster state update
 * to add the index to the index graveyard.
 *
 * @opensearch.internal
 */
public class TransportDeleteDanglingIndexAction extends TransportClusterManagerNodeAction<
    DeleteDanglingIndexRequest,
    AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportDeleteDanglingIndexAction.class);

    private final Settings settings;
    private final NodeClient nodeClient;
    private final ClusterManagerTaskThrottler.ThrottlingKey deleteDanglingIndexTaskKey;

    @Inject
    public TransportDeleteDanglingIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        NodeClient nodeClient
    ) {
        super(
            DeleteDanglingIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDanglingIndexRequest::new,
            indexNameExpressionResolver
        );
        this.settings = settings;
        this.nodeClient = nodeClient;
        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        deleteDanglingIndexTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.DELETE_DANGLING_INDEX_KEY, true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        DeleteDanglingIndexRequest deleteRequest,
        ClusterState state,
        ActionListener<AcknowledgedResponse> deleteListener
    ) throws Exception {
        findDanglingIndex(deleteRequest.getIndexUUID(), new ActionListener<Index>() {
            @Override
            public void onResponse(Index indexToDelete) {
                // This flag is checked at this point so that we always check that the supplied index ID
                // does correspond to a dangling index.
                if (deleteRequest.isAcceptDataLoss() == false) {
                    deleteListener.onFailure(new IllegalArgumentException("accept_data_loss must be set to true"));
                    return;
                }

                String indexName = indexToDelete.getName();
                String indexUUID = indexToDelete.getUUID();

                final ActionListener<AcknowledgedResponse> clusterStateUpdatedListener = new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        deleteListener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("Failed to delete dangling index [" + indexName + "] [" + indexUUID + "]", e);
                        deleteListener.onFailure(e);
                    }
                };

                final String taskSource = "delete-dangling-index [" + indexName + "] [" + indexUUID + "]";

                clusterService.submitStateUpdateTask(
                    taskSource,
                    new AckedClusterStateUpdateTask<AcknowledgedResponse>(deleteRequest, clusterStateUpdatedListener) {

                        @Override
                        protected AcknowledgedResponse newResponse(boolean acknowledged) {
                            return new AcknowledgedResponse(acknowledged);
                        }

                        @Override
                        public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                            return deleteDanglingIndexTaskKey;
                        }

                        @Override
                        public ClusterState execute(final ClusterState currentState) {
                            return deleteDanglingIndex(currentState, indexToDelete);
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Failed to find dangling index [" + deleteRequest.getIndexUUID() + "]", e);
                deleteListener.onFailure(e);
            }
        });
    }

    private ClusterState deleteDanglingIndex(ClusterState currentState, Index indexToDelete) {
        final Metadata metaData = currentState.getMetadata();

        for (final IndexMetadata each : metaData.indices().values()) {
            if (indexToDelete.getUUID().equals(each.getIndexUUID())) {
                throw new IllegalArgumentException(
                    "Refusing to delete dangling index "
                        + indexToDelete
                        + " as an index with UUID ["
                        + indexToDelete.getUUID()
                        + "] already exists in the cluster state"
                );
            }
        }

        // By definition, a dangling index is an index not present in the cluster state and with no tombstone,
        // so we shouldn't reach this point if these conditions aren't met. For super-safety, however, check
        // that a tombstone doesn't already exist for this index.
        if (metaData.indexGraveyard().containsIndex(indexToDelete)) {
            return currentState;
        }

        Metadata.Builder metaDataBuilder = Metadata.builder(metaData);

        final IndexGraveyard newGraveyard = IndexGraveyard.builder(metaDataBuilder.indexGraveyard())
            .addTombstone(indexToDelete)
            .build(settings);
        metaDataBuilder.indexGraveyard(newGraveyard);

        return ClusterState.builder(currentState).metadata(metaDataBuilder.build()).build();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDanglingIndexRequest request, ClusterState state) {
        return null;
    }

    private void findDanglingIndex(String indexUUID, ActionListener<Index> listener) {
        this.nodeClient.execute(
            ListDanglingIndicesAction.INSTANCE,
            new ListDanglingIndicesRequest(indexUUID),
            new ActionListener<ListDanglingIndicesResponse>() {
                @Override
                public void onResponse(ListDanglingIndicesResponse response) {
                    if (response.hasFailures()) {
                        final String nodeIds = response.failures()
                            .stream()
                            .map(FailedNodeException::nodeId)
                            .collect(Collectors.joining(","));
                        OpenSearchException e = new OpenSearchException("Failed to query nodes [" + nodeIds + "]");

                        for (FailedNodeException failure : response.failures()) {
                            logger.error("Failed to query node [" + failure.nodeId() + "]", failure);
                            e.addSuppressed(failure);
                        }

                        listener.onFailure(e);
                        return;
                    }

                    final List<NodeListDanglingIndicesResponse> nodes = response.getNodes();

                    for (NodeListDanglingIndicesResponse nodeResponse : nodes) {
                        for (DanglingIndexInfo each : nodeResponse.getDanglingIndices()) {
                            if (each.getIndexUUID().equals(indexUUID)) {
                                listener.onResponse(new Index(each.getIndexName(), each.getIndexUUID()));
                                return;
                            }
                        }
                    }

                    listener.onFailure(new IllegalArgumentException("No dangling index found for UUID [" + indexUUID + "]"));
                }

                @Override
                public void onFailure(Exception exp) {
                    listener.onFailure(exp);
                }
            }
        );
    }
}
