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

package org.opensearch.action.admin.indices.dangling.import_index;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.opensearch.action.admin.indices.dangling.find.FindDanglingIndexRequest;
import org.opensearch.action.admin.indices.dangling.find.FindDanglingIndexResponse;
import org.opensearch.action.admin.indices.dangling.find.NodeFindDanglingIndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.inject.Inject;
import org.opensearch.gateway.LocalAllocateDangledIndices;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Implements the import of a dangling index. When handling a {@link ImportDanglingIndexAction},
 * this class first checks that such a dangling index exists. It then calls {@link LocalAllocateDangledIndices}
 * to perform the actual allocation.
 *
 * @opensearch.internal
 */
public class TransportImportDanglingIndexAction extends HandledTransportAction<ImportDanglingIndexRequest, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportImportDanglingIndexAction.class);

    private final LocalAllocateDangledIndices danglingIndexAllocator;
    private final NodeClient nodeClient;

    @Inject
    public TransportImportDanglingIndexAction(
        ActionFilters actionFilters,
        TransportService transportService,
        LocalAllocateDangledIndices danglingIndexAllocator,
        NodeClient nodeClient
    ) {
        super(ImportDanglingIndexAction.NAME, transportService, actionFilters, ImportDanglingIndexRequest::new);
        this.danglingIndexAllocator = danglingIndexAllocator;
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(Task task, ImportDanglingIndexRequest importRequest, ActionListener<AcknowledgedResponse> importListener) {
        findDanglingIndex(importRequest, new ActionListener<IndexMetadata>() {
            @Override
            public void onResponse(IndexMetadata indexMetaDataToImport) {
                // This flag is checked at this point so that we always check that the supplied index UUID
                // does correspond to a dangling index.
                if (importRequest.isAcceptDataLoss() == false) {
                    importListener.onFailure(new IllegalArgumentException("accept_data_loss must be set to true"));
                    return;
                }

                String indexName = indexMetaDataToImport.getIndex().getName();
                String indexUUID = indexMetaDataToImport.getIndexUUID();

                danglingIndexAllocator.allocateDangled(
                    singletonList(indexMetaDataToImport),
                    new ActionListener<LocalAllocateDangledIndices.AllocateDangledResponse>() {
                        @Override
                        public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse allocateDangledResponse) {
                            importListener.onResponse(new AcknowledgedResponse(true));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug("Failed to import dangling index [" + indexName + "] [" + indexUUID + "]", e);
                            importListener.onFailure(e);
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Failed to find dangling index [" + importRequest.getIndexUUID() + "]", e);
                importListener.onFailure(e);
            }
        });
    }

    private void findDanglingIndex(ImportDanglingIndexRequest request, ActionListener<IndexMetadata> listener) {
        final String indexUUID = request.getIndexUUID();

        this.nodeClient.execute(
            FindDanglingIndexAction.INSTANCE,
            new FindDanglingIndexRequest(indexUUID),
            new ActionListener<FindDanglingIndexResponse>() {
                @Override
                public void onResponse(FindDanglingIndexResponse response) {
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

                    final List<IndexMetadata> metaDataSortedByVersion = new ArrayList<>();
                    for (NodeFindDanglingIndexResponse each : response.getNodes()) {
                        metaDataSortedByVersion.addAll(each.getDanglingIndexInfo());
                    }
                    metaDataSortedByVersion.sort(Comparator.comparingLong(IndexMetadata::getVersion));

                    if (metaDataSortedByVersion.isEmpty()) {
                        listener.onFailure(new IllegalArgumentException("No dangling index found for UUID [" + indexUUID + "]"));
                        return;
                    }

                    logger.debug(
                        "Metadata versions {} found for index UUID [{}], selecting the highest",
                        metaDataSortedByVersion.stream().map(IndexMetadata::getVersion).collect(Collectors.toList()),
                        indexUUID
                    );

                    listener.onResponse(metaDataSortedByVersion.get(metaDataSortedByVersion.size() - 1));
                }

                @Override
                public void onFailure(Exception exp) {
                    listener.onFailure(exp);
                }
            }
        );
    }
}
