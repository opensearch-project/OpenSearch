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

package org.opensearch.action.admin.indices.dangling.list;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.gateway.DanglingIndicesState;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the listing of all dangling indices. All nodes in the cluster are queried, and
 * their answers aggregated. Finding dangling indices is performed in {@link DanglingIndicesState}.
 *
 * @opensearch.internal
 */
public class TransportListDanglingIndicesAction extends TransportNodesAction<
    ListDanglingIndicesRequest,
    ListDanglingIndicesResponse,
    NodeListDanglingIndicesRequest,
    NodeListDanglingIndicesResponse> {
    private final TransportService transportService;
    private final DanglingIndicesState danglingIndicesState;

    @Inject
    public TransportListDanglingIndicesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DanglingIndicesState danglingIndicesState
    ) {
        super(
            ListDanglingIndicesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ListDanglingIndicesRequest::new,
            NodeListDanglingIndicesRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeListDanglingIndicesResponse.class
        );
        this.transportService = transportService;
        this.danglingIndicesState = danglingIndicesState;
    }

    @Override
    protected ListDanglingIndicesResponse newResponse(
        ListDanglingIndicesRequest request,
        List<NodeListDanglingIndicesResponse> nodeListDanglingIndicesResponse,
        List<FailedNodeException> failures
    ) {
        return new ListDanglingIndicesResponse(clusterService.getClusterName(), nodeListDanglingIndicesResponse, failures);
    }

    @Override
    protected NodeListDanglingIndicesRequest newNodeRequest(ListDanglingIndicesRequest request) {
        return new NodeListDanglingIndicesRequest(request.getIndexUUID());
    }

    @Override
    protected NodeListDanglingIndicesResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeListDanglingIndicesResponse(in);
    }

    @Override
    protected NodeListDanglingIndicesResponse nodeOperation(NodeListDanglingIndicesRequest request) {
        final DiscoveryNode localNode = transportService.getLocalNode();

        final List<DanglingIndexInfo> indexMetaData = new ArrayList<>();

        final String indexFilter = request.getIndexUUID();

        for (IndexMetadata each : danglingIndicesState.getDanglingIndices().values()) {
            if (indexFilter == null || indexFilter.equals(each.getIndexUUID())) {
                DanglingIndexInfo danglingIndexInfo = new DanglingIndexInfo(
                    localNode.getId(),
                    each.getIndex().getName(),
                    each.getIndexUUID(),
                    each.getCreationDate()
                );
                indexMetaData.add(danglingIndexInfo);
            }
        }

        return new NodeListDanglingIndicesResponse(localNode, indexMetaData);
    }
}
