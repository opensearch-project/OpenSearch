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

package org.opensearch.action.admin.cluster.snapshots.create;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.snapshots.SnapshotsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.repositories.blobstore.BlobStoreRepository.SHALLOW_SNAPSHOT_V2;

/**
 * Transport action for create snapshot operation
 *
 * @opensearch.internal
 */
public class TransportCreateSnapshotAction extends TransportClusterManagerNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportCreateSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SnapshotsService snapshotsService,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            CreateSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateSnapshotRequest::new,
            indexNameExpressionResolver
        );
        this.snapshotsService = snapshotsService;
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateSnapshotResponse read(StreamInput in) throws IOException {
        return new CreateSnapshotResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSnapshotRequest request, ClusterState state) {
        // We only check metadata block, as we want to snapshot closed indices (which have a read block)
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (clusterBlockException != null) {
            return clusterBlockException;
        }
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final CreateSnapshotRequest request,
        ClusterState state,
        final ActionListener<CreateSnapshotResponse> listener
    ) {
        Repository repository = repositoriesService.repository(request.repository());
        boolean isSnapshotV2 = SHALLOW_SNAPSHOT_V2.get(repository.getMetadata().settings());
        if (request.waitForCompletion()) {
            snapshotsService.executeSnapshot(request, ActionListener.map(listener, CreateSnapshotResponse::new));
        } else if (isSnapshotV2) {
            snapshotsService.executeSnapshot(request, ActionListener.map(listener, snapshot -> new CreateSnapshotResponse()));
        } else {
            snapshotsService.createSnapshot(request, ActionListener.map(listener, snapshot -> new CreateSnapshotResponse()));
        }
    }
}
