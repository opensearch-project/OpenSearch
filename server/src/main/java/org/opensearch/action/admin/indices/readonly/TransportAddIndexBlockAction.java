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

package org.opensearch.action.admin.indices.readonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.index.Index;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

/**
 * Adds a single index level block to a given set of indices. Not only does it set the correct setting,
 * but it ensures that, in case of a write block, once successfully returning to the user, all shards
 * of the index are properly accounting for the block, for instance, when adding a write block all
 * in-flight writes to an index have been completed prior to the response being returned. These actions
 * are done in multiple cluster state updates (at least two). See also {@link TransportVerifyShardIndexBlockAction}
 * for the eventual delegation for shard-level verification.
 *
 * @opensearch.internal
 */
public class TransportAddIndexBlockAction extends TransportClusterManagerNodeAction<AddIndexBlockRequest, AddIndexBlockResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAddIndexBlockAction.class);

    private final MetadataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportAddIndexBlockAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexStateService indexStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            AddIndexBlockAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AddIndexBlockRequest::new,
            indexNameExpressionResolver
        );
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AddIndexBlockResponse read(StreamInput in) throws IOException {
        return new AddIndexBlockResponse(in);
    }

    @Override
    protected void doExecute(Task task, AddIndexBlockRequest request, ActionListener<AddIndexBlockResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(AddIndexBlockRequest request, ClusterState state) {
        if (request.getBlock().getBlock().levels().contains(ClusterBlockLevel.METADATA_WRITE)
            && state.blocks().global(ClusterBlockLevel.METADATA_WRITE).isEmpty()) {
            return null;
        }
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(AddIndexBlockRequest request, ClusterState state, ActionListener<AddIndexBlockResponse> listener)
        throws Exception {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final AddIndexBlockRequest request,
        final ClusterState state,
        final ActionListener<AddIndexBlockResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new AddIndexBlockResponse(true, false, Collections.emptyList()));
            return;
        }

        final AddIndexBlockClusterStateUpdateRequest addBlockRequest = new AddIndexBlockClusterStateUpdateRequest(
            request.getBlock(),
            task.getId()
        ).ackTimeout(request.timeout()).masterNodeTimeout(request.clusterManagerNodeTimeout()).indices(concreteIndices);
        indexStateService.addIndexBlock(addBlockRequest, ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
            logger.debug(() -> new ParameterizedMessage("failed to mark indices as readonly [{}]", (Object) concreteIndices), t);
            delegatedListener.onFailure(t);
        }));
    }
}
