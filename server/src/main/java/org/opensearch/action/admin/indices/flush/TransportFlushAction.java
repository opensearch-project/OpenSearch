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

package org.opensearch.action.admin.indices.flush;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportBroadcastReplicationAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.shard.ShardId;
import org.opensearch.transport.TransportService;

import java.util.List;

/**
 * Flush ActionType.
 *
 * @opensearch.internal
 */
public class TransportFlushAction extends TransportBroadcastReplicationAction<
    FlushRequest,
    FlushResponse,
    ShardFlushRequest,
    ReplicationResponse> {

    @Inject
    public TransportFlushAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransportShardFlushAction replicatedFlushAction
    ) {
        super(
            FlushAction.NAME,
            FlushRequest::new,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            replicatedFlushAction
        );
    }

    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }

    @Override
    protected ShardFlushRequest newShardRequest(FlushRequest request, ShardId shardId) {
        return new ShardFlushRequest(request, shardId);
    }

    @Override
    protected FlushResponse newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        return new FlushResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
