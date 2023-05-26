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

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for flushing one or more indices
 *
 * @opensearch.internal
 */
public class TransportShardFlushAction extends TransportReplicationAction<ShardFlushRequest, ShardFlushRequest, ReplicationResponse> {

    public static final String NAME = FlushAction.NAME + "[s]";

    @Inject
    public TransportShardFlushAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters
    ) {
        super(
            settings,
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardFlushRequest::new,
            ShardFlushRequest::new,
            ThreadPool.Names.FLUSH
        );
        transportService.registerRequestHandler(
            PRE_SYNCED_FLUSH_ACTION_NAME,
            ThreadPool.Names.FLUSH,
            PreShardSyncedFlushRequest::new,
            new PreSyncedFlushTransportHandler(indicesService)
        );
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        ShardFlushRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardFlushRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            primary.flush(shardRequest.getRequest());
            logger.trace("{} flush request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardFlushRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            replica.flush(request.getRequest());
            logger.trace("{} flush request executed on replica", replica.shardId());
            return new ReplicaResult();
        });
    }

    // TODO: Remove this transition in OpenSearch 3.0
    private static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";

    /**
     * A Pre Shard Synced Flush Request
     *
     * @opensearch.internal
     */
    private static class PreShardSyncedFlushRequest extends TransportRequest {
        private final ShardId shardId;

        private PreShardSyncedFlushRequest(StreamInput in) throws IOException {
            super(in);
            assert in.getVersion().before(Version.V_2_0_0) : "received pre_sync request from a new node";
            this.shardId = new ShardId(in);
        }

        @Override
        public String toString() {
            return "PreShardSyncedFlushRequest{" + "shardId=" + shardId + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false : "must not send pre_sync request from a new node";
            throw new UnsupportedOperationException("");
        }
    }

    /**
     * Pre synced flush handler for the transport layer
     *
     * @opensearch.internal
     */
    private static final class PreSyncedFlushTransportHandler implements TransportRequestHandler<PreShardSyncedFlushRequest> {
        private final IndicesService indicesService;

        PreSyncedFlushTransportHandler(IndicesService indicesService) {
            this.indicesService = indicesService;
        }

        @Override
        public void messageReceived(PreShardSyncedFlushRequest request, TransportChannel channel, Task task) {
            IndexShard indexShard = indicesService.indexServiceSafe(request.shardId.getIndex()).getShard(request.shardId.id());
            indexShard.flush(new FlushRequest().force(false).waitIfOngoing(true));
            throw new UnsupportedOperationException("Synced flush was removed and a normal flush was performed instead.");
        }
    }
}
