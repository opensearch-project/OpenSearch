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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.seqno;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Background global checkpoint sync action initiated when a shard goes inactive. This is needed because while we send the global checkpoint
 * on every replication operation, after the last operation completes the global checkpoint could advance but without a follow-up operation
 * the global checkpoint will never be synced to the replicas.
 *
 * @opensearch.internal
 */
public class GlobalCheckpointSyncAction extends TransportReplicationAction<
    GlobalCheckpointSyncAction.Request,
    GlobalCheckpointSyncAction.Request,
    ReplicationResponse> {

    public static String ACTION_NAME = "indices:admin/seq_no/global_checkpoint_sync";

    @Inject
    public GlobalCheckpointSyncAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction shardStateAction,
        final ActionFilters actionFilters
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            Request::new,
            Request::new,
            ThreadPool.Names.MANAGEMENT
        );
    }

    public void updateGlobalCheckpointForShard(final ShardId shardId) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            execute(new Request(shardId), ActionListener.wrap(r -> {}, e -> {
                if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                    logger.info(new ParameterizedMessage("{} global checkpoint sync failed", shardId), e);
                }
            }));
        }
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        Request request,
        IndexShard indexShard,
        ActionListener<PrimaryResult<Request, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            maybeSyncTranslog(indexShard);
            return new PrimaryResult<>(request, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            maybeSyncTranslog(replica);
            return new ReplicaResult();
        });
    }

    private void maybeSyncTranslog(final IndexShard indexShard) throws IOException {
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST
            && indexShard.getLastSyncedGlobalCheckpoint() < indexShard.getLastKnownGlobalCheckpoint()
            && indexShard.isRemoteTranslogEnabled() == false) {
            indexShard.sync();
        }
    }

    /**
     * Request for checkpoint sync action
     *
     * @opensearch.internal
     */
    public static final class Request extends ReplicationRequest<Request> {

        private Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(final ShardId shardId) {
            super(shardId);
        }

        @Override
        public String toString() {
            return "GlobalCheckpointSyncAction.Request{"
                + "shardId="
                + shardId
                + ", timeout="
                + timeout
                + ", index='"
                + index
                + '\''
                + ", waitForActiveShards="
                + waitForActiveShards
                + "}";
        }
    }

}
