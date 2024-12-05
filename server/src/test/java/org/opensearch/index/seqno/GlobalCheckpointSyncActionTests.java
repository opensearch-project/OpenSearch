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

package org.opensearch.index.seqno;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GlobalCheckpointSyncActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
    }

    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testTranslogSyncAfterGlobalCheckpointSync() throws Exception {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        final Translog.Durability durability = randomFrom(Translog.Durability.ASYNC, Translog.Durability.REQUEST);
        when(indexShard.getTranslogDurability()).thenReturn(durability);

        final long globalCheckpoint = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        final long lastSyncedGlobalCheckpoint;
        if (randomBoolean() && globalCheckpoint != SequenceNumbers.NO_OPS_PERFORMED) {
            lastSyncedGlobalCheckpoint = randomIntBetween(
                Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED),
                Math.toIntExact(globalCheckpoint) - 1
            );
            assert lastSyncedGlobalCheckpoint < globalCheckpoint;
        } else {
            lastSyncedGlobalCheckpoint = globalCheckpoint;
        }

        when(indexShard.getLastKnownGlobalCheckpoint()).thenReturn(globalCheckpoint);
        when(indexShard.getLastSyncedGlobalCheckpoint()).thenReturn(lastSyncedGlobalCheckpoint);

        final GlobalCheckpointSyncAction action = new GlobalCheckpointSyncAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );
        final GlobalCheckpointSyncAction.Request primaryRequest = new GlobalCheckpointSyncAction.Request(indexShard.shardId());
        if (randomBoolean()) {
            action.shardOperationOnPrimary(primaryRequest, indexShard, ActionTestUtils.assertNoFailureListener(r -> {}));
        } else {
            action.shardOperationOnReplica(
                new GlobalCheckpointSyncAction.Request(indexShard.shardId()),
                indexShard,
                ActionTestUtils.assertNoFailureListener(r -> {})
            );
        }

        if (durability == Translog.Durability.ASYNC || lastSyncedGlobalCheckpoint == globalCheckpoint) {
            verify(indexShard, never()).sync();
        } else {
            verify(indexShard).sync();
        }
    }

    public void testGetReplicationModeWithRemoteTranslog() {
        final GlobalCheckpointSyncAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithLocalTranslog() {
        final GlobalCheckpointSyncAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    private GlobalCheckpointSyncAction createAction() {
        final IndicesService indicesService = mock(IndicesService.class);
        return new GlobalCheckpointSyncAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            new ActionFilters(Collections.emptySet())
        );
    }

    public void testMayBeSyncTranslogWithRemoteTranslog() throws Exception {
        final GlobalCheckpointSyncAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(true);
        final GlobalCheckpointSyncAction.Request primaryRequest = new GlobalCheckpointSyncAction.Request(indexShard.shardId());
        final long globalCheckpoint = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        when(indexShard.getLastKnownGlobalCheckpoint()).thenReturn(globalCheckpoint);
        when(indexShard.getLastSyncedGlobalCheckpoint()).thenReturn(globalCheckpoint - 1);
        when(indexShard.getTranslogDurability()).thenReturn(Translog.Durability.REQUEST);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));

        action.shardOperationOnPrimary(primaryRequest, indexShard, ActionTestUtils.assertNoFailureListener(r -> {}));
        verify(indexShard, never()).sync();
    }

    public void testMayBeSyncTranslogWithLocalTranslog() throws Exception {
        final GlobalCheckpointSyncAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(false);
        final GlobalCheckpointSyncAction.Request primaryRequest = new GlobalCheckpointSyncAction.Request(indexShard.shardId());
        final long globalCheckpoint = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        when(indexShard.getLastKnownGlobalCheckpoint()).thenReturn(globalCheckpoint);
        when(indexShard.getLastSyncedGlobalCheckpoint()).thenReturn(globalCheckpoint - 1);
        when(indexShard.getTranslogDurability()).thenReturn(Translog.Durability.REQUEST);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));

        action.shardOperationOnPrimary(primaryRequest, indexShard, ActionTestUtils.assertNoFailureListener(r -> {}));
        verify(indexShard).sync();
    }

}
