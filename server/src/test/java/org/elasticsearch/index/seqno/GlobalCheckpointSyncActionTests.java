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

package org.elasticsearch.index.seqno;

import org.opensearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;

import static org.elasticsearch.mock.orig.Mockito.never;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GlobalCheckpointSyncActionTests extends ESTestCase {

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
        transportService = transport.createTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,  boundAddress -> clusterService.localNode(), null, Collections.emptySet());
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

        final Translog.Durability durability = randomFrom(Translog.Durability.ASYNC, Translog.Durability.REQUEST);
        when(indexShard.getTranslogDurability()).thenReturn(durability);

        final long globalCheckpoint = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        final long lastSyncedGlobalCheckpoint;
        if (randomBoolean() && globalCheckpoint != SequenceNumbers.NO_OPS_PERFORMED) {
            lastSyncedGlobalCheckpoint =
                    randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Math.toIntExact(globalCheckpoint) - 1);
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
            new ActionFilters(Collections.emptySet()));
        final GlobalCheckpointSyncAction.Request primaryRequest = new GlobalCheckpointSyncAction.Request(indexShard.shardId());
        if (randomBoolean()) {
            action.shardOperationOnPrimary(primaryRequest, indexShard, ActionTestUtils.assertNoFailureListener(r -> {}));
        } else {
            action.shardOperationOnReplica(new GlobalCheckpointSyncAction.Request(indexShard.shardId()), indexShard,
                ActionTestUtils.assertNoFailureListener(r -> {}));
        }

        if (durability == Translog.Durability.ASYNC || lastSyncedGlobalCheckpoint == globalCheckpoint) {
            verify(indexShard, never()).sync();
        } else {
            verify(indexShard).sync();
        }
    }

}
