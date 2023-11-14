/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

/**
 * This class runs tests with remote store + segRep while blocking file downloads
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationUsingRemoteStoreDisruptionIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(1);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testCancelReplicationWhileSyncingSegments() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, 0d, "metadata", Long.MAX_VALUE, 1);

        final Set<String> dataNodeNames = internalCluster().getDataNodeNames();
        final String replicaNode = getNode(dataNodeNames, false);
        final String primaryNode = getNode(dataNodeNames, true);

        SegmentReplicationTargetService targetService = internalCluster().getInstance(SegmentReplicationTargetService.class, replicaNode);
        ensureGreen(INDEX_NAME);
        blockNodeOnAnySegmentFile(REPOSITORY_NAME, replicaNode);
        final IndexShard indexShard = getIndexShard(replicaNode, INDEX_NAME);
        indexSingleDoc();
        refresh(INDEX_NAME);
        waitForBlock(replicaNode, REPOSITORY_NAME, TimeValue.timeValueSeconds(10));
        final SegmentReplicationState state = targetService.getOngoingEventSegmentReplicationState(indexShard.shardId());
        assertEquals(SegmentReplicationState.Stage.GET_FILES, state.getStage());
        ReplicationCollection.ReplicationRef<SegmentReplicationTarget> segmentReplicationTargetReplicationRef = targetService.get(
            state.getReplicationId()
        );
        final SegmentReplicationTarget segmentReplicationTarget = segmentReplicationTargetReplicationRef.get();
        // close the target ref here otherwise it will hold a refcount
        segmentReplicationTargetReplicationRef.close();
        assertNotNull(segmentReplicationTarget);
        assertTrue(segmentReplicationTarget.refCount() > 0);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        assertBusy(() -> {
            assertTrue(indexShard.routingEntry().primary());
            assertNull(targetService.getOngoingEventSegmentReplicationState(indexShard.shardId()));
            assertEquals("Target should be closed", 0, segmentReplicationTarget.refCount());
        });
        unblockNode(REPOSITORY_NAME, replicaNode);
        cleanupRepo();
    }

    public void testCancelReplicationWhileFetchingMetadata() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, 0d, "metadata", Long.MAX_VALUE, 1);

        final Set<String> dataNodeNames = internalCluster().getDataNodeNames();
        final String replicaNode = getNode(dataNodeNames, false);
        final String primaryNode = getNode(dataNodeNames, true);

        SegmentReplicationTargetService targetService = internalCluster().getInstance(SegmentReplicationTargetService.class, replicaNode);
        ensureGreen(INDEX_NAME);
        blockNodeOnAnyFiles(REPOSITORY_NAME, replicaNode);
        final IndexShard indexShard = getIndexShard(replicaNode, INDEX_NAME);
        indexSingleDoc();
        refresh(INDEX_NAME);
        waitForBlock(replicaNode, REPOSITORY_NAME, TimeValue.timeValueSeconds(10));
        final SegmentReplicationState state = targetService.getOngoingEventSegmentReplicationState(indexShard.shardId());
        assertEquals(SegmentReplicationState.Stage.GET_CHECKPOINT_INFO, state.getStage());
        ReplicationCollection.ReplicationRef<SegmentReplicationTarget> segmentReplicationTargetReplicationRef = targetService.get(
            state.getReplicationId()
        );
        final SegmentReplicationTarget segmentReplicationTarget = segmentReplicationTargetReplicationRef.get();
        // close the target ref here otherwise it will hold a refcount
        segmentReplicationTargetReplicationRef.close();
        assertNotNull(segmentReplicationTarget);
        assertTrue(segmentReplicationTarget.refCount() > 0);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        assertBusy(() -> {
            assertTrue(indexShard.routingEntry().primary());
            assertNull(targetService.getOngoingEventSegmentReplicationState(indexShard.shardId()));
            assertEquals("Target should be closed", 0, segmentReplicationTarget.refCount());
        });
        unblockNode(REPOSITORY_NAME, replicaNode);
        cleanupRepo();
    }

    private String getNode(Set<String> dataNodeNames, boolean primary) {
        assertEquals(2, dataNodeNames.size());
        for (String name : dataNodeNames) {
            final IndexShard indexShard = getIndexShard(name, INDEX_NAME);
            if (indexShard.routingEntry().primary() == primary) {
                return name;
            }
        }
        return null;
    }

    private IndexShard getIndexShard(String node, String indexName) {
        final Index index = resolveIndex(indexName);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexService(index);
        assertNotNull(indexService);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return shardId.map(indexService::getShard).orElse(null);
    }
}
