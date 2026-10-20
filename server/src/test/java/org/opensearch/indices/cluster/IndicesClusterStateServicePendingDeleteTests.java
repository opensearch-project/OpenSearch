/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.PrimaryReplicaSyncer;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.MergedSegmentPublisher;
import org.opensearch.indices.replication.checkpoint.ReferencedSegmentsPublisher;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the pending deletion of a deleted index is retried, rather than abandoned, while a shard lock of that
 * index is still held (for example by a recovery that is still writing files).
 */
public class IndicesClusterStateServicePendingDeleteTests extends AbstractIndicesClusterStateServiceTestCase {

    private final List<Scheduled> scheduled = new ArrayList<>();
    private ExecutorService generic;

    private record Scheduled(Runnable command, TimeValue delay) {
    }

    /** {@link MockIndicesService} whose pending-delete processing fails with a held shard lock a given number of times. */
    private class LockedIndicesService extends MockIndicesService {
        private final int failures;
        private int calls;

        LockedIndicesService(int failures) {
            this.failures = failures;
        }

        @Override
        public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException,
            InterruptedException, ShardLockObtainFailedException {
            calls++;
            if (calls <= failures) {
                throw new ShardLockObtainFailedException(new ShardId(index, 2), "lock already held for [starting shard]");
            }
        }
    }

    public void testRetryDelayDoublesUpToCap() {
        assertEquals(TimeValue.timeValueMinutes(1), IndicesClusterStateService.PendingIndexDeletion.retryDelay(1));
        assertEquals(TimeValue.timeValueMinutes(2), IndicesClusterStateService.PendingIndexDeletion.retryDelay(2));
        assertEquals(TimeValue.timeValueMinutes(4), IndicesClusterStateService.PendingIndexDeletion.retryDelay(3));
        assertEquals(TimeValue.timeValueMinutes(16), IndicesClusterStateService.PendingIndexDeletion.retryDelay(5));
        assertEquals(
            IndicesClusterStateService.PENDING_DELETE_MAX_RETRY_DELAY,
            IndicesClusterStateService.PendingIndexDeletion.retryDelay(6)
        );
        assertEquals(
            IndicesClusterStateService.PENDING_DELETE_MAX_RETRY_DELAY,
            IndicesClusterStateService.PendingIndexDeletion.retryDelay(40)
        );
    }

    public void testPendingDeleteIsRetriedUntilLockIsReleased() {
        final int failures = randomIntBetween(1, 4);
        final LockedIndicesService indicesService = new LockedIndicesService(failures);
        final IndicesClusterStateService service = createService(indicesService);
        final IndicesClusterStateService.PendingIndexDeletion deletion = service.new PendingIndexDeletion(index(), indexSettings());

        deletion.run();
        for (int attempt = 1; attempt <= failures; attempt++) {
            assertEquals("attempt " + attempt + " should have been rescheduled", attempt, scheduled.size());
            final Scheduled next = scheduled.get(attempt - 1);
            assertEquals(IndicesClusterStateService.PendingIndexDeletion.retryDelay(attempt), next.delay());
            next.command().run();
        }
        // the lock was released: the last attempt succeeded and nothing more is scheduled
        assertEquals(failures + 1, indicesService.calls);
        assertEquals(failures + 1, deletion.attempts());
        assertEquals(failures, scheduled.size());
    }

    public void testPendingDeleteKeepsRetryingWhileLockIsHeld() {
        final LockedIndicesService indicesService = new LockedIndicesService(Integer.MAX_VALUE);
        final IndicesClusterStateService service = createService(indicesService);
        final IndicesClusterStateService.PendingIndexDeletion deletion = service.new PendingIndexDeletion(index(), indexSettings());

        deletion.run();
        final int attempts = randomIntBetween(10, 50);
        for (int attempt = 1; attempt < attempts; attempt++) {
            assertEquals(attempt, scheduled.size());
            scheduled.get(attempt - 1).command().run();
        }
        // never gives up: every attempt reschedules, and the delay settles at the cap
        assertEquals(attempts, scheduled.size());
        assertEquals(attempts, indicesService.calls);
        assertEquals(IndicesClusterStateService.PENDING_DELETE_MAX_RETRY_DELAY, scheduled.get(attempts - 1).delay());
    }

    public void testInFlightIndexIsReleasedOnceDeletionCompletes() {
        final LockedIndicesService indicesService = new LockedIndicesService(1);
        final IndicesClusterStateService service = createService(indicesService);
        final Index index = index();
        final Set<Index> inFlight = service.pendingIndexDeletions();

        assertTrue(inFlight.add(index));
        final IndicesClusterStateService.PendingIndexDeletion deletion = service.new PendingIndexDeletion(index, indexSettings());
        deletion.run(); // the lock is held: fails once and reschedules
        assertTrue("index must stay marked in-flight while retrying", inFlight.contains(index));

        scheduled.get(0).command().run(); // the lock was released: succeeds
        assertFalse("index must be released by the successful attempt", inFlight.contains(index));
    }

    public void testDeletionOfSameIndexIsScheduledOnlyOnceWhileRetrying() {
        final LockedIndicesService indicesService = new LockedIndicesService(Integer.MAX_VALUE);
        final IndicesClusterStateService service = createService(indicesService);
        service.start();

        // a cluster that knows the index, then two consecutive states in which it is deleted. The second one re-reports
        // the deletion, as a node that re-joins the cluster or receives a tombstone-based state does.
        final IndexMetadata metadata = IndexMetadata.builder("deleted")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(3)
            .numberOfReplicas(0)
            .build();
        final DiscoveryNode node = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(node)
            .localNodeId(node.getId())
            .clusterManagerNodeId(node.getId())
            .build();
        final ClusterState withIndex = ClusterState.builder(new ClusterName("test"))
            .nodes(nodes)
            .metadata(Metadata.builder().put(metadata, false))
            .build();
        final ClusterState withoutIndex = ClusterState.builder(withIndex)
            .metadata(Metadata.builder(withIndex.metadata()).remove("deleted"))
            .build();

        service.applyClusterState(new ClusterChangedEvent("index deleted", withoutIndex, withIndex));
        service.applyClusterState(new ClusterChangedEvent("index deleted again", withoutIndex, withIndex));

        verify(generic, times(1)).execute(any(Runnable.class));
        assertEquals(1, service.pendingIndexDeletions().size());
    }

    private static Index index() {
        return new Index("deleted", UUIDs.randomBase64UUID());
    }

    private static IndexSettings indexSettings() {
        final IndexMetadata metadata = IndexMetadata.builder("deleted")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(3)
            .numberOfReplicas(0)
            .build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    private IndicesClusterStateService createService(MockIndicesService indicesService) {
        final ThreadPool threadPool = mock(ThreadPool.class);
        generic = mock(ExecutorService.class);
        when(threadPool.generic()).thenReturn(generic);
        doAnswer(invocation -> {
            scheduled.add(new Scheduled(invocation.getArgument(2), invocation.getArgument(0)));
            return null;
        }).when(threadPool).scheduleUnlessShuttingDown(any(TimeValue.class), eq(ThreadPool.Names.GENERIC), any(Runnable.class));
        final Settings settings = Settings.builder().put("node.name", "node").build();
        final TransportService transportService = new TransportService(
            settings,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        final ClusterService clusterService = mock(ClusterService.class);
        final RepositoriesService repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            Collections.emptyMap(),
            Collections.emptyMap(),
            threadPool
        );
        final PeerRecoveryTargetService recoveryTargetService = new PeerRecoveryTargetService(
            threadPool,
            transportService,
            null,
            clusterService
        );
        return new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            threadPool,
            SegmentReplicationCheckpointPublisher.EMPTY,
            mock(SegmentReplicationTargetService.class),
            mock(SegmentReplicationSourceService.class),
            recoveryTargetService,
            mock(ShardStateAction.class),
            null,
            repositoriesService,
            null,
            null,
            null,
            mock(PrimaryReplicaSyncer.class),
            s -> {},
            RetentionLeaseSyncer.EMPTY,
            null,
            null,
            MergedSegmentPublisher.EMPTY,
            ReferencedSegmentsPublisher.EMPTY
        );
    }
}
