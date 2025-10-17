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

package org.opensearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.ActionType;
import org.opensearch.action.RequestValidators;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.opensearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.cluster.state.TransportClusterStateAction;
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.create.TransportCreateIndexAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.opensearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.opensearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.bulk.TransportBulkAction;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.resync.TransportResyncReplicationAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchExecutionStatsCollector;
import org.opensearch.action.search.SearchPhaseController;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestOperationsCompositeListenerFactory;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.term.GetTermVersionAction;
import org.opensearch.action.support.clustermanager.term.TransportGetTermVersionAction;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.index.NodeMappingRefreshAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.opensearch.cluster.coordination.ClusterBootstrapService;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.coordination.CoordinationState;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.coordination.CoordinatorTests;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.coordination.ElectionStrategy;
import org.opensearch.cluster.coordination.InMemoryPersistedState;
import org.opensearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.coordination.PersistedStateRegistry.PersistedStateType;
import org.opensearch.cluster.metadata.AliasValidator;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.MetadataDeleteIndexService;
import org.opensearch.cluster.metadata.MetadataIndexAliasesService;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.metadata.MetadataMappingService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.BatchedRerouteService;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.FakeThreadPoolClusterManagerService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.cache.module.CacheModule;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.gateway.MetaStateService;
import org.opensearch.gateway.TransportNodesListGatewayStartedShards;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.SegmentReplicationStatsTracker;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.mapper.MappingTransformerRegistry;
import org.opensearch.index.remote.RemoteStorePressureService;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.seqno.GlobalCheckpointSyncAction;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.PrimaryReplicaSyncer;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.SystemIndices;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.indices.recovery.DefaultRecoverySettings;
import org.opensearch.indices.recovery.PeerRecoverySourceService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.MergedSegmentPublisher;
import org.opensearch.indices.replication.checkpoint.ReferencedSegmentsPublisher;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.SystemIngestPipelineCache;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.snapshots.mockstore.MockEventuallyConsistentRepository;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.disruption.DisruptableMockTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.node.NodeClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.mockito.Mockito;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.opensearch.env.Environment.PATH_HOME_SETTING;
import static org.opensearch.monitor.StatusInfo.Status.HEALTHY;
import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.opensearch.node.Node.NODE_SEARCH_CACHE_SIZE_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotResiliencyTests extends OpenSearchTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    private TestClusterNodes testClusterNodes;

    private Path tempDir;

    /**
     * Context shared by all the node's {@link Repository} instances if the eventually consistent blobstore is to be used.
     * {@code null} if not using the eventually consistent blobstore.
     */
    @Nullable
    private MockEventuallyConsistentRepository.Context blobStoreContext;

    @Before
    public void createServices() {
        tempDir = createTempDir();
        if (randomBoolean()) {
            blobStoreContext = new MockEventuallyConsistentRepository.Context();
        }
        deterministicTaskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "shared").build(), random());
    }

    @After
    public void verifyReposThenStopServices() {
        try {
            clearDisruptionsAndAwaitSync();

            final StepListener<CleanupRepositoryResponse> cleanupResponse = new StepListener<>();
            final StepListener<CreateSnapshotResponse> createSnapshotResponse = new StepListener<>();
            // Create another snapshot and then clean up the repository to verify that the repository works correctly no matter the
            // failures seen during the previous test.
            client().admin()
                .cluster()
                .prepareCreateSnapshot("repo", "last-snapshot")
                .setWaitForCompletion(true)
                .setPartial(true)
                .execute(createSnapshotResponse);
            continueOrDie(createSnapshotResponse, r -> {
                final SnapshotInfo snapshotInfo = r.getSnapshotInfo();
                // Snapshot can be partial because some tests leave indices in a red state because data nodes were stopped
                assertThat(snapshotInfo.state(), either(is(SnapshotState.SUCCESS)).or(is(SnapshotState.PARTIAL)));
                assertThat(snapshotInfo.shardFailures(), iterableWithSize(snapshotInfo.failedShards()));
                assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards() - snapshotInfo.failedShards()));
                client().admin().cluster().cleanupRepository(new CleanupRepositoryRequest("repo"), cleanupResponse);
            });
            final AtomicBoolean cleanedUp = new AtomicBoolean(false);
            continueOrDie(cleanupResponse, r -> cleanedUp.set(true));

            runUntil(cleanedUp::get, TimeUnit.MINUTES.toMillis(1L));

            if (blobStoreContext != null) {
                blobStoreContext.forceConsistent();
            }
            BlobStoreTestUtil.assertConsistency(
                (BlobStoreRepository) testClusterNodes.randomClusterManagerNodeSafe().repositoriesService.repository("repo"),
                Runnable::run
            );
        } finally {
            testClusterNodes.nodes.values().forEach(TestClusterNodes.TestClusterNode::stop);
        }
    }

    public void testSuccessfulSnapshotAndRestore() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(0, 100);

        final TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseListener = new StepListener<>();

        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            final Runnable afterIndexing = () -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseListener);
            if (documents == 0) {
                afterIndexing.run();
            } else {
                final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int i = 0; i < documents; ++i) {
                    bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
                }
                final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
                client().bulk(bulkRequest, bulkResponseStepListener);
                continueOrDie(bulkResponseStepListener, bulkResponse -> {
                    assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                    assertEquals(documents, bulkResponse.getItems().length);
                    afterIndexing.run();
                });
            }
        });

        final StepListener<AcknowledgedResponse> deleteIndexListener = new StepListener<>();

        continueOrDie(
            createSnapshotResponseListener,
            createSnapshotResponse -> client().admin().indices().delete(new DeleteIndexRequest(index), deleteIndexListener)
        );

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new StepListener<>();
        continueOrDie(
            deleteIndexListener,
            ignored -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(repoName, snapshotName).waitForCompletion(true),
                    restoreSnapshotResponseListener
                )
        );

        final StepListener<SearchResponse> searchResponseListener = new StepListener<>();
        continueOrDie(restoreSnapshotResponseListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            client().search(
                new SearchRequest(index).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchResponseListener
            );
        });

        final AtomicBoolean documentCountVerified = new AtomicBoolean();
        continueOrDie(searchResponseListener, r -> {
            assertEquals(documents, Objects.requireNonNull(r.getHits().getTotalHits()).value());
            documentCountVerified.set(true);
        });

        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));
        assertNotNull(createSnapshotResponseListener.result());
        assertNotNull(restoreSnapshotResponseListener.result());
        assertTrue(documentCountVerified.get());
        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testSearchableSnapshotOverSubscription() {
        setupTestCluster(1, 2, 2);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(0, 100);

        final TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        Map<String, AggregateFileCacheStats> nodeFileCacheStats = new HashMap<>();
        for (TestClusterNodes.TestClusterNode node : testClusterNodes.nodes.values()) {
            nodeFileCacheStats.put(
                node.node.getId(),
                new AggregateFileCacheStats(
                    0,
                    new FileCacheStats(1, 0, 0, 0, 0, 0, 0, FileCacheStatsType.OVER_ALL_STATS),
                    new FileCacheStats(0, 0, 0, 0, 0, 0, 0, FileCacheStatsType.FULL_FILE_STATS),
                    new FileCacheStats(1, 0, 0, 0, 0, 0, 0, FileCacheStatsType.BLOCK_FILE_STATS),
                    new FileCacheStats(1, 0, 0, 0, 0, 0, 0, FileCacheStatsType.PINNED_FILE_STATS)
                )
            );
        }
        ClusterInfo clusterInfo = new ClusterInfo(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), nodeFileCacheStats, Map.of());
        testClusterNodes.nodes.values().forEach(node -> when(node.getMockClusterInfoService().getClusterInfo()).thenReturn(clusterInfo));

        final StepListener<CreateSnapshotResponse> createSnapshotResponseListener = new StepListener<>();

        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            final Runnable afterIndexing = () -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseListener);
            if (documents == 0) {
                afterIndexing.run();
            } else {
                final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int i = 0; i < documents; ++i) {
                    bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
                }
                final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
                client().bulk(bulkRequest, bulkResponseStepListener);
                continueOrDie(bulkResponseStepListener, bulkResponse -> {
                    assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                    assertEquals(documents, bulkResponse.getItems().length);
                    afterIndexing.run();
                });
            }
        });

        final StepListener<AcknowledgedResponse> deleteIndexListener = new StepListener<>();

        continueOrDie(
            createSnapshotResponseListener,
            createSnapshotResponse -> client().admin().indices().delete(new DeleteIndexRequest(index), deleteIndexListener)
        );

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new StepListener<>();
        continueOrDie(
            deleteIndexListener,
            ignored -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(repoName, snapshotName).waitForCompletion(true)
                        .storageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT),
                    restoreSnapshotResponseListener
                )
        );

        final AtomicBoolean exceptionVerified = new AtomicBoolean();

        restoreSnapshotResponseListener.whenComplete(null, restoreSnapshotException -> {
            Throwable throwable = restoreSnapshotException;
            if (restoreSnapshotException instanceof RemoteTransportException) {
                throwable = restoreSnapshotException.getCause();
            }
            try {
                assertTrue(throwable instanceof SnapshotRestoreException);
                assertTrue(
                    throwable.getMessage()
                        .contains(
                            "Size of the indexes to be restored exceeds the file cache bounds. Increase the file cache capacity on the cluster nodes using "
                                + NODE_SEARCH_CACHE_SIZE_SETTING.getKey()
                                + " setting."
                        )
                );
            } catch (SnapshotRestoreException ignored) {}
            exceptionVerified.set(true);
        });

        runUntil(exceptionVerified::get, TimeUnit.MINUTES.toMillis(5L));
        assertTrue(exceptionVerified.get());
        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testSnapshotWithNodeDisconnects() {
        final int dataNodes = randomIntBetween(2, 10);
        final int clusterManagerNodes = randomFrom(1, 3, 5);
        setupTestCluster(clusterManagerNodes, dataNodes);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        final boolean partial = randomBoolean();
        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                scheduleNow(this::disconnectRandomDataNode);
            }
            if (randomBoolean()) {
                scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
            }
            testClusterNodes.randomClusterManagerNodeSafe().client.admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setPartial(partial)
                .execute(createSnapshotResponseStepListener);
        });

        final AtomicBoolean snapshotNeverStarted = new AtomicBoolean(false);

        createSnapshotResponseStepListener.whenComplete(createSnapshotResponse -> {
            for (int i = 0; i < randomIntBetween(0, dataNodes); ++i) {
                scheduleNow(this::disconnectOrRestartDataNode);
            }
            // Only disconnect cluster-manager if we have more than a single cluster-manager and can simulate a failover
            final boolean disconnectedClusterManager = randomBoolean() && clusterManagerNodes > 1;
            if (disconnectedClusterManager) {
                scheduleNow(this::disconnectOrRestartClusterManagerNode);
            }
            if (disconnectedClusterManager || randomBoolean()) {
                scheduleSoon(() -> testClusterNodes.clearNetworkDisruptions());
            } else if (randomBoolean()) {
                scheduleNow(() -> testClusterNodes.clearNetworkDisruptions());
            }
        }, e -> {
            if (partial == false) {
                final SnapshotException unwrapped = (SnapshotException) ExceptionsHelper.unwrap(e, SnapshotException.class);
                assertNotNull(unwrapped);
                assertThat(unwrapped.getMessage(), endsWith("Indices don't have primary shards [test]"));
                snapshotNeverStarted.set(true);
            } else {
                throw new AssertionError(e);
            }
        });

        runUntil(() -> testClusterNodes.randomClusterManagerNode().map(clusterManager -> {
            if (snapshotNeverStarted.get()) {
                return true;
            }
            final SnapshotsInProgress snapshotsInProgress = clusterManager.clusterService.state().custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.entries().isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        final TestClusterNodes.TestClusterNode randomClusterManager = testClusterNodes.randomClusterManagerNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active cluster-manager node"));
        SnapshotsInProgress finalSnapshotsInProgress = randomClusterManager.clusterService.state()
            .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        assertThat(finalSnapshotsInProgress.entries(), empty());
        final Repository repository = randomClusterManager.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        if (snapshotNeverStarted.get()) {
            assertThat(snapshotIds, empty());
        } else {
            assertThat(snapshotIds, hasSize(1));
        }
    }

    public void testSnapshotDeleteWithClusterManagerFailover() {
        final int dataNodes = randomIntBetween(2, 10);
        final int clusterManagerNodes = randomFrom(3, 5);
        setupTestCluster(clusterManagerNodes, dataNodes);

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        final boolean waitForSnapshot = randomBoolean();
        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();
        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> testClusterNodes.randomClusterManagerNodeSafe().client.admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(waitForSnapshot)
                .execute(createSnapshotResponseStepListener)
        );

        final AtomicBoolean snapshotDeleteResponded = new AtomicBoolean(false);
        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(this::disconnectOrRestartClusterManagerNode);
            testClusterNodes.randomDataNodeSafe().client.admin()
                .cluster()
                .prepareDeleteSnapshot(repoName, snapshotName)
                .execute(ActionListener.wrap(() -> snapshotDeleteResponded.set(true)));
        });

        runUntil(
            () -> testClusterNodes.randomClusterManagerNode()
                .map(
                    clusterManager -> snapshotDeleteResponded.get()
                        && clusterManager.clusterService.state()
                            .custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
                            .getEntries()
                            .isEmpty()
                )
                .orElse(false),
            TimeUnit.MINUTES.toMillis(1L)
        );

        clearDisruptionsAndAwaitSync();

        final TestClusterNodes.TestClusterNode randomClusterManager = testClusterNodes.randomClusterManagerNode()
            .orElseThrow(() -> new AssertionError("expected to find at least one active cluster-manager node"));
        SnapshotsInProgress finalSnapshotsInProgress = randomClusterManager.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertThat(finalSnapshotsInProgress.entries(), empty());
        final Repository repository = randomClusterManager.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(0));
    }

    public void testConcurrentSnapshotCreateAndDelete() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .execute(createSnapshotResponseStepListener)
        );

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();

        clusterManagerNode.clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty() == false) {
                    client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute(deleteSnapshotStepListener);
                    clusterManagerNode.clusterService.removeListener(this);
                }
            }
        });

        final StepListener<CreateSnapshotResponse> createAnotherSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            deleteSnapshotStepListener,
            acknowledgedResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createAnotherSnapshotResponseStepListener)
        );
        continueOrDie(
            createAnotherSnapshotResponseStepListener,
            createSnapshotResponse -> assertEquals(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS)
        );

        deterministicTaskQueue.runAllRunnableTasks();

        assertNotNull(createSnapshotResponseStepListener.result());
        assertNotNull(createAnotherSnapshotResponseStepListener.result());
        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testConcurrentSnapshotCreateAndDeleteOther() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final StepListener<CreateSnapshotResponse> createOtherSnapshotResponseStepListener = new StepListener<>();
        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, "snapshot-2")
                .execute(createOtherSnapshotResponseStepListener)
        );
        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();

        continueOrDie(
            createOtherSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareDeleteSnapshot(repoName, snapshotName)
                .execute(deleteSnapshotStepListener)
        );

        final StepListener<CreateSnapshotResponse> createAnotherSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(deleteSnapshotStepListener, deleted -> {
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createAnotherSnapshotResponseStepListener);
            continueOrDie(
                createAnotherSnapshotResponseStepListener,
                createSnapshotResponse -> assertEquals(createSnapshotResponse.getSnapshotInfo().state(), SnapshotState.SUCCESS)
            );
        });

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        // We end up with two snapshots no matter if the delete worked out or not
        assertThat(snapshotIds, hasSize(2));
        for (SnapshotId snapshotId : snapshotIds) {
            final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
            assertEquals(shards, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
        }
    }

    public void testTransportGetSnapshotsAction() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        final String[] snapshotsList = { "snapshot-1" };
        final String index = "index-1";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();
        final String snapshot = snapshotsList[0];
        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createSnapshotResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshot)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {

            TransportAction getSnapshotsAction = clusterManagerNode.actions.get(GetSnapshotsAction.INSTANCE);
            TransportGetSnapshotsAction transportGetSnapshotsAction = (TransportGetSnapshotsAction) getSnapshotsAction;
            GetSnapshotsRequest repoSnapshotRequest = new GetSnapshotsRequest().repository(repoName).snapshots(snapshotsList);

            transportGetSnapshotsAction.execute(null, repoSnapshotRequest, ActionListener.wrap(repoSnapshotResponse -> {
                assertNotNull("Snapshot list should not be null", repoSnapshotResponse.getSnapshots());
                assertThat(repoSnapshotResponse.getSnapshots(), hasSize(1));
                List<SnapshotInfo> snapshotInfos = repoSnapshotResponse.getSnapshots();
                SnapshotInfo snapshotInfo = snapshotInfos.get(0);
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
                assertEquals(0, snapshotInfo.failedShards());
                assertEquals(snapshotInfo.snapshotId().getName(), snapshotsList[0]);
            }, exception -> { throw new AssertionError(exception); }));
        });
    }

    public void testTransportGetCurrentSnapshotsAction() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        final String index = "index-1";
        final String[] snapshotsList = { GetSnapshotsRequest.CURRENT_SNAPSHOT };
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseListener = new StepListener<>();
        clusterManagerNode.clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().custom(SnapshotsInProgress.TYPE) != null) {
                    TransportAction getSnapshotsAction = clusterManagerNode.actions.get(GetSnapshotsAction.INSTANCE);
                    TransportGetSnapshotsAction transportGetSnapshotsAction = (TransportGetSnapshotsAction) getSnapshotsAction;
                    GetSnapshotsRequest repoSnapshotRequest = new GetSnapshotsRequest().repository(repoName)
                        .snapshots(snapshotsList)
                        .ignoreUnavailable(false)
                        .verbose(false);
                    transportGetSnapshotsAction.execute(null, repoSnapshotRequest, ActionListener.wrap(repoSnapshotResponse -> {
                        assertNotNull("Snapshot list should not be null", repoSnapshotResponse.getSnapshots());
                        List<SnapshotInfo> snapshotInfos = repoSnapshotResponse.getSnapshots();
                        assertThat(repoSnapshotResponse.getSnapshots(), hasSize(snapshotsList.length));
                        for (SnapshotInfo snapshotInfo : snapshotInfos) {
                            assertEquals(SnapshotState.IN_PROGRESS, snapshotInfo.state());
                            assertEquals(0, snapshotInfo.failedShards());
                            assertTrue(snapshotInfo.snapshotId().getName().contains("last-snapshot"));
                        }
                    }, exception -> { throw new AssertionError(exception); }));
                    clusterManagerNode.clusterService.removeListener(this);
                }
            }
        });
        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, GetSnapshotsRequest.CURRENT_SNAPSHOT)
                .execute(createSnapshotResponseListener)
        );
        deterministicTaskQueue.runAllRunnableTasks();
    }

    public void testBulkSnapshotDeleteWithAbort() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final int inProgressSnapshots = randomIntBetween(1, 5);
        final StepListener<Collection<CreateSnapshotResponse>> createOtherSnapshotResponseStepListener = new StepListener<>();
        final ActionListener<CreateSnapshotResponse> createSnapshotListener = new GroupedActionListener<>(
            createOtherSnapshotResponseStepListener,
            inProgressSnapshots
        );

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            for (int i = 0; i < inProgressSnapshots; i++) {
                client().admin().cluster().prepareCreateSnapshot(repoName, "other-" + i).execute(createSnapshotListener);
            }
        });

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();

        continueOrDie(
            createOtherSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .deleteSnapshot(new DeleteSnapshotRequest(repoName, "*"), deleteSnapshotStepListener)
        );

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        // No snapshots should be left in the repository
        assertThat(snapshotIds, empty());
    }

    public void testConcurrentSnapshotRestoreAndDeleteOther() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final String alias = "test_alias";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        final int documentsFirstSnapshot = randomIntBetween(0, 100);
        continueOrDie(
            createRepoAndIndexAndAlias(repoName, index, shards, alias),
            createIndexResponse -> indexNDocuments(
                documentsFirstSnapshot,
                index,
                () -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(repoName, snapshotName)
                    .setWaitForCompletion(true)
                    .execute(createSnapshotResponseStepListener)
            )
        );

        final int documentsSecondSnapshot = randomIntBetween(0, 100);

        final StepListener<CreateSnapshotResponse> createOtherSnapshotResponseStepListener = new StepListener<>();

        final String secondSnapshotName = "snapshot-2";
        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> indexNDocuments(
                documentsSecondSnapshot,
                index,
                () -> client().admin()
                    .cluster()
                    .prepareCreateSnapshot(repoName, secondSnapshotName)
                    .setWaitForCompletion(true)
                    .execute(createOtherSnapshotResponseStepListener)
            )
        );

        final StepListener<AcknowledgedResponse> deleteSnapshotStepListener = new StepListener<>();
        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseListener = new StepListener<>();

        continueOrDie(createOtherSnapshotResponseStepListener, createSnapshotResponse -> {
            scheduleNow(() -> client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute(deleteSnapshotStepListener));
            scheduleNow(
                () -> client().admin()
                    .cluster()
                    .restoreSnapshot(
                        new RestoreSnapshotRequest(repoName, secondSnapshotName).waitForCompletion(true)
                            .includeAliases(true)
                            .renamePattern("(.+)")
                            .renameReplacement("restored_$1")
                            .renameAliasPattern("(.+)")
                            .renameAliasReplacement("restored_alias_$1"),
                        restoreSnapshotResponseListener
                    )
            );
        });

        final StepListener<SearchResponse> searchIndexResponseListener = new StepListener<>();
        final StepListener<SearchResponse> searchAliasResponseListener = new StepListener<>();
        continueOrDie(restoreSnapshotResponseListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            client().search(
                new SearchRequest("restored_" + index).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchIndexResponseListener
            );
            client().search(
                new SearchRequest("restored_alias_" + alias).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchAliasResponseListener
            );
        });

        deterministicTaskQueue.runAllRunnableTasks();

        assertEquals(
            documentsFirstSnapshot + documentsSecondSnapshot,
            Objects.requireNonNull(searchIndexResponseListener.result().getHits().getTotalHits()).value()
        );
        assertEquals(
            documentsFirstSnapshot + documentsSecondSnapshot,
            Objects.requireNonNull(searchAliasResponseListener.result().getHits().getTotalHits()).value()
        );
        assertThat(deleteSnapshotStepListener.result().isAcknowledged(), is(true));
        assertThat(restoreSnapshotResponseListener.result().getRestoreInfo().failedShards(), is(0));

        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, contains(createOtherSnapshotResponseStepListener.result().getSnapshotInfo().snapshotId()));

        for (SnapshotId snapshotId : snapshotIds) {
            final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
            assertEquals(shards, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
        }
    }

    private void indexNDocuments(int documents, String index, Runnable afterIndexing) {
        if (documents == 0) {
            afterIndexing.run();
            return;
        }
        final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < documents; ++i) {
            bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
        }
        final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
        client().bulk(bulkRequest, bulkResponseStepListener);
        continueOrDie(bulkResponseStepListener, bulkResponse -> {
            assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
            assertEquals(documents, bulkResponse.getItems().length);
            afterIndexing.run();
        });
    }

    public void testConcurrentSnapshotDeleteAndDeleteIndex() throws IOException {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<Collection<CreateIndexResponse>> createIndicesListener = new StepListener<>();
        final int indices = randomIntBetween(5, 20);

        final SetOnce<Index> firstIndex = new SetOnce<>();
        continueOrDie(createRepoAndIndex(repoName, index, 1), createIndexResponse -> {
            firstIndex.set(clusterManagerNode.clusterService.state().metadata().index(index).getIndex());
            // create a few more indices to make it more likely that the subsequent index delete operation happens before snapshot
            // finalization
            final GroupedActionListener<CreateIndexResponse> listener = new GroupedActionListener<>(createIndicesListener, indices);
            for (int i = 0; i < indices; ++i) {
                client().admin().indices().create(new CreateIndexRequest("index-" + i), listener);
            }
        });

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        final boolean partialSnapshot = randomBoolean();

        continueOrDie(
            createIndicesListener,
            createIndexResponses -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(false)
                .setPartial(partialSnapshot)
                .setIncludeGlobalState(randomBoolean())
                .execute(createSnapshotResponseStepListener)
        );

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .indices()
                .delete(new DeleteIndexRequest(index), new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        if (partialSnapshot) {
                            // Recreate index by the same name to test that we don't snapshot conflicting metadata in this scenario
                            client().admin().indices().create(new CreateIndexRequest(index), noopListener());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (partialSnapshot) {
                            throw new AssertionError("Delete index should always work during partial snapshots", e);
                        }
                    }
                })
        );

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        if (partialSnapshot) {
            assertThat(snapshotInfo.state(), either(is(SnapshotState.SUCCESS)).or(is(SnapshotState.PARTIAL)));
            // Single shard for each index so we either get all indices or all except for the deleted index
            assertThat(snapshotInfo.successfulShards(), either(is(indices + 1)).or(is(indices)));
            if (snapshotInfo.successfulShards() == indices + 1) {
                final IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(
                    repositoryData,
                    snapshotInfo.snapshotId(),
                    repositoryData.resolveIndexId(index)
                );
                // Make sure we snapshotted the metadata of this index and not the recreated version
                assertEquals(indexMetadata.getIndex(), firstIndex.get());
            }
        } else {
            assertEquals(snapshotInfo.state(), SnapshotState.SUCCESS);
            // Index delete must be blocked for non-partial snapshots and we get a snapshot for every index
            assertEquals(snapshotInfo.successfulShards(), indices + 1);
        }
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testConcurrentDeletes() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";
        final int shards = randomIntBetween(1, 10);

        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true)
                .execute(createSnapshotResponseStepListener)
        );

        final Collection<StepListener<Boolean>> deleteSnapshotStepListeners = Arrays.asList(new StepListener<>(), new StepListener<>());

        final AtomicInteger successfulDeletes = new AtomicInteger(0);

        continueOrDie(createSnapshotResponseStepListener, createSnapshotResponse -> {
            for (StepListener<Boolean> deleteListener : deleteSnapshotStepListeners) {
                client().admin()
                    .cluster()
                    .prepareDeleteSnapshot(repoName, snapshotName)
                    .execute(ActionListener.wrap(resp -> deleteListener.onResponse(true), e -> {
                        final Throwable unwrapped = ExceptionsHelper.unwrap(
                            e,
                            ConcurrentSnapshotExecutionException.class,
                            SnapshotMissingException.class
                        );
                        assertThat(unwrapped, notNullValue());
                        deleteListener.onResponse(false);
                    }));
            }
        });

        for (StepListener<Boolean> deleteListener : deleteSnapshotStepListeners) {
            continueOrDie(deleteListener, deleted -> {
                if (deleted) {
                    successfulDeletes.incrementAndGet();
                }
            });
        }

        deterministicTaskQueue.runAllRunnableTasks();

        SnapshotDeletionsInProgress deletionsInProgress = clusterManagerNode.clusterService.state()
            .custom(SnapshotDeletionsInProgress.TYPE);
        assertFalse(deletionsInProgress.hasDeletionsInProgress());
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repository);
        Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        // We end up with no snapshots since at least one of the deletes worked out
        assertThat(snapshotIds, empty());
        assertThat(successfulDeletes.get(), either(is(1)).or(is(2)));
        // We did one snapshot and one delete so we went two steps from the empty generation (-1) to 1
        assertThat(repositoryData.getGenId(), is(1L));
    }

    /**
     * Simulates concurrent restarts of data and cluster-manager nodes as well as relocating a primary shard, while starting and subsequently
     * deleting a snapshot.
     */
    public void testSnapshotPrimaryRelocations() {
        final int clusterManagerNodeCount = randomFrom(1, 3, 5);
        setupTestCluster(clusterManagerNodeCount, randomIntBetween(2, 5));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 5);

        final TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );
        final AtomicBoolean createdSnapshot = new AtomicBoolean();
        final AdminClient clusterManagerAdminClient = clusterManagerNode.client.admin();

        final StepListener<ClusterStateResponse> clusterStateResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            createIndexResponse -> client().admin().cluster().state(new ClusterStateRequest(), clusterStateResponseStepListener)
        );

        continueOrDie(clusterStateResponseStepListener, clusterStateResponse -> {
            final ShardRouting shardToRelocate = clusterStateResponse.getState().routingTable().allShards(index).get(0);
            final TestClusterNodes.TestClusterNode currentPrimaryNode = testClusterNodes.nodeById(shardToRelocate.currentNodeId());
            final TestClusterNodes.TestClusterNode otherNode = testClusterNodes.randomDataNodeSafe(currentPrimaryNode.node.getName());
            scheduleNow(() -> testClusterNodes.stopNode(currentPrimaryNode));
            scheduleNow(new Runnable() {
                @Override
                public void run() {
                    final StepListener<ClusterStateResponse> updatedClusterStateResponseStepListener = new StepListener<>();
                    clusterManagerAdminClient.cluster().state(new ClusterStateRequest(), updatedClusterStateResponseStepListener);
                    continueOrDie(updatedClusterStateResponseStepListener, updatedClusterState -> {
                        final ShardRouting shardRouting = updatedClusterState.getState()
                            .routingTable()
                            .shardRoutingTable(shardToRelocate.shardId())
                            .primaryShard();
                        if (shardRouting.unassigned() && shardRouting.unassignedInfo().getReason() == UnassignedInfo.Reason.NODE_LEFT) {
                            if (clusterManagerNodeCount > 1) {
                                scheduleNow(() -> testClusterNodes.stopNode(clusterManagerNode));
                            }
                            testClusterNodes.randomDataNodeSafe().client.admin()
                                .cluster()
                                .prepareCreateSnapshot(repoName, snapshotName)
                                .execute(ActionListener.wrap(() -> {
                                    createdSnapshot.set(true);
                                    testClusterNodes.randomDataNodeSafe().client.admin()
                                        .cluster()
                                        .deleteSnapshot(new DeleteSnapshotRequest(repoName, snapshotName), noopListener());
                                }));
                            scheduleNow(
                                () -> testClusterNodes.randomClusterManagerNodeSafe().client.admin()
                                    .cluster()
                                    .reroute(
                                        new ClusterRerouteRequest().add(
                                            new AllocateEmptyPrimaryAllocationCommand(
                                                index,
                                                shardRouting.shardId().id(),
                                                otherNode.node.getName(),
                                                true
                                            )
                                        ),
                                        noopListener()
                                    )
                            );
                        } else {
                            scheduleSoon(this);
                        }
                    });
                }
            });
        });

        runUntil(() -> testClusterNodes.randomClusterManagerNode().map(clusterManager -> {
            if (createdSnapshot.get() == false) {
                return false;
            }
            return clusterManager.clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty();
        }).orElse(false), TimeUnit.MINUTES.toMillis(1L));

        clearDisruptionsAndAwaitSync();

        assertTrue(createdSnapshot.get());
        assertThat(
            testClusterNodes.randomDataNodeSafe().clusterService.state()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .entries(),
            empty()
        );
        final Repository repository = testClusterNodes.randomClusterManagerNodeSafe().repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, either(hasSize(1)).or(hasSize(0)));
    }

    public void testSuccessfulSnapshotWithConcurrentDynamicMappingUpdates() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        String repoName = "repo";
        String snapshotName = "snapshot";
        final String index = "test";

        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(2, 100);
        TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            final AtomicBoolean initiatedSnapshot = new AtomicBoolean(false);
            for (int i = 0; i < documents; ++i) {
                // Index a few documents with different field names so we trigger a dynamic mapping update for each of them
                client().bulk(
                    new BulkRequest().add(new IndexRequest(index).source(Collections.singletonMap("foo" + i, "bar")))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    assertNoFailureListener(bulkResponse -> {
                        assertFalse("Failures in bulkresponse: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                        if (initiatedSnapshot.compareAndSet(false, true)) {
                            client().admin()
                                .cluster()
                                .prepareCreateSnapshot(repoName, snapshotName)
                                .setWaitForCompletion(true)
                                .execute(createSnapshotResponseStepListener);
                        }
                    })
                );
            }
        });

        final String restoredIndex = "restored";

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseStepListener = new StepListener<>();

        continueOrDie(
            createSnapshotResponseStepListener,
            createSnapshotResponse -> client().admin()
                .cluster()
                .restoreSnapshot(
                    new RestoreSnapshotRequest(repoName, snapshotName).renamePattern(index)
                        .renameReplacement(restoredIndex)
                        .waitForCompletion(true),
                    restoreSnapshotResponseStepListener
                )
        );

        final StepListener<SearchResponse> searchResponseStepListener = new StepListener<>();

        continueOrDie(restoreSnapshotResponseStepListener, restoreSnapshotResponse -> {
            assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            client().search(
                new SearchRequest(restoredIndex).source(new SearchSourceBuilder().size(documents).trackTotalHits(true)),
                searchResponseStepListener
            );
        });

        final AtomicBoolean documentCountVerified = new AtomicBoolean();

        continueOrDie(searchResponseStepListener, r -> {
            final long hitCount = r.getHits().getTotalHits().value();
            assertThat(
                "Documents were restored but the restored index mapping was older than some documents and misses some of their fields",
                (int) hitCount,
                lessThanOrEqualTo(
                    ((Map<?, ?>) clusterManagerNode.clusterService.state()
                        .metadata()
                        .index(restoredIndex)
                        .mapping()
                        .sourceAsMap()
                        .get("properties")).size()
                )
            );
            documentCountVerified.set(true);
        });

        runUntil(documentCountVerified::get, TimeUnit.MINUTES.toMillis(5L));

        assertNotNull(createSnapshotResponseStepListener.result());
        assertNotNull(restoreSnapshotResponseStepListener.result());
        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(1));

        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotIds.iterator().next());
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
        assertEquals(shards, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testRunConcurrentSnapshots() {
        setupTestCluster(randomFrom(1, 3, 5), randomIntBetween(2, 10));

        final String repoName = "repo";
        final List<String> snapshotNames = IntStream.range(1, randomIntBetween(2, 4))
            .mapToObj(i -> "snapshot-" + i)
            .collect(Collectors.toList());
        final String index = "test";
        final int shards = randomIntBetween(1, 10);
        final int documents = randomIntBetween(1, 100);

        final TestClusterNodes.TestClusterNode clusterManagerNode = testClusterNodes.currentClusterManager(
            testClusterNodes.nodes.values().iterator().next().clusterService.state()
        );

        final StepListener<Collection<CreateSnapshotResponse>> allSnapshotsListener = new StepListener<>();
        final ActionListener<CreateSnapshotResponse> snapshotListener = new GroupedActionListener<>(
            allSnapshotsListener,
            snapshotNames.size()
        );
        final AtomicBoolean doneIndexing = new AtomicBoolean(false);
        continueOrDie(createRepoAndIndex(repoName, index, shards), createIndexResponse -> {
            for (String snapshotName : snapshotNames) {
                scheduleNow(
                    () -> client().admin()
                        .cluster()
                        .prepareCreateSnapshot(repoName, snapshotName)
                        .setWaitForCompletion(true)
                        .execute(snapshotListener)
                );
            }
            final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < documents; ++i) {
                bulkRequest.add(new IndexRequest(index).source(Collections.singletonMap("foo", "bar" + i)));
            }
            final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
            client().bulk(bulkRequest, bulkResponseStepListener);
            continueOrDie(bulkResponseStepListener, bulkResponse -> {
                assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                assertEquals(documents, bulkResponse.getItems().length);
                doneIndexing.set(true);
            });
        });

        final AtomicBoolean doneSnapshotting = new AtomicBoolean(false);
        continueOrDie(allSnapshotsListener, createSnapshotResponses -> {
            for (CreateSnapshotResponse createSnapshotResponse : createSnapshotResponses) {
                final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
            }
            doneSnapshotting.set(true);
        });

        runUntil(() -> doneIndexing.get() && doneSnapshotting.get(), TimeUnit.MINUTES.toMillis(5L));
        SnapshotsInProgress finalSnapshotsInProgress = clusterManagerNode.clusterService.state().custom(SnapshotsInProgress.TYPE);
        assertFalse(finalSnapshotsInProgress.entries().stream().anyMatch(entry -> entry.state().completed() == false));
        final Repository repository = clusterManagerNode.repositoriesService.repository(repoName);
        Collection<SnapshotId> snapshotIds = getRepositoryData(repository).getSnapshotIds();
        assertThat(snapshotIds, hasSize(snapshotNames.size()));

        for (SnapshotId snapshotId : snapshotIds) {
            final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertThat(snapshotInfo.indices(), containsInAnyOrder(index));
            assertEquals(shards, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
        }
    }

    private RepositoryData getRepositoryData(Repository repository) {
        final PlainActionFuture<RepositoryData> res = PlainActionFuture.newFuture();
        repository.getRepositoryData(res);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(res.isDone());
        return res.actionGet();
    }

    private StepListener<CreateIndexResponse> createRepoAndIndex(String repoName, String index, int shards) {
        final StepListener<AcknowledgedResponse> createRepositoryListener = new StepListener<>();

        Settings.Builder settings = Settings.builder().put("location", randomAlphaOfLength(10));
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), repoName, FsRepository.TYPE, settings, createRepositoryListener);

        final StepListener<CreateIndexResponse> createIndexResponseStepListener = new StepListener<>();

        continueOrDie(
            createRepositoryListener,
            acknowledgedResponse -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.ALL).settings(defaultIndexSettings(shards)),
                    createIndexResponseStepListener
                )
        );

        return createIndexResponseStepListener;
    }

    private StepListener<AcknowledgedResponse> createRepoAndIndexAndAlias(String repoName, String index, int shards, String alias) {
        final StepListener<AcknowledgedResponse> createAliasListener = new StepListener<>();

        continueOrDie(
            createRepoAndIndex(repoName, index, shards),
            acknowledgedResponse -> client().admin()
                .indices()
                .aliases(
                    new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias)),
                    createAliasListener
                )
        );

        return createAliasListener;
    }

    private void clearDisruptionsAndAwaitSync() {
        testClusterNodes.clearNetworkDisruptions();
        stabilize();
    }

    private void disconnectOrRestartDataNode() {
        if (randomBoolean()) {
            disconnectRandomDataNode();
        } else {
            testClusterNodes.randomDataNode().ifPresent(TestClusterNodes.TestClusterNode::restart);
        }
    }

    private void disconnectOrRestartClusterManagerNode() {
        testClusterNodes.randomClusterManagerNode().ifPresent(clusterManagerNode -> {
            if (randomBoolean()) {
                testClusterNodes.disconnectNode(clusterManagerNode);
            } else {
                clusterManagerNode.restart();
            }
        });
    }

    private void disconnectRandomDataNode() {
        testClusterNodes.randomDataNode().ifPresent(n -> testClusterNodes.disconnectNode(n));
    }

    private void startCluster() {
        final ClusterState initialClusterState = new ClusterState.Builder(ClusterName.DEFAULT).nodes(testClusterNodes.discoveryNodes())
            .build();
        testClusterNodes.nodes.values().forEach(testClusterNode -> testClusterNode.start(initialClusterState));

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        final VotingConfiguration votingConfiguration = new VotingConfiguration(
            testClusterNodes.nodes.values()
                .stream()
                .map(n -> n.node)
                .filter(DiscoveryNode::isClusterManagerNode)
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet())
        );
        testClusterNodes.nodes.values()
            .stream()
            .filter(n -> n.node.isClusterManagerNode())
            .forEach(testClusterNode -> testClusterNode.coordinator.setInitialConfiguration(votingConfiguration));
        // Connect all nodes to each other
        testClusterNodes.nodes.values()
            .forEach(
                node -> testClusterNodes.nodes.values()
                    .forEach(
                        n -> n.transportService.connectToNode(
                            node.node,
                            null,
                            ActionTestUtils.assertNoFailureListener(c -> logger.info("--> Connected [{}] to [{}]", n.node, node.node))
                        )
                    )
            );
        stabilize();
    }

    private void stabilize() {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + AbstractCoordinatorTestCase.DEFAULT_STABILISATION_TIME;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime) {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
        }
        runUntil(() -> {
            final Collection<ClusterState> clusterStates = testClusterNodes.nodes.values()
                .stream()
                .map(node -> node.clusterService.state())
                .collect(Collectors.toList());
            final Set<String> clusterManagerNodeIds = clusterStates.stream()
                .map(clusterState -> clusterState.nodes().getClusterManagerNodeId())
                .collect(Collectors.toSet());
            final Set<Long> terms = clusterStates.stream().map(ClusterState::term).collect(Collectors.toSet());
            final List<Long> versions = clusterStates.stream().map(ClusterState::version).distinct().collect(Collectors.toList());
            return versions.size() == 1
                && clusterManagerNodeIds.size() == 1
                && clusterManagerNodeIds.contains(null) == false
                && terms.size() == 1;
        }, TimeUnit.MINUTES.toMillis(1L));
    }

    private void runUntil(Supplier<Boolean> fulfilled, long timeout) {
        final long start = deterministicTaskQueue.getCurrentTimeMillis();
        while (timeout > deterministicTaskQueue.getCurrentTimeMillis() - start) {
            if (fulfilled.get()) {
                return;
            }
            deterministicTaskQueue.runAllRunnableTasks();
            deterministicTaskQueue.advanceTime();
        }
        fail("Condition wasn't fulfilled.");
    }

    private void setupTestCluster(int clusterManagerNodes, int dataNodes) {
        testClusterNodes = new TestClusterNodes(clusterManagerNodes, dataNodes);
        startCluster();
    }

    private void setupTestCluster(int clusterManagerNodes, int dataNodes, int warmNodes) {
        testClusterNodes = new TestClusterNodes(clusterManagerNodes, dataNodes, warmNodes);
        startCluster();
    }

    private void scheduleSoon(Runnable runnable) {
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + randomLongBetween(0, 100L), runnable);
    }

    private void scheduleNow(Runnable runnable) {
        deterministicTaskQueue.scheduleNow(runnable);
    }

    private static Settings defaultIndexSettings(int shards) {
        // TODO: randomize replica count settings once recovery operations aren't blocking anymore
        return Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shards)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
    }

    private static <T> void continueOrDie(StepListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.whenComplete(onResponse, e -> { throw new AssertionError(e); });
    }

    private static <T> ActionListener<T> noopListener() {
        return ActionListener.wrap(() -> {});
    }

    public NodeClient client() {
        // Select from sorted list of nodes
        final List<TestClusterNodes.TestClusterNode> nodes = testClusterNodes.nodes.values()
            .stream()
            .filter(n -> testClusterNodes.disconnectedNodes.contains(n.node.getName()) == false)
            .sorted(Comparator.comparing(n -> n.node.getName()))
            .collect(Collectors.toList());
        if (nodes.isEmpty()) {
            throw new AssertionError("No nodes available");
        }
        return randomFrom(nodes).client;
    }

    /**
     * Create a {@link Environment} with random path.home and path.repo
     **/
    private Environment createEnvironment(String nodeName) {
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), nodeName)
                .put(PATH_HOME_SETTING.getKey(), tempDir.resolve(nodeName).toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
                .putList(
                    ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey(),
                    ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.get(Settings.EMPTY)
                )
                .put(MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING.getKey(), 1000) // o.w. some tests might block
                .build()
        );
    }

    private static ClusterState stateForNode(ClusterState state, DiscoveryNode node) {
        // Remove and add back local node to update ephemeral id on restarts
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).remove(node.getId()).add(node).localNodeId(node.getId()))
            .build();
    }

    private final class TestClusterNodes {

        // LinkedHashMap so we have deterministic ordering when iterating over the map in tests
        private final Map<String, TestClusterNode> nodes = new LinkedHashMap<>();

        /**
         * Node names that are disconnected from all other nodes.
         */
        private final Set<String> disconnectedNodes = new HashSet<>();

        TestClusterNodes(int clusterManagerNodes, int dataNodes) {
            this(clusterManagerNodes, dataNodes, 0);
        }

        TestClusterNodes(int clusterManagerNodes, int dataNodes, int warmNodes) {
            for (int i = 0; i < clusterManagerNodes; ++i) {
                nodes.computeIfAbsent("node" + i, nodeName -> {
                    try {
                        return newClusterManagerNode(nodeName);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
            for (int i = 0; i < dataNodes; ++i) {
                nodes.computeIfAbsent("data-node" + i, nodeName -> {
                    try {
                        return newDataNode(nodeName);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
            for (int i = 0; i < warmNodes; ++i) {
                nodes.computeIfAbsent("warm-node" + i, nodeName -> {
                    try {
                        return newWarmNode(nodeName);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }
        }

        public TestClusterNode nodeById(final String nodeId) {
            return nodes.values()
                .stream()
                .filter(n -> n.node.getId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Could not find node by id [" + nodeId + ']'));
        }

        private TestClusterNode newClusterManagerNode(String nodeName) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
        }

        private TestClusterNode newDataNode(String nodeName) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.DATA_ROLE);
        }

        private TestClusterNode newWarmNode(String nodeName) throws IOException {
            return newNode(nodeName, DiscoveryNodeRole.WARM_ROLE);
        }

        private TestClusterNode newNode(String nodeName, DiscoveryNodeRole role) throws IOException {
            return new TestClusterNode(
                new DiscoveryNode(
                    nodeName,
                    randomAlphaOfLength(10),
                    buildNewFakeTransportAddress(),
                    emptyMap(),
                    Collections.singleton(role),
                    Version.CURRENT
                )
            );
        }

        public TestClusterNode randomClusterManagerNodeSafe() {
            return randomClusterManagerNode().orElseThrow(
                () -> new AssertionError("Expected to find at least one connected cluster-manager node")
            );
        }

        public Optional<TestClusterNode> randomClusterManagerNode() {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> clusterManagerNodes = testClusterNodes.nodes.values()
                .stream()
                .filter(n -> n.node.isClusterManagerNode())
                .filter(n -> disconnectedNodes.contains(n.node.getName()) == false)
                .sorted(Comparator.comparing(n -> n.node.getName()))
                .collect(Collectors.toList());
            return clusterManagerNodes.isEmpty() ? Optional.empty() : Optional.of(randomFrom(clusterManagerNodes));
        }

        public void stopNode(TestClusterNode node) {
            node.stop();
            nodes.remove(node.node.getName());
        }

        public TestClusterNode randomDataNodeSafe(String... excludedNames) {
            return randomDataNode(excludedNames).orElseThrow(() -> new AssertionError("Could not find another data node."));
        }

        public Optional<TestClusterNode> randomDataNode(String... excludedNames) {
            // Select from sorted list of data-nodes here to not have deterministic behaviour
            final List<TestClusterNode> dataNodes = testClusterNodes.nodes.values().stream().filter(n -> n.node.isDataNode()).filter(n -> {
                for (final String nodeName : excludedNames) {
                    if (n.node.getName().equals(nodeName)) {
                        return false;
                    }
                }
                return true;
            }).sorted(Comparator.comparing(n -> n.node.getName())).collect(Collectors.toList());
            return dataNodes.isEmpty() ? Optional.empty() : Optional.ofNullable(randomFrom(dataNodes));
        }

        public void disconnectNode(TestClusterNode node) {
            if (disconnectedNodes.contains(node.node.getName())) {
                return;
            }
            testClusterNodes.nodes.values().forEach(n -> n.transportService.getConnectionManager().disconnectFromNode(node.node));
            disconnectedNodes.add(node.node.getName());
        }

        public void clearNetworkDisruptions() {
            final Set<String> disconnectedNodes = new HashSet<>(this.disconnectedNodes);
            this.disconnectedNodes.clear();
            disconnectedNodes.forEach(nodeName -> {
                if (testClusterNodes.nodes.containsKey(nodeName)) {
                    final DiscoveryNode node = testClusterNodes.nodes.get(nodeName).node;
                    testClusterNodes.nodes.values()
                        .forEach(
                            n -> n.transportService.openConnection(
                                node,
                                null,
                                ActionTestUtils.assertNoFailureListener(c -> logger.debug("--> Connected [{}] to [{}]", n.node, node))
                            )
                        );
                }
            });
        }

        /**
         * Builds a {@link DiscoveryNodes} instance that holds the nodes in this test cluster.
         * @return DiscoveryNodes
         */
        public DiscoveryNodes discoveryNodes() {
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
            nodes.values().forEach(node -> builder.add(node.node));
            return builder.build();
        }

        /**
         * Returns the {@link TestClusterNode} for the cluster-manager node in the given {@link ClusterState}.
         * @param state ClusterState
         * @return Cluster Manager Node
         */
        public TestClusterNode currentClusterManager(ClusterState state) {
            TestClusterNode clusterManager = nodes.get(state.nodes().getClusterManagerNode().getName());
            assertNotNull(clusterManager);
            assertTrue(clusterManager.node.isClusterManagerNode());
            return clusterManager;
        }

        private final class TestClusterNode {
            private final Logger logger = LogManager.getLogger(TestClusterNode.class);

            private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
                Stream.concat(ClusterModule.getNamedWriteables().stream(), NetworkModule.getNamedWriteables().stream())
                    .collect(Collectors.toList())
            );

            private final TransportService transportService;

            private final ClusterService clusterService;

            private final RecoverySettings recoverySettings;

            private final NodeConnectionsService nodeConnectionsService;

            private final RepositoriesService repositoriesService;

            private final SnapshotsService snapshotsService;

            private final SnapshotShardsService snapshotShardsService;

            private final IndicesService indicesService;

            private final IndicesClusterStateService indicesClusterStateService;

            private final DiscoveryNode node;

            private final ClusterManagerService clusterManagerService;

            private final AllocationService allocationService;

            private final RerouteService rerouteService;

            private final NodeClient client;

            private final NodeEnvironment nodeEnv;

            private final DisruptableMockTransport mockTransport;

            private final ThreadPool threadPool;

            private final ClusterInfoService clusterInfoService;

            private Coordinator coordinator;
            private RemoteStoreNodeService remoteStoreNodeService;

            private final MappingTransformerRegistry mappingTransformerRegistry;

            private Map<ActionType, TransportAction> actions = new HashMap<>();

            TestClusterNode(DiscoveryNode node) throws IOException {
                this.node = node;
                final Environment environment = createEnvironment(node.getName());
                threadPool = deterministicTaskQueue.getThreadPool(runnable -> CoordinatorTests.onNodeLog(node, runnable));
                clusterManagerService = new FakeThreadPoolClusterManagerService(
                    node.getName(),
                    "test",
                    threadPool,
                    deterministicTaskQueue::scheduleNow
                );
                final Settings settings = environment.settings();
                final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                clusterService = new ClusterService(
                    settings,
                    clusterSettings,
                    clusterManagerService,
                    new ClusterApplierService(node.getName(), settings, clusterSettings, threadPool) {
                        @Override
                        protected PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
                            return new MockSinglePrioritizingExecutor(node.getName(), deterministicTaskQueue, threadPool);
                        }
                    }
                );
                recoverySettings = new RecoverySettings(settings, clusterSettings);
                mockTransport = new DisruptableMockTransport(node, logger, deterministicTaskQueue) {
                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                        if (node.equals(destination)) {
                            return ConnectionStatus.CONNECTED;
                        }
                        // Check if both nodes are still part of the cluster
                        if (nodes.containsKey(node.getName()) == false || nodes.containsKey(destination.getName()) == false) {
                            return ConnectionStatus.DISCONNECTED;
                        }
                        return disconnectedNodes.contains(node.getName()) || disconnectedNodes.contains(destination.getName())
                            ? ConnectionStatus.DISCONNECTED
                            : ConnectionStatus.CONNECTED;
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                        return nodes.values()
                            .stream()
                            .map(cn -> cn.mockTransport)
                            .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                            .findAny();
                    }

                    @Override
                    protected void execute(Runnable runnable) {
                        scheduleNow(CoordinatorTests.onNodeLog(getLocalNode(), runnable));
                    }

                    @Override
                    protected NamedWriteableRegistry writeableRegistry() {
                        return namedWriteableRegistry;
                    }
                };
                transportService = mockTransport.createTransportService(settings, threadPool, new TransportInterceptor() {
                    @Override
                    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                        String action,
                        String executor,
                        boolean forceExecution,
                        TransportRequestHandler<T> actualHandler
                    ) {
                        // TODO: Remove this hack once recoveries are async and can be used in these tests
                        if (action.startsWith("internal:index/shard/recovery")) {
                            return (request, channel, task) -> scheduleSoon(new AbstractRunnable() {
                                @Override
                                protected void doRun() throws Exception {
                                    channel.sendResponse(new TransportException(new IOException("failed to recover shard")));
                                }

                                @Override
                                public void onFailure(final Exception e) {
                                    throw new AssertionError(e);
                                }
                            });
                        } else {
                            return actualHandler;
                        }
                    }
                }, a -> node, null, emptySet(), NoopTracer.INSTANCE);
                final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(
                    new ThreadContext(Settings.EMPTY)
                );
                transportService.getTaskManager()
                    .setTaskResourceTrackingService(new TaskResourceTrackingService(settings, clusterSettings, threadPool));
                repositoriesService = new RepositoriesService(
                    settings,
                    clusterService,
                    transportService,
                    Collections.singletonMap(FsRepository.TYPE, getRepoFactory(environment)),
                    emptyMap(),
                    threadPool
                );
                remoteStoreNodeService = new RemoteStoreNodeService(new SetOnce<>(repositoriesService)::get, threadPool);
                final ActionFilters actionFilters = new ActionFilters(emptySet());
                snapshotsService = new SnapshotsService(
                    settings,
                    clusterService,
                    indexNameExpressionResolver,
                    repositoriesService,
                    transportService,
                    actionFilters,
                    null,
                    DefaultRemoteStoreSettings.INSTANCE
                );
                nodeEnv = new NodeEnvironment(settings, environment);
                final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());
                final ScriptService scriptService = new ScriptService(settings, emptyMap(), emptyMap());
                client = new NodeClient(settings, threadPool);
                clusterInfoService = Mockito.mock(ClusterInfoService.class);
                final SetOnce<RerouteService> rerouteServiceSetOnce = new SetOnce<>();
                final SnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
                    settings,
                    clusterService,
                    () -> repositoriesService,
                    rerouteServiceSetOnce::get
                );
                allocationService = OpenSearchAllocationTestCase.createAllocationService(settings, snapshotsInfoService);
                rerouteService = new BatchedRerouteService(clusterService, allocationService::reroute);
                rerouteServiceSetOnce.set(rerouteService);
                final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(
                    settings,
                    IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
                );
                final BigArrays bigArrays = new BigArrays(new PageCacheRecycler(settings), null, "test");
                final MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
                final SetOnce<RepositoriesService> repositoriesServiceReference = new SetOnce<>();
                repositoriesServiceReference.set(repositoriesService);
                indicesService = new IndicesService(
                    settings,
                    mock(PluginsService.class),
                    nodeEnv,
                    namedXContentRegistry,
                    new AnalysisRegistry(
                        environment,
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap(),
                        emptyMap()
                    ),
                    indexNameExpressionResolver,
                    mapperRegistry,
                    namedWriteableRegistry,
                    threadPool,
                    indexScopedSettings,
                    new NoneCircuitBreakerService(),
                    bigArrays,
                    scriptService,
                    clusterService,
                    client,
                    new MetaStateService(nodeEnv, namedXContentRegistry),
                    Collections.emptyList(),
                    emptyMap(),
                    null,
                    emptyMap(),
                    new RemoteSegmentStoreDirectoryFactory(() -> repositoriesService, threadPool, ""),
                    repositoriesServiceReference::get,
                    null,
                    new RemoteStoreStatsTrackerFactory(clusterService, settings),
                    emptyMap(),
                    DefaultRecoverySettings.INSTANCE,
                    new CacheModule(new ArrayList<>(), settings).getCacheService(),
                    DefaultRemoteStoreSettings.INSTANCE
                );
                final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
                snapshotShardsService = new SnapshotShardsService(
                    settings,
                    clusterService,
                    repositoriesService,
                    transportService,
                    indicesService
                );
                final ShardStateAction shardStateAction = new ShardStateAction(
                    clusterService,
                    transportService,
                    allocationService,
                    rerouteService,
                    threadPool
                );
                nodeConnectionsService = createTestNodeConnectionsService(clusterService.getSettings(), threadPool, transportService);
                final MetadataMappingService metadataMappingService = new MetadataMappingService(clusterService, indicesService);
                indicesClusterStateService = new IndicesClusterStateService(
                    settings,
                    indicesService,
                    clusterService,
                    threadPool,
                    new PeerRecoveryTargetService(threadPool, transportService, recoverySettings, clusterService),
                    new SegmentReplicationTargetService(
                        threadPool,
                        recoverySettings,
                        transportService,
                        new SegmentReplicationSourceFactory(transportService, recoverySettings, clusterService),
                        indicesService,
                        clusterService
                    ),
                    mock(SegmentReplicationSourceService.class),
                    shardStateAction,
                    new NodeMappingRefreshAction(transportService, metadataMappingService),
                    repositoriesService,
                    mock(SearchService.class),
                    new PeerRecoverySourceService(transportService, indicesService, recoverySettings),
                    snapshotShardsService,
                    new PrimaryReplicaSyncer(
                        transportService,
                        new TransportResyncReplicationAction(
                            settings,
                            transportService,
                            clusterService,
                            indicesService,
                            threadPool,
                            shardStateAction,
                            actionFilters,
                            new IndexingPressureService(settings, clusterService),
                            new SystemIndices(emptyMap()),
                            NoopTracer.INSTANCE
                        )
                    ),
                    new GlobalCheckpointSyncAction(
                        settings,
                        transportService,
                        clusterService,
                        indicesService,
                        threadPool,
                        shardStateAction,
                        actionFilters
                    ),
                    RetentionLeaseSyncer.EMPTY,
                    SegmentReplicationCheckpointPublisher.EMPTY,
                    mock(RemoteStoreStatsTrackerFactory.class),
                    new MergedSegmentWarmerFactory(null, null, null),
                    MergedSegmentPublisher.EMPTY,
                    ReferencedSegmentsPublisher.EMPTY
                );

                final SystemIndices systemIndices = new SystemIndices(emptyMap());
                final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService, systemIndices);
                final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                    settings,
                    clusterService,
                    indicesService,
                    allocationService,
                    new AliasValidator(),
                    shardLimitValidator,
                    environment,
                    indexScopedSettings,
                    threadPool,
                    namedXContentRegistry,
                    systemIndices,
                    false,
                    new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                    DefaultRemoteStoreSettings.INSTANCE,
                    null
                );

                mappingTransformerRegistry = new MappingTransformerRegistry(List.of(), namedXContentRegistry);

                actions.put(
                    CreateIndexAction.INSTANCE,
                    new TransportCreateIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataCreateIndexService,
                        actionFilters,
                        indexNameExpressionResolver,
                        mappingTransformerRegistry
                    )
                );
                final MetadataDeleteIndexService metadataDeleteIndexService = new MetadataDeleteIndexService(
                    settings,
                    clusterService,
                    allocationService
                );
                final MetadataIndexAliasesService metadataIndexAliasesService = new MetadataIndexAliasesService(
                    clusterService,
                    indicesService,
                    new AliasValidator(),
                    metadataDeleteIndexService,
                    namedXContentRegistry
                );
                actions.put(
                    IndicesAliasesAction.INSTANCE,
                    new TransportIndicesAliasesAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataIndexAliasesService,
                        actionFilters,
                        indexNameExpressionResolver,
                        new RequestValidators<>(Collections.emptyList())
                    )
                );
                final MappingUpdatedAction mappingUpdatedAction = new MappingUpdatedAction(settings, clusterSettings, clusterService);
                mappingUpdatedAction.setClient(client);
                final TransportShardBulkAction transportShardBulkAction = new TransportShardBulkAction(
                    settings,
                    transportService,
                    clusterService,
                    indicesService,
                    threadPool,
                    shardStateAction,
                    mappingUpdatedAction,
                    new UpdateHelper(scriptService),
                    actionFilters,
                    new IndexingPressureService(settings, clusterService),
                    new SegmentReplicationPressureService(
                        settings,
                        clusterService,
                        mock(IndicesService.class),
                        mock(ShardStateAction.class),
                        mock(SegmentReplicationStatsTracker.class),
                        mock(ThreadPool.class)
                    ),
                    mock(RemoteStorePressureService.class),
                    new SystemIndices(emptyMap()),
                    NoopTracer.INSTANCE
                );
                actions.put(
                    BulkAction.INSTANCE,
                    new TransportBulkAction(
                        threadPool,
                        transportService,
                        clusterService,
                        new IngestService(
                            clusterService,
                            threadPool,
                            environment,
                            scriptService,
                            new AnalysisModule(environment, Collections.emptyList()).getAnalysisRegistry(),
                            Collections.emptyList(),
                            client,
                            indicesService,
                            namedXContentRegistry,
                            new SystemIngestPipelineCache()
                        ),
                        transportShardBulkAction,
                        client,
                        actionFilters,
                        indexNameExpressionResolver,
                        new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, new SystemIndices(emptyMap())),
                        new IndexingPressureService(settings, clusterService),
                        mock(IndicesService.class),
                        new SystemIndices(emptyMap()),
                        NoopTracer.INSTANCE
                    )
                );
                final RestoreService restoreService = new RestoreService(
                    clusterService,
                    repositoriesService,
                    allocationService,
                    metadataCreateIndexService,
                    new MetadataIndexUpgradeService(
                        settings,
                        namedXContentRegistry,
                        mapperRegistry,
                        indexScopedSettings,
                        new SystemIndices(emptyMap()),
                        null
                    ),
                    shardLimitValidator,
                    indicesService,
                    clusterInfoService::getClusterInfo,
                    () -> 5.0
                );
                actions.put(
                    PutMappingAction.INSTANCE,
                    new TransportPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        indexNameExpressionResolver,
                        new RequestValidators<>(Collections.emptyList()),
                        mappingTransformerRegistry
                    )
                );
                actions.put(
                    AutoPutMappingAction.INSTANCE,
                    new TransportAutoPutMappingAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataMappingService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
                final SearchTransportService searchTransportService = new SearchTransportService(
                    transportService,
                    SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
                );
                final SearchService searchService = new SearchService(
                    clusterService,
                    indicesService,
                    threadPool,
                    scriptService,
                    bigArrays,
                    new QueryPhase(),
                    new FetchPhase(Collections.emptyList()),
                    responseCollectorService,
                    new NoneCircuitBreakerService(),
                    null,
                    new TaskResourceTrackingService(settings, clusterSettings, threadPool),
                    Collections.emptyList(),
                    Collections.emptyList()
                );
                SearchPhaseController searchPhaseController = new SearchPhaseController(
                    writableRegistry(),
                    searchService::aggReduceContextBuilder
                );
                SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory =
                    new SearchRequestOperationsCompositeListenerFactory();
                actions.put(
                    SearchAction.INSTANCE,
                    new TransportSearchAction(
                        client,
                        threadPool,
                        new NoneCircuitBreakerService(),
                        transportService,
                        searchService,
                        searchTransportService,
                        searchPhaseController,
                        clusterService,
                        actionFilters,
                        indexNameExpressionResolver,
                        namedWriteableRegistry,
                        new SearchPipelineService(
                            clusterService,
                            threadPool,
                            environment,
                            scriptService,
                            new AnalysisModule(environment, Collections.emptyList()).getAnalysisRegistry(),
                            namedXContentRegistry,
                            namedWriteableRegistry,
                            List.of(),
                            client
                        ),
                        NoopMetricsRegistry.INSTANCE,
                        searchRequestOperationsCompositeListenerFactory,
                        NoopTracer.INSTANCE,
                        new TaskResourceTrackingService(settings, clusterSettings, threadPool)
                    )
                );
                actions.put(
                    RestoreSnapshotAction.INSTANCE,
                    new TransportRestoreSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        restoreService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    DeleteIndexAction.INSTANCE,
                    new TransportDeleteIndexAction(
                        transportService,
                        clusterService,
                        threadPool,
                        metadataDeleteIndexService,
                        actionFilters,
                        indexNameExpressionResolver,
                        new DestructiveOperations(settings, clusterSettings)
                    )
                );
                actions.put(
                    PutRepositoryAction.INSTANCE,
                    new TransportPutRepositoryAction(
                        transportService,
                        clusterService,
                        repositoriesService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    CleanupRepositoryAction.INSTANCE,
                    new TransportCleanupRepositoryAction(
                        transportService,
                        clusterService,
                        repositoriesService,
                        snapshotsService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        DefaultRemoteStoreSettings.INSTANCE
                    )
                );
                actions.put(
                    CreateSnapshotAction.INSTANCE,
                    new TransportCreateSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        repositoriesService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    ClusterRerouteAction.INSTANCE,
                    new TransportClusterRerouteAction(
                        transportService,
                        clusterService,
                        threadPool,
                        allocationService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    GetSnapshotsAction.INSTANCE,
                    new TransportGetSnapshotsAction(
                        transportService,
                        clusterService,
                        threadPool,
                        repositoriesService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );
                actions.put(
                    ClusterStateAction.INSTANCE,
                    new TransportClusterStateAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        null
                    )
                );
                actions.put(
                    IndicesShardStoresAction.INSTANCE,
                    new TransportIndicesShardStoresAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        new TransportNodesListGatewayStartedShards(
                            settings,
                            threadPool,
                            clusterService,
                            transportService,
                            actionFilters,
                            nodeEnv,
                            indicesService,
                            namedXContentRegistry
                        ),
                        new ClusterManagerMetrics(NoopMetricsRegistry.INSTANCE)
                    )
                );
                actions.put(
                    DeleteSnapshotAction.INSTANCE,
                    new TransportDeleteSnapshotAction(
                        transportService,
                        clusterService,
                        threadPool,
                        snapshotsService,
                        actionFilters,
                        indexNameExpressionResolver
                    )
                );

                actions.put(
                    GetTermVersionAction.INSTANCE,
                    new TransportGetTermVersionAction(
                        transportService,
                        clusterService,
                        threadPool,
                        actionFilters,
                        indexNameExpressionResolver,
                        null
                    )
                );

                DynamicActionRegistry dynamicActionRegistry = new DynamicActionRegistry();
                dynamicActionRegistry.registerUnmodifiableActionMap(actions);
                client.initialize(
                    dynamicActionRegistry,
                    () -> clusterService.localNode().getId(),
                    transportService.getRemoteClusterService(),
                    new NamedWriteableRegistry(Collections.emptyList())
                );
            }

            private Repository.Factory getRepoFactory(Environment environment) {
                // Run half the tests with the eventually consistent repository
                if (blobStoreContext == null) {
                    return metadata -> new FsRepository(metadata, environment, xContentRegistry(), clusterService, recoverySettings) {
                        @Override
                        protected void assertSnapshotOrGenericThread() {
                            // eliminate thread name check as we create repo in the test thread
                        }
                    };
                } else {
                    return metadata -> new MockEventuallyConsistentRepository(
                        metadata,
                        xContentRegistry(),
                        clusterService,
                        recoverySettings,
                        blobStoreContext,
                        random()
                    );
                }
            }

            public NodeConnectionsService createTestNodeConnectionsService(
                Settings settings,
                ThreadPool threadPool,
                TransportService transportService
            ) {
                return new NodeConnectionsService(settings, threadPool, transportService) {
                    @Override
                    public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                        // just update targetsByNode to ensure disconnect runs for these nodes
                        // we rely on disconnect to run for keeping track of pendingDisconnects and ensuring node-joins can happen
                        for (final DiscoveryNode discoveryNode : discoveryNodes) {
                            this.targetsByNode.put(discoveryNode, createConnectionTarget(discoveryNode));
                        }
                        onCompletion.run();
                    }
                };
            }

            public ClusterInfoService getMockClusterInfoService() {
                return clusterInfoService;
            }

            public void restart() {
                testClusterNodes.disconnectNode(this);
                final ClusterState oldState = this.clusterService.state();
                stop();
                nodes.remove(node.getName());
                scheduleSoon(() -> {
                    try {
                        final TestClusterNode restartedNode = new TestClusterNode(
                            new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), emptyMap(), node.getRoles(), Version.CURRENT)
                        );
                        nodes.put(node.getName(), restartedNode);
                        restartedNode.start(oldState);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }

            public void stop() {
                testClusterNodes.disconnectNode(this);
                indicesService.close();
                clusterService.close();
                nodeConnectionsService.stop();
                indicesClusterStateService.close();
                if (coordinator != null) {
                    coordinator.close();
                }
                nodeEnv.close();
            }

            public void start(ClusterState initialState) {
                transportService.start();
                transportService.acceptIncomingRequests();
                snapshotsService.start();
                snapshotShardsService.start();
                repositoriesService.start();
                final CoordinationState.PersistedState persistedState = new InMemoryPersistedState(
                    initialState.term(),
                    stateForNode(initialState, node)
                );
                final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
                persistedStateRegistry.addPersistedState(PersistedStateType.LOCAL, persistedState);
                coordinator = new Coordinator(
                    node.getName(),
                    clusterService.getSettings(),
                    clusterService.getClusterSettings(),
                    transportService,
                    namedWriteableRegistry,
                    allocationService,
                    clusterManagerService,
                    () -> persistedState,
                    hostsResolver -> nodes.values()
                        .stream()
                        .filter(n -> n.node.isClusterManagerNode())
                        .map(n -> n.node.getAddress())
                        .collect(Collectors.toList()),
                    clusterService.getClusterApplierService(),
                    Collections.emptyList(),
                    random(),
                    rerouteService,
                    ElectionStrategy.DEFAULT_INSTANCE,
                    () -> new StatusInfo(HEALTHY, "healthy-info"),
                    persistedStateRegistry,
                    remoteStoreNodeService,
                    new ClusterManagerMetrics(NoopMetricsRegistry.INSTANCE),
                    null
                );
                coordinator.setNodeConnectionsService(nodeConnectionsService);
                clusterManagerService.setClusterStatePublisher(coordinator);
                clusterService.getClusterApplierService().setNodeConnectionsService(nodeConnectionsService);
                nodeConnectionsService.start();
                coordinator.start();
                clusterService.start();
                indicesService.start();
                indicesClusterStateService.start();
                coordinator.startInitialJoin();
            }
        }
    }
}
