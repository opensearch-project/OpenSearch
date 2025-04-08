/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_SEARCH_ONLY_BLOCK_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportScaleIndexActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private AllocationService allocationService;
    private IndicesService indicesService;
    private ThreadPool threadPool;
    private TransportScaleIndexAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("ScaleIndexActionTests");
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        allocationService = mock(AllocationService.class);
        indicesService = mock(IndicesService.class);

        action = new TransportScaleIndexAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            allocationService,
            indicesService
        );

        // Setup basic cluster state
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("test"));
        stateBuilder.nodes(DiscoveryNodes.builder().build());
        when(clusterService.state()).thenReturn(stateBuilder.build());
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testScaleDownValidation() {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, true);

        // Test validation when index doesn't exist
        ClusterState state = createClusterStateWithoutIndex(indexName);
        when(clusterService.state()).thenReturn(state);

        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                fail("Expected validation to fail");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalArgumentException);
                assertEquals("Index [" + indexName + "] not found", e.getMessage());
            }
        };

        action.clusterManagerOperation(request, state, listener);
    }

    public void testScaleDownWithSearchOnlyAlreadyEnabled() {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, true);

        // Create cluster state with search-only already enabled
        ClusterState state = createClusterStateWithSearchOnlyEnabled(indexName);
        when(clusterService.state()).thenReturn(state);

        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                fail("Expected validation to fail");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertEquals("Index [" + indexName + "] is already in search-only mode", e.getMessage());
            }
        };

        action.clusterManagerOperation(request, state, listener);
    }

    public void testScaleUpValidation() {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, false);

        // Test validation when index is not in search-only mode
        ClusterState state = createClusterStateWithoutSearchOnly(indexName);
        when(clusterService.state()).thenReturn(state);

        ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                fail("Expected validation to fail");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertEquals("Index [" + indexName + "] is not in search-only mode", e.getMessage());
            }
        };

        action.clusterManagerOperation(request, state, listener);
    }

    private ClusterState createClusterStateWithoutIndex(String indexName) {
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
    }

    private ClusterState createClusterStateWithSearchOnlyEnabled(String indexName) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, true).build()).build();
    }

    private ClusterState createClusterStateWithoutSearchOnly(String indexName) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, true).build()).build();
    }

    public void testAddBlockClusterStateUpdateTask() {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, true);

        // Create initial cluster state with necessary index metadata
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_READ_REPLICAS, 1)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState initialState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        when(clusterService.state()).thenReturn(initialState);

        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                assertTrue("Expected block to be added successfully", response.isAcknowledged());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };

        action.clusterManagerOperation(request, initialState, listener);

        // Verify that the appropriate block was added
        verify(clusterService).submitStateUpdateTask(eq("add-block-index-to-scale " + indexName), any());
    }

    public void testFinalizeScaleDownTaskSimple() throws Exception {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, true);

        // Create minimal index metadata that meets scale-down prerequisites.
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_READ_REPLICAS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        // Build a minimal routing table for the index.
        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);
        ShardRouting primaryShardRouting = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);
        IndexShardRoutingTable shardRoutingTable = new IndexShardRoutingTable.Builder(shardId).addShard(primaryShardRouting).build();
        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).addIndexShard(shardRoutingTable).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        // Create a DiscoveryNode and include it in the cluster state's nodes.
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node).localNodeId("node1").build();

        // Build the complete cluster state with metadata, routing table, and nodes.
        ClusterState initialState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();
        when(clusterService.state()).thenReturn(initialState);

        // Stub transportService.sendRequest so that any shard sync request immediately succeeds.
        doAnswer(invocation -> {
            TransportResponseHandler<ScaleIndexNodeResponse> handler = invocation.getArgument(3);
            handler.handleResponse(new ScaleIndexNodeResponse(node, Collections.emptyList()));
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(TransportScaleIndexAction.NAME),
                any(ScaleIndexNodeRequest.class),
                any(TransportResponseHandler.class)
            );

        // Execute the scale-down operation.
        action.clusterManagerOperation(request, initialState, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {}

            @Override
            public void onFailure(Exception e) {
                fail("Operation should not fail: " + e.getMessage());
            }
        });

        // Capture the add-block task submitted by the action.
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(eq("add-block-index-to-scale " + indexName), captor.capture());
        ClusterStateUpdateTask addBlockTask = captor.getValue();

        // Create a new cluster state that is different from initialState.
        // For example, add a dummy block to the index.
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(initialState.blocks());
        blocksBuilder.addIndexBlock(
            indexName,
            new ClusterBlock(123, "dummy", false, false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE))
        );
        ClusterState newState = ClusterState.builder(initialState).blocks(blocksBuilder).build();

        // Simulate the add-block task callback (with a changed state) to trigger finalize.
        addBlockTask.clusterStateProcessed("test-source", initialState, newState);

        // Verify that the finalize-scale-down update task was submitted.
        verify(clusterService).submitStateUpdateTask(eq("finalize-scale-down"), any(ClusterStateUpdateTask.class));
    }

    public void testScaleUpClusterStateUpdateTask() throws Exception {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, false);

        // Create index metadata with search-only mode enabled.
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        // Build a minimal routing table for the index.
        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);
        // Create a dummy shard routing in STARTED state.
        ShardRouting searchOnlyShardRouting = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);
        IndexShardRoutingTable shardRoutingTable = new IndexShardRoutingTable.Builder(shardId).addShard(searchOnlyShardRouting).build();
        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).addIndexShard(shardRoutingTable).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        // Build the complete cluster state with metadata and the routing table.
        ClusterState initialState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(routingTable)
            .build();

        // Stub allocationService.reroute to return a valid state.
        ClusterState stateAfterReroute = ClusterState.builder(initialState).build();
        when(allocationService.reroute(any(ClusterState.class), anyString())).thenReturn(stateAfterReroute);
        when(clusterService.state()).thenReturn(initialState);

        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                assertTrue("Expected scale up to complete successfully", response.isAcknowledged());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };

        // Trigger the scale-up operation.
        action.clusterManagerOperation(request, initialState, listener);

        // Capture the update task submitted for scaling up.
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(eq("scale-up-index"), captor.capture());
        ClusterStateUpdateTask scaleUpTask = captor.getValue();

        // Manually simulate execution of the scale-up task.
        ClusterState updatedState = scaleUpTask.execute(initialState);
        scaleUpTask.clusterStateProcessed("test-source", initialState, updatedState);

        // Verify that allocationService.reroute was called with the expected reason.
        verify(allocationService).reroute(any(ClusterState.class), eq("restore indexing shards"));
    }

    public void testScaleDownWithMissingIndex() {
        String indexName = "non_existent_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, true);

        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();

        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                fail("Should fail for missing index");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalArgumentException);
                assertEquals("Index [" + indexName + "] not found", e.getMessage());
            }
        };

        action.clusterManagerOperation(request, state, listener);
    }

    public void testScaleUpWithSearchOnlyNotEnabled() {
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, false);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                fail("Should fail when search-only is not enabled");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertEquals("Index [" + indexName + "] is not in search-only mode", e.getMessage());
            }
        };
        action.clusterManagerOperation(request, state, listener);
    }

    public void testHandleShardSyncRequest() throws Exception {
        // Mock dependencies
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        IndicesService indicesService = mock(IndicesService.class);
        TransportChannel channel = mock(TransportChannel.class);
        DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

        // Use a real ThreadPool but with a controlled executor
        ThreadPool threadPool = new TestThreadPool("testHandleShardSyncRequest");

        try {
            // Create test data
            String indexName = "test_index";
            Index index = new Index(indexName, "_na_");
            ShardId shardId = new ShardId(index, 0);
            List<ShardId> shardIds = Collections.singletonList(shardId);
            ScaleIndexNodeRequest request = new ScaleIndexNodeRequest(indexName, shardIds);

            // Mock cluster state
            ClusterState clusterState = mock(ClusterState.class);
            Metadata metadata = mock(Metadata.class);
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(clusterService.state()).thenReturn(clusterState);
            when(clusterState.metadata()).thenReturn(metadata);
            when(metadata.index(indexName)).thenReturn(indexMetadata);
            when(indexMetadata.getIndex()).thenReturn(index);
            when(clusterService.localNode()).thenReturn(localNode);

            // Mock index service and shard
            IndexService indexService = mock(IndexService.class);
            IndexShard indexShard = mock(IndexShard.class);
            TranslogStats translogStats = mock(TranslogStats.class);

            when(indicesService.indexService(any(Index.class))).thenReturn(indexService);
            when(indexService.getShardOrNull(anyInt())).thenReturn(indexShard);
            when(indexShard.shardId()).thenReturn(shardId);
            when(indexShard.translogStats()).thenReturn(translogStats);
            when(translogStats.getUncommittedOperations()).thenReturn(0);
            when(indexShard.isSyncNeeded()).thenReturn(false);

            // Mock shard routing to return a primary routing
            ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.primary()).thenReturn(true);
            when(indexShard.routingEntry()).thenReturn(shardRouting);

            // Mock the acquireAllPrimaryOperationsPermits method to immediately call the listener
            doAnswer(invocation -> {
                ActionListener<Releasable> listener = invocation.getArgument(0);
                Releasable releasable = mock(Releasable.class);
                listener.onResponse(releasable);
                return null;
            }).when(indexShard).acquireAllPrimaryOperationsPermits(any(ActionListener.class), any(TimeValue.class));

            // Create action instance with the real ThreadPool
            TransportScaleIndexAction action = new TransportScaleIndexAction(
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(Collections.emptySet()),
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
                allocationService,
                indicesService
            );

            // Call handleShardSyncRequest
            action.handleShardSyncRequest(request, channel);

            // Wait a short time for the async task to execute
            assertBusy(() -> { verify(channel).sendResponse(any(ScaleIndexNodeResponse.class)); }, 5, TimeUnit.SECONDS);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testSyncSingleShard() throws Exception {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test_index", "_na_"), 0);
        TranslogStats translogStats = mock(TranslogStats.class);

        when(shard.shardId()).thenReturn(shardId);
        when(shard.translogStats()).thenReturn(translogStats);

        TransportScaleIndexAction action = new TransportScaleIndexAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            allocationService,
            indicesService
        );

        when(translogStats.getUncommittedOperations()).thenReturn(0);
        when(shard.isSyncNeeded()).thenReturn(false);

        final AtomicReference<ScaleIndexShardResponse> successResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> successExceptionRef = new AtomicReference<>();
        final CountDownLatch successLatch = new CountDownLatch(1);

        action.syncSingleShard(shard, new ActionListener<>() {
            @Override
            public void onResponse(ScaleIndexShardResponse response) {
                successResponseRef.set(response);
                successLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                successExceptionRef.set(e);
                successLatch.countDown();
            }
        });

        ArgumentCaptor<ActionListener> successPermitCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(shard).acquireAllPrimaryOperationsPermits(successPermitCaptor.capture(), any(TimeValue.class));
        successPermitCaptor.getValue().onResponse(mock(Releasable.class));

        assertTrue(successLatch.await(1, TimeUnit.SECONDS));

        assertNull("No exception expected", successExceptionRef.get());
        assertNotNull("Response should not be null", successResponseRef.get());
        assertFalse("Response should not indicate sync needed", successResponseRef.get().needsSync());
        assertFalse("Response should not indicate uncommitted operations", successResponseRef.get().hasUncommittedOperations());

        verify(shard, times(1)).sync();
        verify(shard, times(1)).flush(any(FlushRequest.class));
        verify(shard, times(1)).waitForRemoteStoreSync();

        clearInvocations(shard);

        when(translogStats.getUncommittedOperations()).thenReturn(5);
        when(shard.isSyncNeeded()).thenReturn(true);

        final AtomicReference<ScaleIndexShardResponse> responseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        action.syncSingleShard(shard, new ActionListener<>() {
            @Override
            public void onResponse(ScaleIndexShardResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        ArgumentCaptor<ActionListener> permitListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(shard).acquireAllPrimaryOperationsPermits(permitListenerCaptor.capture(), any(TimeValue.class));
        permitListenerCaptor.getValue().onResponse(mock(Releasable.class));

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        assertNull("No exception expected", exceptionRef.get());
        assertNotNull("Response should not be null", responseRef.get());
        assertTrue("Response should indicate uncommitted operations", responseRef.get().hasUncommittedOperations());
        assertTrue("Response should indicate sync needed", responseRef.get().needsSync());

        verify(shard, times(1)).sync();
        verify(shard, times(1)).flush(any(FlushRequest.class));
        verify(shard, times(1)).waitForRemoteStoreSync();
    }

    public void testCheckBlock() {
        // Mock dependencies
        TransportScaleIndexAction action = new TransportScaleIndexAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            allocationService,
            indicesService
        );

        // Create test data
        String indexName = "test_index";
        ScaleIndexRequest request = new ScaleIndexRequest(indexName, true);

        // Create index metadata
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        // Test with no blocks
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(ClusterBlocks.builder().build())
            .build();
        assertNull(action.checkBlock(request, state));

        // Test with metadata write block
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder();
        ClusterBlock metadataBlock = new ClusterBlock(
            1,
            "test block",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );
        blocksBuilder.addGlobalBlock(metadataBlock);
        state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(blocksBuilder.build())
            .build();

        ClusterBlockException exception = action.checkBlock(request, state);
        assertNotNull(exception);
        assertTrue(exception.blocks().contains(metadataBlock));
    }

    public void testAddBlockClusterStateUpdateTaskExecute() {
        // Mock dependencies
        String indexName = "test_index";
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

        // Create initial cluster state
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_READ_REPLICAS, 1)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState initialState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        // Create action and task
        TransportScaleIndexAction action = new TransportScaleIndexAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            allocationService,
            indicesService
        );

        TransportScaleIndexAction.AddBlockClusterStateUpdateTask task = action.new AddBlockClusterStateUpdateTask(
            indexName, blockedIndices, listener
        );

        // Test successful execution
        ClusterState newState = task.execute(initialState);
        assertNotEquals(initialState, newState);

        // Verify that a block with the correct ID was added
        Collection<ClusterBlock> indexBlocks = newState.blocks().indices().get(indexName);
        assertNotNull("Index blocks should not be null", indexBlocks);
        assertTrue("Index should have at least one block", !indexBlocks.isEmpty());
        boolean hasBlockWithCorrectId = indexBlocks.stream().anyMatch(block -> block.id() == INDEX_SEARCH_ONLY_BLOCK_ID);
        assertTrue("Should find a block with ID " + INDEX_SEARCH_ONLY_BLOCK_ID, hasBlockWithCorrectId);

        // Test execution with missing index
        initialState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();

        ClusterState resultState = task.execute(initialState);
        assertEquals(initialState, resultState);

        // Test onFailure
        Exception testException = new Exception("Test failure");
        task.onFailure("test", testException);
        verify(listener).onFailure(testException);
    }

    public void testFinalizeScaleDownTaskFailure() {
        // Mock dependencies
        String indexName = "test_index";
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        TransportScaleIndexAction action = new TransportScaleIndexAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            allocationService,
            indicesService
        );

        TransportScaleIndexAction.FinalizeScaleDownTask task = action.new FinalizeScaleDownTask(indexName, listener);

        // Test onFailure
        Exception testException = new Exception("Test failure");
        task.onFailure("test", testException);
        verify(listener).onFailure(testException);

        // Test successful processing
        ClusterState state = mock(ClusterState.class);
        task.clusterStateProcessed("test", state, state);
        verify(listener).onResponse(any(AcknowledgedResponse.class));
    }

    public void testThreadPoolConstantValidity() {
        ThreadPool threadPool = new TestThreadPool("testThreadPoolConstantValidity");
        try {
            // Verify that our constant points to a valid thread pool
            assertNotNull("Thread pool executor should exist", threadPool.executor(TransportScaleIndexAction.SHARD_SYNC_EXECUTOR));

            // Verify SHARD_SYNC_EXECUTOR is using the MANAGEMENT pool as expected
            assertEquals(
                "SHARD_SYNC_EXECUTOR should be set to MANAGEMENT",
                ThreadPool.Names.MANAGEMENT,
                TransportScaleIndexAction.SHARD_SYNC_EXECUTOR
            );
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

}
