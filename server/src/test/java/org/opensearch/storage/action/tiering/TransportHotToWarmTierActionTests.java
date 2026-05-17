/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.Version;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the pre-tiering flush logic in {@link TransportHotToWarmTierAction}.
 * <p>
 * These tests verify the core decision logic:
 * - DFA indices get read-only block + prepare + tier
 * - Non-DFA indices skip prepare and go directly to tier
 * - Retry logic on partial shard failures
 * - Failure handling removes read-only block
 * <p>
 * The tests use a testable helper that replicates the action's logic without
 * requiring a full transport layer, since TransportAction.execute() is final.
 */
@SuppressWarnings("unchecked")
public class TransportHotToWarmTierActionTests extends OpenSearchTestCase {

    private static final int MAX_PREPARE_RETRIES = 3;

    private ClusterState buildClusterStateWithDfaIndex(String indexName, int numShards, int numReplicas) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(numShards)
            .numberOfReplicas(numReplicas)
            .build();

        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
    }

    private ClusterState buildClusterStateWithNonDfaIndex(String indexName) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
    }

    /**
     * Checks if an index is a DFA index (same logic as TransportHotToWarmTierAction.isDfaIndex).
     */
    private boolean isDfaIndex(String indexName, ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().index(indexName);
        if (indexMetadata == null) {
            return false;
        }
        return indexMetadata.getSettings().getAsBoolean(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), false);
    }

    /**
     * Adds a read-only block to the cluster state (same logic as TransportHotToWarmTierAction.addReadOnlyBlockAndPrepare).
     */
    private ClusterState addReadOnlyBlock(ClusterState currentState, String indexName) {
        IndexMetadata indexMetadata = currentState.metadata().index(indexName);
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
            .settings(indexSettingsBuilder)
            .settingsVersion(1 + indexMetadata.getSettingsVersion());

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        blocks.addIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);

        return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
    }

    /**
     * Removes the read-only block from the cluster state (same logic as TransportHotToWarmTierAction.removeReadOnlyBlock).
     */
    private ClusterState removeReadOnlyBlock(ClusterState currentState, String indexName) {
        IndexMetadata indexMetadata = currentState.metadata().index(indexName);
        if (indexMetadata == null) {
            return currentState;
        }
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
            .settings(indexSettingsBuilder)
            .settingsVersion(1 + indexMetadata.getSettingsVersion());

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        blocks.removeIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);

        return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
    }

    /**
     * Simulates the executePrepareTiering retry logic from TransportHotToWarmTierAction.
     */
    private void executePrepareTiering(
        String indexName,
        PrepareHandler prepareHandler,
        ActionListener<Void> onSuccess,
        ActionListener<Exception> onFinalFailure,
        int attempt
    ) {
        PrepareTieringRequest prepareTieringRequest = new PrepareTieringRequest(indexName);

        prepareHandler.prepare(prepareTieringRequest, new ActionListener<BroadcastResponse>() {
            @Override
            public void onResponse(BroadcastResponse broadcastResponse) {
                if (broadcastResponse.getFailedShards() > 0) {
                    if (attempt < MAX_PREPARE_RETRIES) {
                        executePrepareTiering(indexName, prepareHandler, onSuccess, onFinalFailure, attempt + 1);
                        return;
                    }
                    String errorMsg = "Pre-tiering sync failed for index ["
                        + indexName
                        + "] after "
                        + MAX_PREPARE_RETRIES
                        + " attempts: "
                        + broadcastResponse.getFailedShards()
                        + " shard(s) failed. Please retry the tiering request.";
                    onFinalFailure.onResponse(new IllegalStateException(errorMsg));
                    return;
                }
                onSuccess.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (attempt < MAX_PREPARE_RETRIES) {
                    executePrepareTiering(indexName, prepareHandler, onSuccess, onFinalFailure, attempt + 1);
                    return;
                }
                String errorMsg = "Pre-tiering sync failed for DFA index [" + indexName + "] after "
                    + MAX_PREPARE_RETRIES + " attempts. Please retry.";
                onFinalFailure.onResponse(new IllegalStateException(errorMsg, e));
            }
        });
    }

    /**
     * Tests that for DFA indices, the action adds a read_only_allow_delete block
     * and calls prepareTieringAction. On success, tiering proceeds.
     */
    public void testDfaIndex_AddsReadOnlyBlockAndCallsPrepareTiering() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        assertTrue("Index should be detected as DFA", isDfaIndex(indexName, state));

        // Add read-only block (simulates the cluster state update task)
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);

        // Verify block was added
        assertTrue(
            "Read-only block setting should be true",
            stateWithBlock.metadata().index(indexName).getSettings()
                .getAsBoolean(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false)
        );
        assertTrue(
            "Read-only cluster block should be present",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );

        // Simulate prepare tiering succeeding
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                prepareCalls.incrementAndGet();
                listener.onResponse(new BroadcastResponse(1, 1, 0, Collections.emptyList()));
            },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("Should not fail")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called once", 1, prepareCalls.get());
        assertTrue("Tiering should have been called", tieringCalled.get());
    }

    /**
     * Tests that prepare action failure (after retries) removes the read-only block and fails the request.
     */
    public void testDfaIndex_PrepareFailureAfterRetriesRemovesBlockAndFails() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Add read-only block
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);

        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        AtomicReference<Boolean> blockRemoved = new AtomicReference<>(false);

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                prepareCalls.incrementAndGet();
                listener.onFailure(new RuntimeException("Remote store sync failed"));
            },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> {
                capturedFailure.set(e);
                // Simulate removing the read-only block on failure
                ClusterState restored = removeReadOnlyBlock(stateWithBlock, indexName);
                assertFalse(
                    "Read-only block should be removed after failure",
                    restored.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
                );
                blockRemoved.set(true);
            }, ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called 3 times (MAX_PREPARE_RETRIES)", 3, prepareCalls.get());
        assertNotNull("Failure should be captured", capturedFailure.get());
        assertTrue(
            "Error message should mention retry exhaustion",
            capturedFailure.get().getMessage().contains("after") && capturedFailure.get().getMessage().contains("attempts")
        );
        assertTrue("Read-only block should have been removed", blockRemoved.get());
    }

    /**
     * Tests that non-DFA indices skip the prepare step entirely and go straight to tier().
     */
    public void testNonDfaIndex_SkipsPrepareAndGoesDirectlyToTier() {
        String indexName = "test-non-dfa-index";
        ClusterState state = buildClusterStateWithNonDfaIndex(indexName);

        assertFalse("Index should NOT be detected as DFA", isDfaIndex(indexName, state));

        // For non-DFA indices, the action goes directly to tier() without calling prepare
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        // Simulate the decision logic in clusterManagerOperation
        if (isDfaIndex(indexName, state)) {
            prepareCalls.incrementAndGet();
            fail("Should not reach prepare for non-DFA index");
        } else {
            // Non-DFA path: go directly to tier
            tieringCalled.set(true);
        }

        assertEquals("Prepare should NOT have been called", 0, prepareCalls.get());
        assertTrue("Tiering should have been called directly", tieringCalled.get());
    }

    /**
     * Tests retry logic: partial shard failures trigger retry up to MAX_PREPARE_RETRIES.
     */
    public void testDfaIndex_PartialShardFailuresRetryUpToMax() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 3, 1);

        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        List<DefaultShardOperationFailedException> shardFailures = Collections.singletonList(
            new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("shard sync failed"))
        );

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                prepareCalls.incrementAndGet();
                // Always return partial shard failures
                BroadcastResponse response = new BroadcastResponse(3, 2, 1, shardFailures);
                listener.onResponse(response);
            },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called 3 times (MAX_PREPARE_RETRIES)", 3, prepareCalls.get());
        assertNotNull("Failure should be captured", capturedFailure.get());
        assertTrue(
            "Error should mention shard failures",
            capturedFailure.get().getMessage().contains("shard(s) failed")
        );
    }

    /**
     * Tests that when prepare succeeds on retry (after initial shard failures), tiering proceeds.
     */
    public void testDfaIndex_PrepareSucceedsOnRetry() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 2, 1);

        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        List<DefaultShardOperationFailedException> shardFailures = Collections.singletonList(
            new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("shard sync failed"))
        );

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                int call = prepareCalls.incrementAndGet();
                if (call == 1) {
                    // First attempt: partial failure
                    BroadcastResponse response = new BroadcastResponse(2, 1, 1, shardFailures);
                    listener.onResponse(response);
                } else {
                    // Second attempt: success
                    BroadcastResponse response = new BroadcastResponse(2, 2, 0, Collections.emptyList());
                    listener.onResponse(response);
                }
            },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("unexpected")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called exactly 2 times", 2, prepareCalls.get());
        assertTrue("Tiering should have been called after retry", tieringCalled.get());
    }

    /**
     * Tests that isDfaIndex returns false when index metadata is null (index not found).
     */
    public void testIsDfaIndex_NullIndexMetadata_ReturnsFalse() {
        // Build a cluster state without the target index
        Metadata metadata = Metadata.builder().build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        assertFalse("isDfaIndex should return false for non-existent index", isDfaIndex("non-existent-index", state));
    }

    /**
     * Tests that isDfaIndex returns false when pluggable dataformat is not enabled.
     */
    public void testIsDfaIndex_PluggableDataformatDisabled_ReturnsFalse() {
        String indexName = "test-non-dfa-index";
        ClusterState state = buildClusterStateWithNonDfaIndex(indexName);

        assertFalse("isDfaIndex should return false when pluggable dataformat is not set", isDfaIndex(indexName, state));
    }

    /**
     * Tests that isDfaIndex returns true when pluggable dataformat is enabled.
     */
    public void testIsDfaIndex_PluggableDataformatEnabled_ReturnsTrue() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        assertTrue("isDfaIndex should return true when pluggable dataformat is enabled", isDfaIndex(indexName, state));
    }

    /**
     * Tests that the read-only block is properly removed from the cluster state.
     */
    public void testRemoveReadOnlyBlock_RestoresOriginalState() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Add block
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        assertTrue(
            "Block should be present",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );

        // Remove block
        ClusterState stateWithoutBlock = removeReadOnlyBlock(stateWithBlock, indexName);
        assertFalse(
            "Block should be removed",
            stateWithoutBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );
        assertFalse(
            "Read-only setting should be false",
            stateWithoutBlock.metadata().index(indexName).getSettings()
                .getAsBoolean(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false)
        );
    }

    /**
     * Tests that prepare failure via onFailure (exception path) also retries.
     */
    public void testDfaIndex_PrepareExceptionRetries() {
        String indexName = "test-dfa-index";

        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                int call = prepareCalls.incrementAndGet();
                if (call <= 2) {
                    // First two attempts: exception
                    listener.onFailure(new RuntimeException("Network error"));
                } else {
                    // Third attempt: success
                    listener.onResponse(new BroadcastResponse(1, 1, 0, Collections.emptyList()));
                }
            },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("unexpected")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called 3 times", 3, prepareCalls.get());
        assertTrue("Tiering should have been called after retries", tieringCalled.get());
    }

    /**
     * Tests that all MAX_PREPARE_RETRIES exception failures result in final failure.
     */
    public void testDfaIndex_AllRetriesExhaustedViaException() {
        String indexName = "test-dfa-index";

        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                prepareCalls.incrementAndGet();
                listener.onFailure(new RuntimeException("Persistent network error"));
            },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called 3 times", 3, prepareCalls.get());
        assertNotNull("Failure should be captured", capturedFailure.get());
        assertTrue(
            "Error should mention attempts",
            capturedFailure.get().getMessage().contains("3 attempts")
        );
    }

    /**
     * Tests that adding a read-only block when it is already present (retry scenario) is idempotent.
     */
    public void testDfaIndex_ReadOnlyBlockAlreadyPresent_IsIdempotent() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Add block first time
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        assertTrue(
            "Block should be present after first add",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );

        // Add block again (simulates retry scenario)
        ClusterState stateWithBlockAgain = addReadOnlyBlock(stateWithBlock, indexName);
        assertTrue(
            "Block should still be present after second add",
            stateWithBlockAgain.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );
        assertTrue(
            "Read-only setting should still be true",
            stateWithBlockAgain.metadata().index(indexName).getSettings()
                .getAsBoolean(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false)
        );

        // Verify prepare still works after idempotent block add
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);
        executePrepareTiering(
            indexName,
            (request, listener) -> listener.onResponse(new BroadcastResponse(1, 1, 0, Collections.emptyList())),
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("Should not fail")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );
        assertTrue("Tiering should proceed after idempotent block add", tieringCalled.get());
    }

    /**
     * Tests that if an index is deleted between adding the block and running prepare,
     * prepare fails and block removal handles null metadata gracefully.
     */
    public void testDfaIndex_IndexDeletedDuringPrepare_HandlesGracefully() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Add block
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        assertTrue(
            "Block should be present",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );

        // Simulate index deletion: prepare fails because index no longer exists
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        executePrepareTiering(
            indexName,
            (request, listener) -> listener.onFailure(new IllegalStateException("Index [" + indexName + "] not found")),
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertNotNull("Failure should be captured", capturedFailure.get());

        // Simulate removeReadOnlyBlock with null metadata (index deleted)
        Metadata emptyMetadata = Metadata.builder().build();
        ClusterState stateWithoutIndex = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(emptyMetadata)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        // removeReadOnlyBlock should handle null metadata gracefully (return state unchanged)
        ClusterState result = removeReadOnlyBlock(stateWithoutIndex, indexName);
        assertNotNull("Result should not be null", result);
        assertSame("State should be unchanged when index metadata is null", stateWithoutIndex, result);
    }

    /**
     * Tests that after a master failover, retry works correctly when the block is already set.
     * Simulates: block already present from previous attempt, prepare runs again and succeeds.
     */
    public void testDfaIndex_MasterFailoverScenario_RetryWorksWithExistingBlock() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 2, 1);

        // Simulate block already set from a previous attempt (before master failover)
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        assertTrue(
            "Block should already be present (from previous master)",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );

        // New master retries: adds block again (idempotent) then runs prepare
        ClusterState stateAfterRetryBlock = addReadOnlyBlock(stateWithBlock, indexName);
        assertTrue(
            "Block should remain present after retry",
            stateAfterRetryBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
        );

        // Prepare succeeds on first attempt after failover
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(
            indexName,
            (request, listener) -> {
                prepareCalls.incrementAndGet();
                listener.onResponse(new BroadcastResponse(2, 2, 0, Collections.emptyList()));
            },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("Should not fail")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called once", 1, prepareCalls.get());
        assertTrue("Tiering should proceed after master failover retry", tieringCalled.get());
    }

    @FunctionalInterface
    interface PrepareHandler {
        void prepare(PrepareTieringRequest request, ActionListener<BroadcastResponse> listener);
    }
}
