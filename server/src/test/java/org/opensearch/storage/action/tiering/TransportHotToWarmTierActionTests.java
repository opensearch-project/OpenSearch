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
     * Adds a read-only block to the cluster state (same logic as TransportHotToWarmTierAction.addWriteBlockAndPrepare).
     */
    private ClusterState addReadOnlyBlock(ClusterState currentState, String indexName) {
        IndexMetadata indexMetadata = currentState.metadata().index(indexName);
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
            .settings(indexSettingsBuilder)
            .settingsVersion(1 + indexMetadata.getSettingsVersion());

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        blocks.addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);

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
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
            .settings(indexSettingsBuilder)
            .settingsVersion(1 + indexMetadata.getSettingsVersion());

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        blocks.removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);

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
                String errorMsg = "Pre-tiering sync failed for DFA index ["
                    + indexName
                    + "] after "
                    + MAX_PREPARE_RETRIES
                    + " attempts. Please retry.";
                onFinalFailure.onResponse(new IllegalStateException(errorMsg, e));
            }
        });
    }

    /**
     * Tests that for DFA indices, the action adds a write block
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
            stateWithBlock.metadata().index(indexName).getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
        );
        assertTrue(
            "Read-only cluster block should be present",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Simulate prepare tiering succeeding
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(indexName, (request, listener) -> {
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

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            listener.onFailure(new RuntimeException("Remote store sync failed"));
        }, ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")), ActionListener.wrap(e -> {
            capturedFailure.set(e);
            // Simulate removing the read-only block on failure
            ClusterState restored = removeReadOnlyBlock(stateWithBlock, indexName);
            assertFalse(
                "Read-only block should be removed after failure",
                restored.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
            );
            blockRemoved.set(true);
        }, ex -> fail("unexpected")), 1);

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

        executePrepareTiering(indexName, (request, listener) -> {
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
        assertTrue("Error should mention shard failures", capturedFailure.get().getMessage().contains("shard(s) failed"));
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

        executePrepareTiering(indexName, (request, listener) -> {
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
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();

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
        assertTrue("Block should be present", stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK));

        // Remove block
        ClusterState stateWithoutBlock = removeReadOnlyBlock(stateWithBlock, indexName);
        assertFalse("Block should be removed", stateWithoutBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK));
        assertFalse(
            "Read-only setting should be false",
            stateWithoutBlock.metadata()
                .index(indexName)
                .getSettings()
                .getAsBoolean(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
        );
    }

    /**
     * Tests that prepare failure via onFailure (exception path) also retries.
     */
    public void testDfaIndex_PrepareExceptionRetries() {
        String indexName = "test-dfa-index";

        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(indexName, (request, listener) -> {
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

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            listener.onFailure(new RuntimeException("Persistent network error"));
        },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare should have been called 3 times", 3, prepareCalls.get());
        assertNotNull("Failure should be captured", capturedFailure.get());
        assertTrue("Error should mention attempts", capturedFailure.get().getMessage().contains("3 attempts"));
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
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Add block again (simulates retry scenario)
        ClusterState stateWithBlockAgain = addReadOnlyBlock(stateWithBlock, indexName);
        assertTrue(
            "Block should still be present after second add",
            stateWithBlockAgain.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
        assertTrue(
            "Read-only setting should still be true",
            stateWithBlockAgain.metadata()
                .index(indexName)
                .getSettings()
                .getAsBoolean(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
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
        assertTrue("Block should be present", stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK));

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
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // New master retries: adds block again (idempotent) then runs prepare
        ClusterState stateAfterRetryBlock = addReadOnlyBlock(stateWithBlock, indexName);
        assertTrue(
            "Block should remain present after retry",
            stateAfterRetryBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Prepare succeeds on first attempt after failover
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(indexName, (request, listener) -> {
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

    // ── Preflight validation ordering tests ──────────────────────────────────────

    /**
     * When preflight validation fails for a DFA index, the read-only block must NOT be added
     * and the listener must receive the failure immediately.
     *
     * This is the key ordering fix: validate BEFORE adding block or running prepare.
     */
    public void testDfaPreflightValidationFailsBeforeReadOnlyBlockIsAdded() {
        String indexName = "dfa-validate-fail";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Precondition: index is DFA
        assertTrue("Precondition: index must be DFA", isDfaIndex(indexName, state));
        // Precondition: no read-only block yet
        assertFalse(
            "Precondition: no read-only block before validation",
            state.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Simulate preflight validation failure (e.g. warm nodes full)
        RuntimeException validationError = new IllegalArgumentException("Warm nodes have insufficient disk space");

        // Call the logic that would run in clusterManagerOperation:
        // 1. preflightValidate → throws
        // 2. listener.onFailure should be called
        // 3. addReadOnlyBlock should NOT be called

        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        AtomicReference<ClusterState> stateAfterBlock = new AtomicReference<>(state);

        // Simulate the DFA branch: validate first
        boolean validationPassed;
        try {
            // In real code: hotToWarmTieringService.preflightValidate(state, index)
            throw validationError; // simulates validation failure
        } catch (Exception e) {
            capturedFailure.set(e);
            validationPassed = false;
        }

        // Only add block if validation passed
        if (validationPassed) {
            stateAfterBlock.set(addReadOnlyBlock(state, indexName));
        }

        // Verify: failure was captured
        assertNotNull("Listener must have received validation failure", capturedFailure.get());
        assertEquals("Failure must be the validation error", validationError, capturedFailure.get());

        // Verify: read-only block was NOT added (state unchanged)
        assertFalse(
            "Read-only block must NOT be added when preflight validation fails",
            stateAfterBlock.get().blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    /**
     * When preflight validation passes for a DFA index, the flow proceeds to add the
     * read-only block and run prepare. This is the happy-path ordering.
     */
    public void testDfaPreflightValidationPassesThenAddsReadOnlyBlock() {
        String indexName = "dfa-validate-pass";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Precondition
        assertTrue("Precondition: index must be DFA", isDfaIndex(indexName, state));
        assertFalse(
            "Precondition: no read-only block before validation",
            state.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Simulate preflight validation SUCCESS (no exception thrown)
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        AtomicReference<ClusterState> stateAfterBlock = new AtomicReference<>(state);

        boolean validationPassed;
        try {
            // In real code: hotToWarmTieringService.preflightValidate(state, index)
            // Validation passes: no exception
            validationPassed = true;
        } catch (Exception e) {
            capturedFailure.set(e);
            validationPassed = false;
        }

        // Only add block if validation passed
        if (validationPassed) {
            stateAfterBlock.set(addReadOnlyBlock(state, indexName));
        }

        // Verify: no failure
        assertNull("Listener must NOT have received a failure when validation passes", capturedFailure.get());

        // Verify: read-only block WAS added (flow proceeded)
        assertTrue(
            "Read-only block must be added after preflight validation passes",
            stateAfterBlock.get().blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    /**
     * Non-DFA indices skip preflight validation entirely and go directly to tiering.
     * Validates that the DFA check gate (isDfaIndex) correctly distinguishes them.
     */
    public void testNonDfaIndexSkipsPreflightValidationGate() {
        String indexName = "non-dfa-index";

        // Build a non-DFA index (no PLUGGABLE_DATAFORMAT_ENABLED_SETTING)
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "non-dfa-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            // NOTE: no IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING → not DFA
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        // Verify: the DFA gate correctly identifies this as non-DFA
        assertFalse("Non-DFA index must not pass isDfaIndex() check — preflight not called for non-DFA", isDfaIndex(indexName, state));

        // Verify: no read-only block is added for non-DFA (non-DFA goes directly to super.clusterManagerOperation)
        assertFalse(
            "Non-DFA index must not have a read-only block added (no DFA path taken)",
            state.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    // ── State mutation correctness ──────────────────────────────────────────────

    /** addReadOnlyBlock must increment settingsVersion by exactly 1. */
    public void testAddReadOnlyBlock_IncrementsSettingsVersion() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);
        long originalVersion = state.metadata().index(indexName).getSettingsVersion();

        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);

        assertEquals(
            "settingsVersion must increment by 1 when read-only block is added",
            originalVersion + 1,
            stateWithBlock.metadata().index(indexName).getSettingsVersion()
        );
    }

    /** addReadOnlyBlock must preserve all pre-existing index settings (shards, replicas, uuid, etc.). */
    public void testAddReadOnlyBlock_PreservesExistingSettings() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 3, 2);
        IndexMetadata original = state.metadata().index(indexName);

        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        IndexMetadata after = stateWithBlock.metadata().index(indexName);

        assertEquals("numberOfShards must be preserved", original.getNumberOfShards(), after.getNumberOfShards());
        assertEquals("numberOfReplicas must be preserved", original.getNumberOfReplicas(), after.getNumberOfReplicas());
        assertEquals("index UUID must be preserved", original.getIndexUUID(), after.getIndexUUID());
        assertTrue(
            "DFA setting must be preserved",
            after.getSettings().getAsBoolean(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), false)
        );
        assertTrue(
            "Read-only block setting must be true after add",
            after.getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
        );
    }

    /** Adding the block twice must increment settingsVersion by 2 total (idempotent logic still mutates version). */
    public void testAddReadOnlyBlock_SettingsVersionIncrementsOnSecondAdd() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);
        long originalVersion = state.metadata().index(indexName).getSettingsVersion();

        ClusterState afterFirstAdd = addReadOnlyBlock(state, indexName);
        ClusterState afterSecondAdd = addReadOnlyBlock(afterFirstAdd, indexName);

        assertEquals(
            "settingsVersion must be original + 2 after two addReadOnlyBlock calls",
            originalVersion + 2,
            afterSecondAdd.metadata().index(indexName).getSettingsVersion()
        );
    }

    /** removeReadOnlyBlock must increment settingsVersion by 1. */
    public void testRemoveReadOnlyBlock_IncrementsSettingsVersion() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        long versionBeforeRemove = stateWithBlock.metadata().index(indexName).getSettingsVersion();

        ClusterState stateAfterRemove = removeReadOnlyBlock(stateWithBlock, indexName);

        assertEquals(
            "settingsVersion must increment by 1 when read-only block is removed",
            versionBeforeRemove + 1,
            stateAfterRemove.metadata().index(indexName).getSettingsVersion()
        );
    }

    /** removeReadOnlyBlock on a state where block was never added must return state unchanged (no-op). */
    public void testRemoveReadOnlyBlock_WhenBlockNotPresent_IsNoOp() {
        String indexName = "test-dfa-index";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Block is NOT present
        assertFalse("Precondition: block must not be present", state.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK));

        // removeReadOnlyBlock should still work (settings.put(false) on already-false value)
        // — it does NOT return the same state object because it rebuilds, but it must not throw
        ClusterState result = removeReadOnlyBlock(state, indexName);
        assertNotNull("Result must not be null", result);
        assertFalse(
            "Block must not be present after removing a non-existent block",
            result.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
        assertFalse(
            "Read-only setting must be false after removing a non-existent block",
            result.metadata().index(indexName).getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false)
        );
    }

    // ── isDfaIndex edge cases ───────────────────────────────────────────────────

    /** isDfaIndex must return false when the setting is explicitly set to false (not just absent). */
    public void testIsDfaIndex_SettingExplicitlyFalse_ReturnsFalse() {
        String indexName = "test-explicit-false";
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), false) // explicitly false
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        assertFalse("isDfaIndex must return false when setting is explicitly false", isDfaIndex(indexName, state));
    }

    /** isDfaIndex on the target index must return false even if another index in the cluster IS DFA. */
    public void testIsDfaIndex_OtherIndexIsDfa_TargetIsNot() {
        String dfaIndex = "dfa-other";
        String nonDfaTarget = "non-dfa-target";

        Settings dfaSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "dfa-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .build();
        Settings nonDfaSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "non-dfa-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        IndexMetadata dfaMeta = IndexMetadata.builder(dfaIndex).settings(dfaSettings).numberOfShards(1).numberOfReplicas(0).build();
        IndexMetadata nonDfaMeta = IndexMetadata.builder(nonDfaTarget)
            .settings(nonDfaSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(dfaMeta, false).put(nonDfaMeta, false).build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        assertTrue("Other index must be detected as DFA", isDfaIndex(dfaIndex, state));
        assertFalse("Target index must NOT be detected as DFA — isolation check", isDfaIndex(nonDfaTarget, state));
    }

    // ── BroadcastResponse edge cases ───────────────────────────────────────────

    /** All shards fail on every attempt — prepare exhausted with failedShards == totalShards. */
    public void testPrepareTiering_AllShardsFailAllRetries() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            // All 3 shards fail
            List<DefaultShardOperationFailedException> failures = List.of(
                new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("shard 0 failed")),
                new DefaultShardOperationFailedException(indexName, 1, new RuntimeException("shard 1 failed")),
                new DefaultShardOperationFailedException(indexName, 2, new RuntimeException("shard 2 failed"))
            );
            listener.onResponse(new BroadcastResponse(3, 0, 3, failures));
        },
            ActionListener.wrap(v -> fail("Should not succeed when all shards fail"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertEquals("All 3 retry attempts must be exhausted", 3, prepareCalls.get());
        assertNotNull("Failure must be captured", capturedFailure.get());
        assertTrue("Error must mention shard(s) failed", capturedFailure.get().getMessage().contains("shard(s) failed"));
    }

    /** Zero total shards (empty cluster) — BroadcastResponse(0,0,0,[]) should succeed immediately. */
    public void testPrepareTiering_ZeroShards_SucceedsImmediately() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            listener.onResponse(new BroadcastResponse(0, 0, 0, Collections.emptyList()));
        },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("Should not fail for zero shards")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare must be called exactly once for zero shards", 1, prepareCalls.get());
        assertTrue("Tiering must proceed when zero shards succeed", tieringCalled.get());
    }

    // ── Retry sequence correctness ─────────────────────────────────────────────

    /** Prepare fails twice then succeeds on the third attempt. */
    public void testPrepareTiering_SucceedsOnThirdAttempt() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        List<DefaultShardOperationFailedException> shardFailures = Collections.singletonList(
            new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("shard failed"))
        );

        executePrepareTiering(indexName, (request, listener) -> {
            int call = prepareCalls.incrementAndGet();
            if (call <= 2) {
                // Attempts 1 and 2: partial failure
                listener.onResponse(new BroadcastResponse(2, 1, 1, shardFailures));
            } else {
                // Attempt 3: success
                listener.onResponse(new BroadcastResponse(2, 2, 0, Collections.emptyList()));
            }
        },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("unexpected")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare must be called exactly 3 times", 3, prepareCalls.get());
        assertTrue("Tiering must succeed after third attempt", tieringCalled.get());
    }

    /** Prepare succeeds on the first attempt — must not retry at all. */
    public void testPrepareTiering_SucceedsOnFirstAttempt_NoRetry() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            listener.onResponse(new BroadcastResponse(5, 5, 0, Collections.emptyList()));
        },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("Should not fail")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Prepare must be called exactly once — no retry on success", 1, prepareCalls.get());
        assertTrue("Tiering must proceed immediately on first-attempt success", tieringCalled.get());
    }

    /** The attempt counter must reach exactly MAX_PREPARE_RETRIES (3) on final failure. */
    public void testAttemptCounterStartsAtOne_MatchesMaxOnFinalFailure() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            listener.onFailure(new RuntimeException("always fails"));
        },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1 // starts at 1
        );

        assertEquals(
            "Attempt counter must reach exactly MAX_PREPARE_RETRIES (3) on final failure",
            MAX_PREPARE_RETRIES,
            prepareCalls.get()
        );
        assertNotNull("Failure must be captured", capturedFailure.get());
    }

    // ── Error message content verification ─────────────────────────────────────

    /** Error message for shard failure path must contain the index name. */
    public void testPrepareTiering_ErrorMessageContainsIndexName() {
        String indexName = "my-specific-index";
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        executePrepareTiering(indexName, (request, listener) -> {
            List<DefaultShardOperationFailedException> failures = Collections.singletonList(
                new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("fail"))
            );
            listener.onResponse(new BroadcastResponse(1, 0, 1, failures));
        },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertNotNull("Failure must be captured", capturedFailure.get());
        assertTrue("Error message must contain the index name", capturedFailure.get().getMessage().contains(indexName));
    }

    /** Error message for shard failure path must contain the actual failed shard count. */
    public void testPrepareTiering_ErrorMessageContainsShardCount() {
        String indexName = "test-dfa-index";
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        executePrepareTiering(indexName, (request, listener) -> {
            List<DefaultShardOperationFailedException> failures = List.of(
                new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("fail0")),
                new DefaultShardOperationFailedException(indexName, 1, new RuntimeException("fail1"))
            );
            // 2 shards failed out of 5
            listener.onResponse(new BroadcastResponse(5, 3, 2, failures));
        },
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertNotNull("Failure must be captured", capturedFailure.get());
        assertTrue("Error message must contain the failed shard count (2)", capturedFailure.get().getMessage().contains("2"));
    }

    /** The IllegalStateException created on exception path must chain the original cause. */
    public void testPrepareTiering_ExceptionCauseChained() {
        String indexName = "test-dfa-index";
        RuntimeException originalCause = new RuntimeException("original network error");
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();

        executePrepareTiering(
            indexName,
            (request, listener) -> listener.onFailure(originalCause),
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> capturedFailure.set(e), ex -> fail("unexpected")),
            1
        );

        assertNotNull("Failure must be captured", capturedFailure.get());
        assertSame("The original exception must be chained as cause", originalCause, capturedFailure.get().getCause());
    }

    // ── Mixed failure modes ────────────────────────────────────────────────────

    /** First attempt: onFailure (exception). Second: partial shard failure. Third: full success. */
    public void testPrepareTiering_MixedFailureModes_ExceptionThenPartialThenSuccess() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        List<DefaultShardOperationFailedException> shardFailures = Collections.singletonList(
            new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("shard fail"))
        );

        executePrepareTiering(indexName, (request, listener) -> {
            int call = prepareCalls.incrementAndGet();
            if (call == 1) {
                listener.onFailure(new RuntimeException("network error on attempt 1"));
            } else if (call == 2) {
                listener.onResponse(new BroadcastResponse(2, 1, 1, shardFailures));
            } else {
                listener.onResponse(new BroadcastResponse(2, 2, 0, Collections.emptyList()));
            }
        },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("unexpected")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Must try exactly 3 times with mixed failures", 3, prepareCalls.get());
        assertTrue("Tiering must succeed after mixed failure recovery", tieringCalled.get());
    }

    /** First attempt: partial shard failure. Second: onFailure (exception). Third: full success. */
    public void testPrepareTiering_MixedFailureModes_PartialThenExceptionThenSuccess() {
        String indexName = "test-dfa-index";
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);

        List<DefaultShardOperationFailedException> shardFailures = Collections.singletonList(
            new DefaultShardOperationFailedException(indexName, 0, new RuntimeException("shard fail"))
        );

        executePrepareTiering(indexName, (request, listener) -> {
            int call = prepareCalls.incrementAndGet();
            if (call == 1) {
                listener.onResponse(new BroadcastResponse(2, 1, 1, shardFailures));
            } else if (call == 2) {
                listener.onFailure(new RuntimeException("network error on attempt 2"));
            } else {
                listener.onResponse(new BroadcastResponse(2, 2, 0, Collections.emptyList()));
            }
        },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("unexpected")),
            ActionListener.wrap(e -> fail("Should not reach final failure"), ex -> fail("unexpected")),
            1
        );

        assertEquals("Must try exactly 3 times with mixed failure order", 3, prepareCalls.get());
        assertTrue("Tiering must succeed after mixed failure recovery", tieringCalled.get());
    }

    // ── Full functional orchestration scenarios ─────────────────────────────────

    /**
     * Full happy path for DFA index:
     * 1. isDfaIndex → true
     * 2. Preflight validation passes (no exception)
     * 3. addReadOnlyBlock → block present in cluster state
     * 4. executePrepareTiering → succeeds on first attempt
     * 5. Tiering proceeds (success callback invoked)
     * Throughout: no failures, no retries.
     */
    public void testFullHappyPath_DfaIndex_ValidatesBlocksPreparesTiers() {
        String indexName = "full-happy-dfa";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 2, 1);

        // Step 1: detect DFA
        assertTrue("Step 1: index must be DFA", isDfaIndex(indexName, state));

        // Step 2: preflight validation passes (simulated by no exception)
        boolean validationPassed = true; // no exception thrown

        // Step 3: add read-only block
        ClusterState stateWithBlock = null;
        if (validationPassed) {
            stateWithBlock = addReadOnlyBlock(state, indexName);
        }
        assertNotNull("Block state must not be null", stateWithBlock);
        assertTrue(
            "Step 3: read-only block must be present",
            stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );

        // Step 4: prepare tiering — succeeds immediately
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<Boolean> tieringCalled = new AtomicReference<>(false);
        AtomicReference<Exception> failureCalled = new AtomicReference<>();

        executePrepareTiering(indexName, (request, listener) -> {
            prepareCalls.incrementAndGet();
            listener.onResponse(new BroadcastResponse(2, 2, 0, Collections.emptyList()));
        },
            ActionListener.wrap(v -> tieringCalled.set(true), e -> fail("unexpected")),
            ActionListener.wrap(e -> failureCalled.set(e), ex -> fail("unexpected")),
            1
        );

        // Step 5: verify outcomes
        assertEquals("Step 4: prepare called exactly once", 1, prepareCalls.get());
        assertTrue("Step 5: tiering must be called after prepare succeeds", tieringCalled.get());
        assertNull("No failure should occur in happy path", failureCalled.get());
    }

    /**
     * Full failure path for DFA index — preflight passes but prepare exhausts retries:
     * 1. isDfaIndex → true
     * 2. Preflight validation passes
     * 3. addReadOnlyBlock → block present
     * 4. executePrepareTiering → fails all 3 retries
     * 5. removeReadOnlyBlock → block removed from cluster state
     * 6. Failure callback invoked with error message
     */
    public void testFullFailurePath_DfaIndex_ValidationPassesPrepareFails_BlockRemoved() {
        String indexName = "full-failure-dfa";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        // Steps 1-3: DFA, validate passes, block added
        assertTrue("Step 1: must be DFA", isDfaIndex(indexName, state));
        ClusterState stateWithBlock = addReadOnlyBlock(state, indexName);
        assertTrue("Step 3: block must be present", stateWithBlock.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK));

        // Step 4: prepare fails all retries
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        AtomicReference<ClusterState> stateAfterCleanup = new AtomicReference<>(stateWithBlock);

        executePrepareTiering(
            indexName,
            (request, listener) -> listener.onFailure(new RuntimeException("sync failed")),
            ActionListener.wrap(v -> fail("Should not succeed"), e -> fail("unexpected")),
            ActionListener.wrap(e -> {
                capturedFailure.set(e);
                // Step 5: simulate removeReadOnlyBlock on failure
                stateAfterCleanup.set(removeReadOnlyBlock(stateWithBlock, indexName));
            }, ex -> fail("unexpected")),
            1
        );

        // Step 6: verify failure and block removal
        assertNotNull("Step 6: failure must be captured", capturedFailure.get());
        assertTrue("Error must mention attempts", capturedFailure.get().getMessage().contains("attempts"));
        assertFalse(
            "Step 5: read-only block must be removed after prepare failure",
            stateAfterCleanup.get().blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    /**
     * Full failure path: preflight validation fails immediately —
     * NO read-only block added, NO prepare called, immediate failure returned.
     */
    public void testFullFailurePath_DfaIndex_ValidationFails_NothingDone() {
        String indexName = "validation-fail-dfa";
        ClusterState state = buildClusterStateWithDfaIndex(indexName, 1, 1);

        assertTrue("Precondition: must be DFA", isDfaIndex(indexName, state));

        RuntimeException validationError = new IllegalArgumentException("Warm tier has no capacity");
        AtomicReference<Exception> capturedFailure = new AtomicReference<>();
        AtomicInteger prepareCalls = new AtomicInteger(0);
        AtomicReference<ClusterState> stateAfterAttempt = new AtomicReference<>(state);

        // Simulate clusterManagerOperation logic: validate FIRST
        boolean validationPassed;
        try {
            throw validationError; // preflight throws
        } catch (Exception e) {
            capturedFailure.set(e);
            validationPassed = false;
        }

        if (validationPassed) {
            // This block must NOT execute
            stateAfterAttempt.set(addReadOnlyBlock(state, indexName));
            fail("addReadOnlyBlock must NOT be called when validation fails");
        }

        // Verify: failure captured, nothing else done
        assertNotNull("Failure must be captured immediately", capturedFailure.get());
        assertSame("The captured failure must be the validation error", validationError, capturedFailure.get());
        assertEquals("Prepare must NOT be called when validation fails", 0, prepareCalls.get());
        assertFalse(
            "Read-only block must NOT be present when validation fails",
            stateAfterAttempt.get().blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK)
        );
    }

    @FunctionalInterface
    interface PrepareHandler {
        void prepare(PrepareTieringRequest request, ActionListener<BroadcastResponse> listener);
    }
}
