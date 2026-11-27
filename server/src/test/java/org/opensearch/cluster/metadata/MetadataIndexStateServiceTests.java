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

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.opensearch.action.admin.indices.datastream.DeleteDataStreamRequestTests;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockResponse.AddBlockResult;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockResponse;
import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockResponse.RemoveBlockResult;
import org.opensearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInProgressException;
import org.opensearch.snapshots.SnapshotInfoTests;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPool;
import org.hamcrest.CoreMatchers;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.opensearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID;
import static org.opensearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class MetadataIndexStateServiceTests extends OpenSearchTestCase {

    @BeforeClass
    public static void initializeIndexMetadataFirst() {
        // IndexMetadata and its nested APIBlock enum initialize each other and only work when IndexMetadata is initialized first, as it
        // always is on a running node. Do not let the first test that mentions APIBlock in this JVM decide.
        assertNotNull(IndexMetadata.INDEX_READ_ONLY_BLOCK);
    }

    public void testCloseRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metadata().index(indexName).getIndex());
            } else {
                final ClusterBlock closingBlock = MetadataIndexStateService.createIndexClosingBlock();
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metadata().index(indexName).getIndex();
                blockedIndices.put(index, closingBlock);
                if (randomBoolean()) {
                    results.put(index, new CloseIndexResponse.IndexResult(index));
                } else {
                    results.put(index, new CloseIndexResponse.IndexResult(index, new Exception("test")));
                }
            }
        }

        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(state, blockedIndices, results).v1();
        assertThat(updatedState.metadata().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), updatedState);
            assertThat(updatedState.blocks().hasIndexBlockWithId(nonBlockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(false));
        }
        for (Index blockedIndex : blockedIndices.keySet()) {
            if (results.get(blockedIndex).hasFailures() == false) {
                assertIsClosed(blockedIndex.getName(), updatedState);
            } else {
                assertIsOpened(blockedIndex.getName(), updatedState);
                assertThat(updatedState.blocks().hasIndexBlockWithId(blockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
            }
        }
    }

    public void testCloseRoutingTableWithRestoredIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithRestoredIndex")).build();

        String indexName = "restored-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addRestoredIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block)).build();

        final Index index = state.metadata().index(indexName).getIndex();
        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            singletonMap(index, block),
            singletonMap(index, new IndexResult(index))
        ).v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableWithSnapshottedIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithSnapshottedIndex")).build();

        String indexName = "snapshotted-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addSnapshotIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block)).build();

        final Index index = state.metadata().index(indexName).getIndex();
        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(
            state,
            singletonMap(index, block),
            singletonMap(index, new IndexResult(index))
        ).v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testAddIndexClosedBlocks() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testAddIndexClosedBlocks")).build();
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = new Index[] { new Index("_name", "_uid") };
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState)
            );
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = Index.EMPTY_ARRAY;

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState);
            assertSame(initialState, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            Index[] indices = new Index[] { state.metadata().index("closed").getIndex() };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertSame(state, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index[] indices = new Index[] { state.metadata().index("opened").getIndex(), state.metadata().index("closed").getIndex() };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            Index opened = updatedState.metadata().index("opened").getIndex();
            assertTrue(blockedIndices.containsKey(opened));
            assertHasBlock("opened", updatedState, blockedIndices.get(opened));

            Index closed = updatedState.metadata().index("closed").getIndex();
            assertFalse(blockedIndices.containsKey(closed));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                ClusterState state = addRestoredIndex("restored", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                if (randomBoolean()) {
                    state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[] { state.metadata().index("restored").getIndex() };
                MetadataIndexStateService.addIndexClosedBlocks(indices, unmodifiableMap(emptyMap()), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being restored: [[restored]]"));
        }
        {
            SnapshotInProgressException exception = expectThrows(SnapshotInProgressException.class, () -> {
                ClusterState state = addSnapshotIndex("snapshotted", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                if (randomBoolean()) {
                    state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[] { state.metadata().index("snapshotted").getIndex() };
                MetadataIndexStateService.addIndexClosedBlocks(indices, unmodifiableMap(emptyMap()), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being snapshotted: [[snapshotted]]"));
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addOpenedIndex("index-1", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("index-2", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            state = addOpenedIndex("index-3", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

            Index index1 = state.metadata().index("index-1").getIndex();
            Index index2 = state.metadata().index("index-2").getIndex();
            Index index3 = state.metadata().index("index-3").getIndex();
            Index[] indices = new Index[] { index1, index2, index3 };

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            for (Index index : indices) {
                assertTrue(blockedIndices.containsKey(index));
                assertHasBlock(index.getName(), updatedState, blockedIndices.get(index));
            }
        }
    }

    public void testAddIndexClosedBlocksReusesBlocks() {
        ClusterState state = ClusterState.builder(new ClusterName("testAddIndexClosedBlocksReuseBlocks")).build();
        state = addOpenedIndex("test", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

        Index test = state.metadata().index("test").getIndex();
        Index[] indices = new Index[] { test };

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);

        assertTrue(blockedIndices.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices.get(test));

        final Map<Index, ClusterBlock> blockedIndices2 = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices2, state);

        assertTrue(blockedIndices2.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices2.get(test));
        assertEquals(blockedIndices.get(test), blockedIndices2.get(test));
    }

    public void testIsIndexVerifiedBeforeClosed() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testIsIndexMetadataClosed")).build();
        {
            String indexName = "open";
            ClusterState state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().index(indexName)));
        }
        {
            String indexName = "closed";
            ClusterState state = addClosedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertTrue(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().index(indexName)));
        }
        {
            String indexName = "closed-no-setting";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .state(IndexMetadata.State.CLOSE)
                .creationDate(randomNonNegativeLong())
                .settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                        .put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 3))
                )
                .build();
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(indexMetadata));
        }
    }

    public void testCloseFailedIfBlockDisappeared() {
        ClusterState state = ClusterState.builder(new ClusterName("failedIfBlockDisappeared")).build();
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        int numIndices = between(1, 10);
        Set<Index> disappearedIndices = new HashSet<>();
        Map<Index, IndexResult> verifyResults = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            String indexName = "test-" + i;
            state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index index = state.metadata().index(indexName).getIndex();
            state = MetadataIndexStateService.addIndexClosedBlocks(new Index[] { index }, blockedIndices, state);
            if (randomBoolean()) {
                state = ClusterState.builder(state)
                    .blocks(ClusterBlocks.builder().blocks(state.blocks()).removeIndexBlocks(indexName).build())
                    .build();
                disappearedIndices.add(index);
            }
            verifyResults.put(index, new IndexResult(index));
        }
        Collection<IndexResult> closingResults = MetadataIndexStateService.closeRoutingTable(
            state,
            blockedIndices,
            unmodifiableMap(verifyResults)
        ).v2();
        assertThat(closingResults, hasSize(numIndices));
        Set<Index> failedIndices = closingResults.stream()
            .filter(IndexResult::hasFailures)
            .map(IndexResult::getIndex)
            .collect(Collectors.toSet());
        assertThat(failedIndices, equalTo(disappearedIndices));
    }

    public void testCloseCurrentWriteIndexForDataStream() {
        int numDataStreams = randomIntBetween(1, 3);
        List<Tuple<String, Integer>> dataStreamsToCreate = new ArrayList<>();
        List<String> writeIndices = new ArrayList<>();
        for (int k = 0; k < numDataStreams; k++) {
            String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
            int numBackingIndices = randomIntBetween(1, 5);
            dataStreamsToCreate.add(new Tuple<>(dataStreamName, numBackingIndices));
            writeIndices.add(DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices));
        }
        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dataStreamsToCreate, List.of());

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(cs);

        List<String> indicesToDelete = randomSubsetOf(randomIntBetween(1, numDataStreams), writeIndices);
        Index[] indicesToDeleteArray = new Index[indicesToDelete.size()];
        for (int k = 0; k < indicesToDelete.size(); k++) {
            Index indexToDelete = cs.metadata().index(indicesToDelete.get(k)).getIndex();
            indicesToDeleteArray[k] = indexToDelete;
        }
        MetadataIndexStateService service = new MetadataIndexStateService(clusterService, null, null, null, null, null, null, null);
        CloseIndexClusterStateUpdateRequest request = new CloseIndexClusterStateUpdateRequest(0L).indices(indicesToDeleteArray);
        Exception e = expectThrows(IllegalArgumentException.class, () -> service.closeIndices(request, null));
        assertThat(
            e.getMessage(),
            CoreMatchers.containsString(
                "cannot close the following data stream write indices [" + Strings.collectionToCommaDelimitedString(indicesToDelete) + "]"
            )
        );
    }

    public void testAddIndexBlockSkipsSearchOnlyShardWithoutPrimary() throws Exception {
        final String indexName = "search-only";
        final ClusterState initialState = searchOnlyClusterState(indexName);
        final AtomicReference<ClusterState> currentState = new AtomicReference<>(initialState);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenAnswer(invocation -> currentState.get());
        doAnswer(invocation -> {
            final String source = invocation.getArgument(0);
            final ClusterStateUpdateTask task = invocation.getArgument(1);
            final ClusterState oldState = currentState.get();
            final ClusterState newState = task.execute(oldState);
            currentState.set(newState);
            task.clusterStateProcessed(source, oldState, newState);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));

        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.MANAGEMENT)).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        final TransportVerifyShardIndexBlockAction verifyShardIndexBlockAction = mock(TransportVerifyShardIndexBlockAction.class);
        final MetadataIndexStateService service = new MetadataIndexStateService(
            clusterService,
            null,
            null,
            null,
            null,
            threadPool,
            null,
            verifyShardIndexBlockAction
        );
        final Index index = initialState.metadata().index(indexName).getIndex();
        final AddIndexBlockClusterStateUpdateRequest request = new AddIndexBlockClusterStateUpdateRequest(APIBlock.WRITE, 1L).indices(
            new Index[] { index }
        );
        final PlainActionFuture<AddIndexBlockResponse> future = PlainActionFuture.newFuture();

        service.addIndexBlock(request, future);
        final AddIndexBlockResponse response = future.actionGet();

        assertTrue(response.isAcknowledged());
        assertTrue(response.isShardsAcknowledged());
        assertThat(response.getIndices(), hasSize(1));
        assertFalse(response.getIndices().get(0).hasFailures());
        assertTrue(currentState.get().blocks().hasIndexBlockWithId(indexName, APIBlock.WRITE.getBlock().id()));
        verifyNoInteractions(verifyShardIndexBlockAction);
    }

    /** The API blocks that can be removed with the remove index block API. */
    private static List<APIBlock> removableBlocks() {
        return List.of(APIBlock.READ_ONLY, APIBlock.READ, APIBlock.WRITE, APIBlock.METADATA);
    }

    public void testRemoveIndexBlockClearsBlockAndSetting() {
        for (APIBlock block : removableBlocks()) {
            final ClusterState state = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, block);
            final IndexMetadata before = state.metadata().index("test");
            assertTrue(state.blocks().hasIndexBlock("test", block.getBlock()));

            final Tuple<ClusterState, List<RemoveBlockResult>> result = MetadataIndexStateService.removeIndexBlock(
                new Index[] { before.getIndex() },
                state,
                block
            );

            final ClusterState updated = result.v1();
            final IndexMetadata after = updated.metadata().index("test");
            assertFalse(block.name(), updated.blocks().hasIndexBlockWithId("test", block.getBlock().id()));
            assertTrue(block.name(), after.getSettings().hasValue(block.settingName()));
            assertFalse(block.name(), after.getSettings().getAsBoolean(block.settingName(), true));
            assertThat(block.name(), after.getSettingsVersion(), equalTo(before.getSettingsVersion() + 1));
            assertSame("routing must not be touched", state.routingTable(), updated.routingTable());
            assertThat(result.v2(), hasSize(1));
            assertThat(result.v2().get(0).getIndex(), equalTo(before.getIndex()));
            assertTrue(result.v2().get(0).isUnblocked());
        }
    }

    public void testRemoveIndexBlockIsNoOpWhenBlockIsAbsent() {
        // an index without any API block, and an index that has only other blocks
        final ClusterState plain = addOpenedIndex("test", 1, 0, emptyClusterState());
        final ClusterState withOther = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, APIBlock.READ);
        for (ClusterState state : List.of(plain, withOther)) {
            final Index index = state.metadata().index("test").getIndex();
            final Tuple<ClusterState, List<RemoveBlockResult>> result = MetadataIndexStateService.removeIndexBlock(
                new Index[] { index },
                state,
                APIBlock.WRITE
            );
            assertSame("a no-op must return the very same cluster state", state, result.v1());
            assertThat(result.v2(), hasSize(1));
            assertTrue(result.v2().get(0).isUnblocked());
            assertNull(result.v2().get(0).getException());
        }
    }

    public void testRemoveIndexBlockTwiceIsNoOpTheSecondTime() {
        final APIBlock block = randomFrom(removableBlocks());
        final ClusterState state = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, block);
        final Index[] indices = new Index[] { state.metadata().index("test").getIndex() };

        final ClusterState first = MetadataIndexStateService.removeIndexBlock(indices, state, block).v1();
        assertNotSame(state, first);
        final Tuple<ClusterState, List<RemoveBlockResult>> second = MetadataIndexStateService.removeIndexBlock(indices, first, block);
        assertSame(first, second.v1());
        assertTrue(second.v2().get(0).isUnblocked());
    }

    public void testRemoveIndexBlockRepairsSettingWhenBlockIsAlreadyGone() {
        final APIBlock block = randomFrom(removableBlocks());
        final ClusterState blocked = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, block);
        // settings still say true while the cluster block has been lost
        final ClusterState state = ClusterState.builder(blocked)
            .blocks(ClusterBlocks.builder().blocks(blocked.blocks()).removeIndexBlock("test", block.getBlock()))
            .build();
        assertFalse(state.blocks().hasIndexBlockWithId("test", block.getBlock().id()));
        final IndexMetadata before = state.metadata().index("test");

        final ClusterState updated = MetadataIndexStateService.removeIndexBlock(new Index[] { before.getIndex() }, state, block).v1();

        final IndexMetadata after = updated.metadata().index("test");
        assertFalse(after.getSettings().getAsBoolean(block.settingName(), true));
        assertThat(after.getSettingsVersion(), equalTo(before.getSettingsVersion() + 1));
        assertFalse(updated.blocks().hasIndexBlockWithId("test", block.getBlock().id()));
    }

    public void testCombinedReadOnlyAndWriteBlocksCanBeClearedInEitherOrder() {
        // https://github.com/opensearch-project/OpenSearch/issues/20293: the settings API cannot clear both and leaves the index stuck
        final List<APIBlock> order = randomBoolean()
            ? List.of(APIBlock.READ_ONLY, APIBlock.WRITE)
            : List.of(APIBlock.WRITE, APIBlock.READ_ONLY);
        final APIBlock first = order.get(0);
        final APIBlock second = order.get(1);
        ClusterState state = addIndexWithApiBlocks(
            emptyClusterState(),
            "test",
            IndexMetadata.State.OPEN,
            APIBlock.READ_ONLY,
            APIBlock.WRITE
        );
        final Index[] indices = new Index[] { state.metadata().index("test").getIndex() };
        assertTrue(state.blocks().hasIndexBlock("test", APIBlock.READ_ONLY.getBlock()));
        assertTrue(state.blocks().hasIndexBlock("test", APIBlock.WRITE.getBlock()));

        state = MetadataIndexStateService.removeIndexBlock(indices, state, first).v1();

        assertFalse(state.blocks().hasIndexBlockWithId("test", first.getBlock().id()));
        assertFalse(state.metadata().index("test").getSettings().getAsBoolean(first.settingName(), true));
        // the other block and its setting are untouched
        assertTrue(state.blocks().hasIndexBlock("test", second.getBlock()));
        assertTrue(state.metadata().index("test").getSettings().getAsBoolean(second.settingName(), false));

        state = MetadataIndexStateService.removeIndexBlock(indices, state, second).v1();

        assertThat("the index must not be left with any block", state.blocks().indices().containsKey("test"), is(false));
        assertFalse(state.blocks().indexBlocked(ClusterBlockLevel.WRITE, "test"));
        assertFalse(state.blocks().indexBlocked(ClusterBlockLevel.METADATA_WRITE, "test"));
        assertFalse(state.metadata().index("test").getSettings().getAsBoolean(APIBlock.READ_ONLY.settingName(), true));
        assertFalse(state.metadata().index("test").getSettings().getAsBoolean(APIBlock.WRITE.settingName(), true));
    }

    public void testRemoveIndexBlockLeavesOtherBlocksAndIndicesAlone() {
        ClusterState state = addIndexWithApiBlocks(
            emptyClusterState(),
            "test",
            IndexMetadata.State.OPEN,
            APIBlock.READ,
            APIBlock.WRITE,
            APIBlock.METADATA
        );
        state = addIndexWithApiBlocks(state, "other", IndexMetadata.State.OPEN, APIBlock.WRITE);
        final Index index = state.metadata().index("test").getIndex();

        final ClusterState updated = MetadataIndexStateService.removeIndexBlock(new Index[] { index }, state, APIBlock.WRITE).v1();

        assertFalse(updated.blocks().hasIndexBlockWithId("test", APIBlock.WRITE.getBlock().id()));
        assertTrue(updated.blocks().hasIndexBlock("test", APIBlock.READ.getBlock()));
        assertTrue(updated.blocks().hasIndexBlock("test", APIBlock.METADATA.getBlock()));
        assertTrue(updated.metadata().index("test").getSettings().getAsBoolean(APIBlock.READ.settingName(), false));
        assertTrue(updated.metadata().index("test").getSettings().getAsBoolean(APIBlock.METADATA.settingName(), false));
        // the untargeted index keeps its block, setting and settings version
        assertTrue(updated.blocks().hasIndexBlock("other", APIBlock.WRITE.getBlock()));
        assertTrue(updated.metadata().index("other").getSettings().getAsBoolean(APIBlock.WRITE.settingName(), false));
        assertThat(updated.metadata().index("other").getSettingsVersion(), equalTo(state.metadata().index("other").getSettingsVersion()));
    }

    public void testRemoveIndexBlockFromClosedIndexKeepsItClosed() {
        final ClusterState state = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.CLOSE, APIBlock.WRITE);
        final Index index = state.metadata().index("test").getIndex();
        assertTrue(state.blocks().hasIndexBlock("test", INDEX_CLOSED_BLOCK));

        final ClusterState updated = MetadataIndexStateService.removeIndexBlock(new Index[] { index }, state, APIBlock.WRITE).v1();

        assertFalse(updated.blocks().hasIndexBlockWithId("test", APIBlock.WRITE.getBlock().id()));
        assertFalse(updated.metadata().index("test").getSettings().getAsBoolean(APIBlock.WRITE.settingName(), true));
        assertThat(updated.metadata().index("test").getState(), is(IndexMetadata.State.CLOSE));
        assertTrue("the closed block is not an API block and must stay", updated.blocks().hasIndexBlock("test", INDEX_CLOSED_BLOCK));
    }

    public void testRemoveIndexBlockDoesNotTouchTemporaryUuidBasedBlock() {
        // a PUT _block that did not complete leaves a temporary UUID based block and index.blocks.write=true behind
        final APIBlock block = randomFrom(removableBlocks());
        final ClusterState plain = addOpenedIndex("test", 1, 0, emptyClusterState());
        final Index index = plain.metadata().index("test").getIndex();
        final ClusterState state = MetadataIndexStateService.addIndexBlock(new Index[] { index }, plain, block).v1();
        final ClusterBlock temporary = state.blocks().getIndexBlockWithId("test", block.getBlock().id());
        assertThat(temporary.uuid(), notNullValue());

        final Tuple<ClusterState, List<RemoveBlockResult>> result = MetadataIndexStateService.removeIndexBlock(
            new Index[] { index },
            state,
            block
        );

        assertSame("nothing may change while a temporary block exists", state, result.v1());
        assertTrue(result.v1().blocks().hasIndexBlock("test", temporary));
        assertTrue(result.v1().metadata().index("test").getSettings().getAsBoolean(block.settingName(), false));
        assertThat(result.v2(), hasSize(1));
        final RemoveBlockResult indexResult = result.v2().get(0);
        assertFalse(indexResult.isUnblocked());
        assertThat(indexResult.getException(), instanceOf(IllegalStateException.class));
        assertThat(indexResult.getException().getMessage(), containsString("test"));
        assertThat(indexResult.getException().getMessage(), containsString("[" + block.name + "]"));
    }

    public void testRemoveIndexBlockSucceedsOnceTemporaryBlockWasFinalized() {
        final APIBlock block = randomFrom(removableBlocks());
        final ClusterState plain = addOpenedIndex("test", 1, 0, emptyClusterState());
        final Index index = plain.metadata().index("test").getIndex();
        final Tuple<ClusterState, Map<Index, ClusterBlock>> adding = MetadataIndexStateService.addIndexBlock(
            new Index[] { index },
            plain,
            block
        );

        // while the add block call is still in flight the remove call is refused
        assertFalse(MetadataIndexStateService.removeIndexBlock(new Index[] { index }, adding.v1(), block).v2().get(0).isUnblocked());

        // once the add block call finished, the block is a regular block and can be removed
        final ClusterState finalized = MetadataIndexStateService.finalizeBlock(
            adding.v1(),
            adding.v2(),
            Map.of(index, new AddBlockResult(index)),
            block
        ).v1();
        assertTrue(finalized.blocks().hasIndexBlock("test", block.getBlock()));
        final Tuple<ClusterState, List<RemoveBlockResult>> removed = MetadataIndexStateService.removeIndexBlock(
            new Index[] { index },
            finalized,
            block
        );
        assertTrue(removed.v2().get(0).isUnblocked());
        assertFalse(removed.v1().blocks().hasIndexBlockWithId("test", block.getBlock().id()));
        assertFalse(removed.v1().metadata().index("test").getSettings().getAsBoolean(block.settingName(), true));
    }

    public void testAddIndexBlockAfterRemoveStartsOverWithFreshTemporaryBlock() {
        final APIBlock block = randomFrom(removableBlocks());
        final ClusterState blocked = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, block);
        final Index index = blocked.metadata().index("test").getIndex();
        final ClusterState removed = MetadataIndexStateService.removeIndexBlock(new Index[] { index }, blocked, block).v1();

        final Tuple<ClusterState, Map<Index, ClusterBlock>> added = MetadataIndexStateService.addIndexBlock(
            new Index[] { index },
            removed,
            block
        );

        assertThat(added.v2().keySet(), contains(index));
        assertThat(added.v2().get(index).uuid(), notNullValue());
        assertTrue(added.v1().metadata().index("test").getSettings().getAsBoolean(block.settingName(), false));
    }

    public void testRemoveIndexBlockReportsEveryIndexAndKeepsRequestOrder() {
        ClusterState state = addIndexWithApiBlocks(emptyClusterState(), "blocked", IndexMetadata.State.OPEN, APIBlock.WRITE);
        state = addOpenedIndex("plain", 1, 0, state);
        // "stuck" only has a temporary block
        state = addOpenedIndex("stuck", 1, 0, state);
        final Index blocked = state.metadata().index("blocked").getIndex();
        final Index plain = state.metadata().index("plain").getIndex();
        final Index stuck = state.metadata().index("stuck").getIndex();
        state = MetadataIndexStateService.addIndexBlock(new Index[] { stuck }, state, APIBlock.WRITE).v1();

        final Tuple<ClusterState, List<RemoveBlockResult>> result = MetadataIndexStateService.removeIndexBlock(
            new Index[] { stuck, blocked, plain },
            state,
            APIBlock.WRITE
        );

        assertThat(
            result.v2().stream().map(r -> r.getIndex().getName()).collect(Collectors.toList()),
            contains("stuck", "blocked", "plain")
        );
        assertThat(result.v2().stream().map(RemoveBlockResult::isUnblocked).collect(Collectors.toList()), contains(false, true, true));
        assertFalse(result.v1().blocks().hasIndexBlockWithId("blocked", APIBlock.WRITE.getBlock().id()));
        assertTrue("the index with the temporary block is left alone", result.v1().blocks().hasIndexBlockWithId("stuck", 8));
        assertTrue(result.v1().metadata().index("stuck").getSettings().getAsBoolean(APIBlock.WRITE.settingName(), false));
    }

    public void testRemoveIndexBlockFailsForUnknownIndex() {
        final ClusterState state = addOpenedIndex("test", 1, 0, emptyClusterState());
        expectThrows(
            IndexNotFoundException.class,
            () -> MetadataIndexStateService.removeIndexBlock(new Index[] { new Index("missing", "_na_") }, state, APIBlock.WRITE)
        );
    }

    public void testRemoveIndexBlockServiceRequiresIndices() {
        final MetadataIndexStateService service = new MetadataIndexStateService(
            mock(ClusterService.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        final RemoveIndexBlockClusterStateUpdateRequest request = new RemoveIndexBlockClusterStateUpdateRequest(APIBlock.WRITE).indices(
            new Index[0]
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.removeIndexBlock(request, PlainActionFuture.newFuture())
        );
        assertThat(e.getMessage(), equalTo("Index name is required"));
    }

    public void testRemoveIndexBlockServiceAnswersWithPerIndexResultsOnceAcknowledged() throws Exception {
        final ClusterState initial = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, APIBlock.WRITE);
        final Index index = initial.metadata().index("test").getIndex();
        final AtomicReference<ClusterStateUpdateTask> submitted = new AtomicReference<>();
        final ClusterService clusterService = mock(ClusterService.class);
        doAnswer(invocation -> {
            submitted.set(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        final MetadataIndexStateService service = new MetadataIndexStateService(clusterService, null, null, null, null, null, null, null);
        final RemoveIndexBlockClusterStateUpdateRequest request = new RemoveIndexBlockClusterStateUpdateRequest(APIBlock.WRITE).indices(
            new Index[] { index }
        ).ackTimeout(TimeValue.timeValueSeconds(7)).clusterManagerNodeTimeout(TimeValue.timeValueSeconds(9));
        final PlainActionFuture<RemoveIndexBlockResponse> future = PlainActionFuture.newFuture();

        service.removeIndexBlock(request, future);

        final ClusterStateUpdateTask task = submitted.get();
        assertThat(task, instanceOf(AckedClusterStateUpdateTask.class));
        assertThat(task.priority(), equalTo(Priority.URGENT));
        assertThat(task.timeout(), equalTo(TimeValue.timeValueSeconds(9)));
        assertThat(((AckedClusterStateUpdateTask<?>) task).ackTimeout(), equalTo(TimeValue.timeValueSeconds(7)));
        assertFalse("must not answer before the cluster state update is acknowledged", future.isDone());

        final ClusterState updated = task.execute(initial);
        assertFalse(updated.blocks().hasIndexBlockWithId("test", APIBlock.WRITE.getBlock().id()));
        ((AckedClusterStateUpdateTask<?>) task).onAllNodesAcked(null);

        final RemoveIndexBlockResponse response = future.actionGet();
        assertTrue(response.isAcknowledged());
        assertThat(response.getIndices(), hasSize(1));
        assertThat(response.getIndices().get(0).getIndex(), equalTo(index));
        assertTrue(response.getIndices().get(0).isUnblocked());
    }

    public void testRemoveIndexBlockServiceIsNotAcknowledgedWhileAnIndexKeepsItsBlock() throws Exception {
        // like the add block API: a response that reports a failed index must not claim success through "acknowledged"
        ClusterState initial = addIndexWithApiBlocks(emptyClusterState(), "unblockable", IndexMetadata.State.OPEN, APIBlock.WRITE);
        initial = addOpenedIndex("stuck", 1, 0, initial);
        final Index unblockable = initial.metadata().index("unblockable").getIndex();
        final Index stuck = initial.metadata().index("stuck").getIndex();
        initial = MetadataIndexStateService.addIndexBlock(new Index[] { stuck }, initial, APIBlock.WRITE).v1();
        final AtomicReference<ClusterStateUpdateTask> submitted = new AtomicReference<>();
        final ClusterService clusterService = mock(ClusterService.class);
        doAnswer(invocation -> {
            submitted.set(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        final MetadataIndexStateService service = new MetadataIndexStateService(clusterService, null, null, null, null, null, null, null);
        final PlainActionFuture<RemoveIndexBlockResponse> future = PlainActionFuture.newFuture();
        service.removeIndexBlock(
            new RemoveIndexBlockClusterStateUpdateRequest(APIBlock.WRITE).indices(new Index[] { unblockable, stuck }),
            future
        );

        submitted.get().execute(initial);
        ((AckedClusterStateUpdateTask<?>) submitted.get()).onAllNodesAcked(null);

        final RemoveIndexBlockResponse response = future.actionGet();
        assertFalse("every node applied the cluster state, but one index still has its block", response.isAcknowledged());
        assertThat(response.getIndices().stream().map(RemoveBlockResult::isUnblocked).collect(Collectors.toList()), contains(true, false));
    }

    public void testRemoveIndexBlockServiceReportsAckTimeoutWithResults() throws Exception {
        final ClusterState initial = addIndexWithApiBlocks(emptyClusterState(), "test", IndexMetadata.State.OPEN, APIBlock.READ);
        final Index index = initial.metadata().index("test").getIndex();
        final AtomicReference<ClusterStateUpdateTask> submitted = new AtomicReference<>();
        final ClusterService clusterService = mock(ClusterService.class);
        doAnswer(invocation -> {
            submitted.set(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        final MetadataIndexStateService service = new MetadataIndexStateService(clusterService, null, null, null, null, null, null, null);
        final PlainActionFuture<RemoveIndexBlockResponse> future = PlainActionFuture.newFuture();
        service.removeIndexBlock(new RemoveIndexBlockClusterStateUpdateRequest(APIBlock.READ).indices(new Index[] { index }), future);

        submitted.get().execute(initial);
        ((AckedClusterStateUpdateTask<?>) submitted.get()).onAckTimeout();

        final RemoveIndexBlockResponse response = future.actionGet();
        assertFalse(response.isAcknowledged());
        assertThat(response.getIndices(), hasSize(1));
        assertTrue(response.getIndices().get(0).isUnblocked());
    }

    public void testRemoveIndexBlockServiceForwardsFailures() throws Exception {
        final ClusterState initial = addOpenedIndex("test", 1, 0, emptyClusterState());
        final AtomicReference<ClusterStateUpdateTask> submitted = new AtomicReference<>();
        final ClusterService clusterService = mock(ClusterService.class);
        doAnswer(invocation -> {
            submitted.set(invocation.getArgument(1));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        final MetadataIndexStateService service = new MetadataIndexStateService(clusterService, null, null, null, null, null, null, null);
        final PlainActionFuture<RemoveIndexBlockResponse> future = PlainActionFuture.newFuture();
        service.removeIndexBlock(
            new RemoveIndexBlockClusterStateUpdateRequest(APIBlock.WRITE).indices(new Index[] { new Index("gone", "_na_") }),
            future
        );

        final Exception failure = expectThrows(IndexNotFoundException.class, () -> submitted.get().execute(initial));
        submitted.get().onFailure("test", failure);

        assertSame(failure, expectThrows(IndexNotFoundException.class, future::actionGet));
    }

    private static ClusterState emptyClusterState() {
        return ClusterState.builder(new ClusterName("testRemoveIndexBlock")).build();
    }

    /**
     * Adds an index that has the given API blocks the way the settings API or a completed add block call leaves it: the
     * {@code index.blocks.*} settings are {@code true} and the matching full (non UUID based) cluster blocks are present.
     */
    private static ClusterState addIndexWithApiBlocks(
        final ClusterState current,
        final String index,
        final IndexMetadata.State indexState,
        final APIBlock... apiBlocks
    ) {
        final ClusterState withIndex = indexState == IndexMetadata.State.OPEN
            ? addOpenedIndex(index, 1, 0, current)
            : addClosedIndex(index, 1, 0, current);
        final IndexMetadata indexMetadata = withIndex.metadata().index(index);
        final Settings.Builder settings = Settings.builder().put(indexMetadata.getSettings());
        for (APIBlock apiBlock : apiBlocks) {
            settings.put(apiBlock.settingName(), true);
        }
        final IndexMetadata blocked = IndexMetadata.builder(indexMetadata).settings(settings).build();
        return ClusterState.builder(withIndex)
            .metadata(Metadata.builder(withIndex.metadata()).put(blocked, true))
            .blocks(ClusterBlocks.builder().blocks(withIndex.blocks()).addBlocks(blocked))
            .build();
    }

    public static ClusterState addOpenedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.OPEN, null);
    }

    public static ClusterState addClosedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.CLOSE, INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addBlockedIndex(
        final String index,
        final int numShards,
        final int numReplicas,
        final ClusterState state,
        final ClusterBlock closingBlock
    ) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.OPEN, closingBlock);
    }

    private static ClusterState addRestoredIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = new HashMap<>();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new RestoreInProgress.ShardRestoreStatus(shardRouting.currentNodeId()));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            "_uuid",
            snapshot,
            RestoreInProgress.State.INIT,
            Collections.singletonList(index),
            shardsBuilder
        );
        return ClusterState.builder(newState).putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(entry).build()).build();
    }

    private static ClusterState addSnapshotIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = new HashMap<>();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.currentNodeId(), "1"));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final SnapshotsInProgress.Entry entry = new SnapshotsInProgress.Entry(
            snapshot,
            randomBoolean(),
            false,
            SnapshotsInProgress.State.INIT,
            Collections.singletonList(new IndexId(index, index)),
            Collections.emptyList(),
            randomNonNegativeLong(),
            randomLong(),
            shardsBuilder,
            null,
            SnapshotInfoTests.randomUserMetadata(),
            VersionUtils.randomVersion(random()),
            false
        );
        return ClusterState.builder(newState)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(Collections.singletonList(entry)))
            .build();
    }

    private static ClusterState searchOnlyClusterState(final String indexName) {
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                    .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
            )
            .build();
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(newShardRouting(shardId, "search-node", false, true, ShardRoutingState.STARTED, null))
            .build();

        return ClusterState.builder(new ClusterName("testSearchOnlyClusterState"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    private static ClusterState addIndex(
        final ClusterState currentState,
        final String index,
        final int numShards,
        final int numReplicas,
        final IndexMetadata.State state,
        @Nullable final ClusterBlock block
    ) {

        final Settings.Builder settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(SETTING_NUMBER_OF_REPLICAS, numReplicas);
        if (state == IndexMetadata.State.CLOSE) {
            settings.put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .state(state)
            .creationDate(randomNonNegativeLong())
            .settings(settings)
            .build();

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        clusterStateBuilder.metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true));

        final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int j = 0; j < indexMetadata.getNumberOfShards(); j++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), j);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), true, ShardRoutingState.STARTED));
            for (int k = 0; k < indexMetadata.getNumberOfReplicas(); k++) {
                indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), false, ShardRoutingState.STARTED));
            }
            indexRoutingTable.addIndexShard(indexShardRoutingBuilder.build());
        }
        clusterStateBuilder.routingTable(RoutingTable.builder(currentState.routingTable()).add(indexRoutingTable).build());

        if (block != null) {
            clusterStateBuilder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).addIndexBlock(index, block));
        }
        return clusterStateBuilder.build();
    }

    private static void assertIsOpened(final String indexName, final ClusterState clusterState) {
        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.OPEN));
        assertThat(indexMetadata.getSettings().hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
    }

    private static void assertIsClosed(final String indexName, final ClusterState clusterState) {
        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
        final Settings indexSettings = indexMetadata.getSettings();
        assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
        assertThat(indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
        assertThat(
            "Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks()
                .indices()
                .getOrDefault(indexName, emptySet())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                .count(),
            equalTo(1L)
        );

        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            assertThat(shardRoutingTable.shards().stream().allMatch(ShardRouting::unassigned), is(true));
            assertThat(
                shardRoutingTable.shards()
                    .stream()
                    .map(ShardRouting::unassignedInfo)
                    .map(UnassignedInfo::getReason)
                    .allMatch(info -> info == UnassignedInfo.Reason.INDEX_CLOSED),
                is(true)
            );
        }
    }

    private static void assertHasBlock(final String indexName, final ClusterState clusterState, final ClusterBlock closingBlock) {
        assertThat(clusterState.blocks().hasIndexBlock(indexName, closingBlock), is(true));
        assertThat(
            "Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks()
                .indices()
                .getOrDefault(indexName, emptySet())
                .stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                .count(),
            equalTo(1L)
        );
    }
}
