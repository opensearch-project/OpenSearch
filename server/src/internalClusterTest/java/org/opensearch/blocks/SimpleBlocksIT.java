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

package org.opensearch.blocks;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockRequestBuilder;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.opensearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class SimpleBlocksIT extends OpenSearchIntegTestCase {
    public void testVerifyIndexAndClusterReadOnly() throws Exception {
        // cluster.read_only = null: write and metadata not blocked
        canCreateIndex("test1");
        canIndexDocument("test1");
        setIndexReadOnly("test1", "false");
        canIndexExists("test1");

        // cluster.read_only = true: block write and metadata
        setClusterReadOnly(true);
        canNotCreateIndex("test2");
        // even if index has index.read_only = false
        canNotIndexDocument("test1");
        canIndexExists("test1");

        // cluster.read_only = false: removes the block
        setClusterReadOnly(false);
        canCreateIndex("test2");
        canIndexDocument("test2");
        canIndexDocument("test1");
        canIndexExists("test1");

        // newly created an index has no blocks
        canCreateIndex("ro");
        canIndexDocument("ro");
        canIndexExists("ro");

        // adds index write and metadata block
        setIndexReadOnly("ro", "true");
        canNotIndexDocument("ro");
        canIndexExists("ro");

        // other indices not blocked
        canCreateIndex("rw");
        canIndexDocument("rw");
        canIndexExists("rw");

        // blocks can be removed
        setIndexReadOnly("ro", "false");
        canIndexDocument("ro");
        canIndexExists("ro");
    }

    public void testIndexReadWriteMetadataBlocks() {
        canCreateIndex("test1");
        canIndexDocument("test1");
        client().admin()
            .indices()
            .prepareUpdateSettings("test1")
            .setSettings(Settings.builder().put(SETTING_BLOCKS_WRITE, true))
            .execute()
            .actionGet();
        canNotIndexDocument("test1");
        client().admin()
            .indices()
            .prepareUpdateSettings("test1")
            .setSettings(Settings.builder().put(SETTING_BLOCKS_WRITE, false))
            .execute()
            .actionGet();
        canIndexDocument("test1");
    }

    private void canCreateIndex(String index) {
        try {
            CreateIndexResponse r = client().admin().indices().prepareCreate(index).execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotCreateIndex(String index) {
        try {
            client().admin().indices().prepareCreate(index).execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void canIndexDocument(String index) {
        try {
            IndexRequestBuilder builder = client().prepareIndex(index);
            builder.setSource("foo", "bar");
            IndexResponse r = builder.execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotIndexDocument(String index) {
        try {
            IndexRequestBuilder builder = client().prepareIndex(index);
            builder.setSource("foo", "bar");
            builder.execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void canIndexExists(String index) {
        try {
            IndicesExistsResponse r = client().admin().indices().prepareExists(index).execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void setIndexReadOnly(String index, Object value) {
        HashMap<String, Object> newSettings = new HashMap<>();
        newSettings.put(SETTING_READ_ONLY, value);

        UpdateSettingsRequestBuilder settingsRequest = client().admin().indices().prepareUpdateSettings(index);
        settingsRequest.setSettings(newSettings);
        AcknowledgedResponse settingsResponse = settingsRequest.execute().actionGet();
        assertThat(settingsResponse, notNullValue());
    }

    public void testAddBlocksWhileExistingBlocks() {
        createIndex("test");
        ensureGreen("test");

        for (APIBlock otherBlock : APIBlock.values()) {
            if (otherBlock == APIBlock.READ_ONLY_ALLOW_DELETE) {
                continue;
            }

            for (APIBlock block : Arrays.asList(APIBlock.READ, APIBlock.WRITE)) {
                try {
                    enableIndexBlock("test", block.settingName());

                    // Adding a block is not blocked
                    AcknowledgedResponse addBlockResponse = client().admin().indices().prepareAddBlock(otherBlock, "test").get();
                    assertAcked(addBlockResponse);
                } finally {
                    disableIndexBlock("test", otherBlock.settingName());
                    disableIndexBlock("test", block.settingName());
                }
            }

            for (APIBlock block : Arrays.asList(APIBlock.READ_ONLY, APIBlock.METADATA, APIBlock.READ_ONLY_ALLOW_DELETE)) {
                boolean success = false;
                try {
                    enableIndexBlock("test", block.settingName());
                    // Adding a block is blocked when there is a metadata block and the new block to be added is not a metadata block
                    if (block.getBlock().contains(ClusterBlockLevel.METADATA_WRITE)
                        && otherBlock.getBlock().contains(ClusterBlockLevel.METADATA_WRITE) == false) {
                        assertBlocked(client().admin().indices().prepareAddBlock(otherBlock, "test"));
                    } else {
                        assertAcked(client().admin().indices().prepareAddBlock(otherBlock, "test"));
                        success = true;
                    }
                } finally {
                    if (success) {
                        disableIndexBlock("test", otherBlock.settingName());
                    }
                    disableIndexBlock("test", block.settingName());
                }
            }
        }
    }

    public void testAddBlockToMissingIndex() {
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareAddBlock(randomAddableBlock(), "test").get()
        );
        assertThat(e.getMessage(), is("no such index [test]"));
    }

    public void testAddBlockToOneMissingIndex() {
        createIndex("test1");
        final IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareAddBlock(randomAddableBlock(), "test1", "test2").get()
        );
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testCloseOneMissingIndexIgnoreMissing() throws Exception {
        createIndex("test1");
        final APIBlock block = randomAddableBlock();
        try {
            assertBusy(
                () -> assertAcked(
                    client().admin().indices().prepareAddBlock(block, "test1", "test2").setIndicesOptions(lenientExpandOpen())
                )
            );
            assertIndexHasBlock(block, "test1");
        } finally {
            disableIndexBlock("test1", block);
        }
    }

    public void testAddBlockNoIndex() {
        final ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().admin().indices().prepareAddBlock(randomAddableBlock()).get()
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testAddBlockNullIndex() {
        expectThrows(NullPointerException.class, () -> client().admin().indices().prepareAddBlock(randomAddableBlock(), (String[]) null));
    }

    public void testCannotAddReadOnlyAllowDeleteBlock() {
        createIndex("test1");
        final AddIndexBlockRequestBuilder request = client().admin().indices().prepareAddBlock(APIBlock.READ_ONLY_ALLOW_DELETE, "test1");
        final ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, request::get);
        assertThat(e.getMessage(), containsString("read_only_allow_delete block is for internal use only"));
    }

    public void testAddIndexBlock() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );

        final APIBlock block = randomAddableBlock();
        try {
            assertAcked(client().admin().indices().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }

        client().admin().indices().prepareRefresh(indexName).get();
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), nbDocs);
    }

    public void testSameBlockTwice() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        if (randomBoolean()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, randomIntBetween(1, 10))
                    .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                    .collect(toList())
            );
        }
        final APIBlock block = randomAddableBlock();
        try {
            assertAcked(client().admin().indices().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
            // Second add block should be acked too, even if it was a METADATA block
            assertAcked(client().admin().indices().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
    }

    public void testAddBlockToUnassignedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setWaitForActiveShards(ActiveShardCount.NONE)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", "nothing").build())
        );

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().indices().get(indexName).getState(), is(IndexMetadata.State.OPEN));
        assertThat(clusterState.routingTable().allShards().stream().allMatch(ShardRouting::unassigned), is(true));

        final APIBlock block = randomAddableBlock();
        try {
            assertAcked(client().admin().indices().prepareAddBlock(block, indexName));
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
    }

    public void testConcurrentAddBlock() throws InterruptedException {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(10, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );
        ensureYellowAndNoInitializingShards(indexName);

        final CountDownLatch startClosing = new CountDownLatch(1);
        final Thread[] threads = new Thread[randomIntBetween(2, 5)];

        final APIBlock block = randomAddableBlock();

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        startClosing.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    try {
                        client().admin().indices().prepareAddBlock(block, indexName).get();
                        assertIndexHasBlock(block, indexName);
                    } catch (final ClusterBlockException e) {
                        assertThat(e.blocks(), hasSize(1));
                        assertTrue(e.blocks().stream().allMatch(b -> b.id() == block.getBlock().id()));
                    }
                });
                threads[i].start();
            }

            startClosing.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
    }

    public void testAddBlockWhileIndexingDocuments() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final APIBlock block = randomAddableBlock();

        int nbDocs = 0;

        try {
            try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), 1000)) {
                indexer.setFailureAssertion(t -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(t);
                    assertThat(cause, instanceOf(ClusterBlockException.class));
                    ClusterBlockException e = (ClusterBlockException) cause;
                    assertThat(e.blocks(), hasSize(1));
                    assertTrue(e.blocks().stream().allMatch(b -> b.id() == block.getBlock().id()));
                });

                waitForDocs(randomIntBetween(10, 50), indexer);
                assertAcked(client().admin().indices().prepareAddBlock(block, indexName));
                indexer.stopAndAwaitStopped();
                nbDocs += indexer.totalIndexedDocs();
            }

            assertIndexHasBlock(block, indexName);
        } finally {
            disableIndexBlock(indexName, block);
        }
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE).get(), nbDocs);
    }

    public void testAddBlockWhileDeletingIndices() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 10)];
        for (int i = 0; i < indices.length; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName);
            if (randomBoolean()) {
                indexRandom(
                    randomBoolean(),
                    false,
                    randomBoolean(),
                    IntStream.range(0, 10)
                        .mapToObj(n -> client().prepareIndex(indexName).setId(String.valueOf(n)).setSource("num", n))
                        .collect(toList())
                );
            }
            indices[i] = indexName;
        }
        assertThat(client().admin().cluster().prepareState().get().getState().metadata().indices().size(), equalTo(indices.length));

        final List<Thread> threads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final APIBlock block = randomAddableBlock();

        Consumer<Exception> exceptionConsumer = t -> {
            Throwable cause = ExceptionsHelper.unwrapCause(t);
            if (cause instanceof ClusterBlockException) {
                ClusterBlockException e = (ClusterBlockException) cause;
                assertThat(e.blocks(), hasSize(1));
                assertTrue(e.blocks().stream().allMatch(b -> b.id() == block.getBlock().id()));
            } else {
                assertThat(cause, instanceOf(IndexNotFoundException.class));
            }
        };

        try {
            for (final String indexToDelete : indices) {
                threads.add(new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    int pendingRetries = 3;
                    boolean success = false;
                    while (success == false && pendingRetries-- > 0) {
                        try {
                            assertAcked(client().admin().indices().prepareDelete(indexToDelete));
                            success = true;
                        } catch (final Exception e) {
                            Throwable cause = ExceptionsHelper.unwrapCause(e);
                            if (cause instanceof ProcessClusterEventTimeoutException && pendingRetries > 0) {
                                // ignore error & retry
                                continue;
                            }
                            exceptionConsumer.accept(e);
                        }
                    }
                }));
            }
            for (final String indexToBlock : indices) {
                threads.add(new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    try {
                        client().admin().indices().prepareAddBlock(block, indexToBlock).get();
                    } catch (final Exception e) {
                        exceptionConsumer.accept(e);
                    }
                }));
            }

            for (Thread thread : threads) {
                thread.start();
            }
            latch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
        } finally {
            for (final String indexToBlock : indices) {
                try {
                    disableIndexBlock(indexToBlock, block);
                } catch (IndexNotFoundException infe) {
                    // ignore
                }
            }
        }
    }

    static void assertIndexHasBlock(APIBlock block, final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().indices().get(index);
            final Settings indexSettings = indexMetadata.getSettings();
            assertThat(indexSettings.hasValue(block.settingName()), is(true));
            assertThat(indexSettings.getAsBoolean(block.settingName(), false), is(true));
            assertThat(clusterState.blocks().hasIndexBlock(index, block.getBlock()), is(true));
            assertThat(
                "Index " + index + " must have only 1 block with [id=" + block.getBlock().id() + "]",
                clusterState.blocks()
                    .indices()
                    .getOrDefault(index, emptySet())
                    .stream()
                    .filter(clusterBlock -> clusterBlock.id() == block.getBlock().id())
                    .count(),
                equalTo(1L)
            );
        }
    }

    public static void disableIndexBlock(String index, APIBlock block) {
        disableIndexBlock(index, block.settingName());
    }

    /**
     * The read-only-allow-delete block cannot be added via the add index block API; this method chooses randomly from the values that
     * the add index block API does support.
     */
    private static APIBlock randomAddableBlock() {
        return randomValueOtherThan(APIBlock.READ_ONLY_ALLOW_DELETE, () -> randomFrom(APIBlock.values()));
    }
}
