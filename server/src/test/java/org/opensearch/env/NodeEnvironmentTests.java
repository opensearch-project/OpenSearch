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

package org.opensearch.env;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.IndexStoreListener;
import org.opensearch.node.Node;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.NodeRoles;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.test.NodeRoles.nonClusterManagerNode;
import static org.opensearch.test.NodeRoles.nonDataNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // TODO: fix test to allow extras
public class NodeEnvironmentTests extends OpenSearchTestCase {
    private final IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", Settings.EMPTY);

    public void testNodeLockSillySettings() {
        try {
            NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(
                Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), between(Integer.MIN_VALUE, 0)).build()
            );
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("must be >= 1"));
        }

        // Even though its silly MAXINT nodes is a-ok!
        int value = between(1, Integer.MAX_VALUE);
        int max = NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(
            Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), value).build()
        );
        assertEquals(value, max);
        assertWarnings(
            "[node.max_local_storage_nodes] setting was deprecated in OpenSearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testNodeLockSingleEnvironment() throws IOException {
        final Settings settings = buildEnvSettings(Settings.builder().put("node.max_local_storage_nodes", 1).build());
        NodeEnvironment env = newNodeEnvironment(settings);
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);

        // Reuse the same location and attempt to lock again
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))
        );
        assertThat(ex.getMessage(), containsString("failed to obtain node lock"));

        // Close the environment that holds the lock and make sure we can get the lock after release
        env.close();
        env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        assertThat(env.nodeDataPaths(), arrayWithSize(dataPaths.size()));

        for (int i = 0; i < dataPaths.size(); i++) {
            assertTrue(env.nodeDataPaths()[i].startsWith(PathUtils.get(dataPaths.get(i))));
        }
        env.close();
        assertThat(env.lockedShards(), empty());
        assertWarnings(
            "[node.max_local_storage_nodes] setting was deprecated in OpenSearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    @SuppressForbidden(reason = "System.out.*")
    public void testSegmentInfosTracing() {
        // Defaults to not hooking up std out
        assertNull(SegmentInfos.getInfoStream());

        try {
            // False means don't hook up std out
            NodeEnvironment.applySegmentInfosTrace(
                Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), false).build()
            );
            assertNull(SegmentInfos.getInfoStream());

            // But true means hook std out up statically
            NodeEnvironment.applySegmentInfosTrace(
                Settings.builder().put(NodeEnvironment.ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.getKey(), true).build()
            );
            assertEquals(System.out, SegmentInfos.getInfoStream());
        } finally {
            // Clean up after ourselves
            SegmentInfos.setInfoStream(null);
        }
    }

    public void testNodeLockMultipleEnvironment() throws IOException {
        final Settings settings = buildEnvSettings(Settings.builder().put("node.max_local_storage_nodes", 2).build());
        final NodeEnvironment first = newNodeEnvironment(settings);
        List<String> dataPaths = Environment.PATH_DATA_SETTING.get(settings);
        NodeEnvironment second = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        assertEquals(first.nodeDataPaths().length, dataPaths.size());
        assertEquals(second.nodeDataPaths().length, dataPaths.size());
        for (int i = 0; i < dataPaths.size(); i++) {
            assertEquals(first.nodeDataPaths()[i].getParent(), second.nodeDataPaths()[i].getParent());
        }
        IOUtils.close(first, second);
        assertWarnings(
            "[node.max_local_storage_nodes] setting was deprecated in OpenSearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testShardLock() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();

        Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0), "1");
        assertEquals(new ShardId(index, 0), fooLock.getShardId());

        try {
            env.shardLock(new ShardId(index, 0), "2");
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }
        try {
            env.lockAllForIndex(index, idxSettings, "3", randomIntBetween(0, 10));
            fail("shard 0 is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        fooLock.close();
        // can lock again?
        env.shardLock(new ShardId(index, 0), "4").close();

        List<ShardLock> locks = env.lockAllForIndex(index, idxSettings, "5", randomIntBetween(0, 10));
        try {
            env.shardLock(new ShardId(index, 0), "6");
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        IOUtils.close(locks);
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testAvailableIndexFolders() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Set<String> actualPaths = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
        }

        assertThat(actualPaths, equalTo(env.availableIndexFolders()));
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testAvailableIndexFoldersWithExclusions() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Set<String> excludedPaths = new HashSet<>();
        Set<String> actualPaths = new HashSet<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
                actualPaths.add(path.getFileName().toString());
            }
            if (randomBoolean()) {
                excludedPaths.add(env.indexPaths(index)[0].getFileName().toString());
            }
        }

        assertThat(Sets.difference(actualPaths, excludedPaths), equalTo(env.availableIndexFolders(excludedPaths::contains)));
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testResolveIndexFolders() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final int numIndices = randomIntBetween(1, 10);
        Map<String, List<Path>> actualIndexDataPaths = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            Index index = new Index("foo" + i, "fooUUID" + i);
            Path[] indexPaths = env.indexPaths(index);
            for (Path path : indexPaths) {
                Files.createDirectories(path);
                String fileName = path.getFileName().toString();
                List<Path> paths = actualIndexDataPaths.get(fileName);
                if (paths == null) {
                    paths = new ArrayList<>();
                }
                paths.add(path);
                actualIndexDataPaths.put(fileName, paths);
            }
        }
        for (Map.Entry<String, List<Path>> actualIndexDataPathEntry : actualIndexDataPaths.entrySet()) {
            List<Path> actual = actualIndexDataPathEntry.getValue();
            Path[] actualPaths = actual.toArray(new Path[0]);
            assertThat(actualPaths, equalTo(env.resolveIndexFolder(actualIndexDataPathEntry.getKey())));
        }
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testDeleteSafe() throws Exception {
        final NodeEnvironment env = newNodeEnvironment();
        final Index index = new Index("foo", "fooUUID");
        ShardLock fooLock = env.shardLock(new ShardId(index, 0), "1");
        assertEquals(new ShardId(index, 0), fooLock.getShardId());

        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
            Files.createDirectories(path.resolve("1"));
        }

        try {
            env.deleteShardDirectorySafe(new ShardId(index, 0), idxSettings);
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
            assertTrue(Files.exists(path.resolve("1")));
        }

        env.deleteShardDirectorySafe(new ShardId(index, 1), idxSettings);

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
            assertFalse(Files.exists(path.resolve("1")));
        }

        try {
            env.deleteIndexDirectorySafe(index, randomIntBetween(0, 10), idxSettings);
            fail("shard is locked");
        } catch (ShardLockObtainFailedException ex) {
            // expected
        }
        fooLock.close();

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path));
        }

        final AtomicReference<Throwable> threadException = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch start = new CountDownLatch(1);
        if (randomBoolean()) {
            Thread t = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error", e);
                    threadException.set(e);
                    latch.countDown();
                    blockLatch.countDown();
                }

                @Override
                protected void doRun() throws Exception {
                    start.await();
                    try (ShardLock autoCloses = env.shardLock(new ShardId(index, 0), "2")) {
                        blockLatch.countDown();
                        Thread.sleep(randomIntBetween(1, 10));
                    }
                    latch.countDown();
                }
            });
            t.start();
        } else {
            latch.countDown();
            blockLatch.countDown();
        }
        start.countDown();
        blockLatch.await();

        env.deleteIndexDirectorySafe(index, 5000, idxSettings);

        assertNull(threadException.get());

        for (Path path : env.indexPaths(index)) {
            assertFalse(Files.exists(path));
        }
        latch.await();
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testIndexStoreListener() throws Exception {
        final AtomicInteger shardCounter1 = new AtomicInteger(0);
        final AtomicInteger shardCounter2 = new AtomicInteger(0);
        final AtomicInteger indexCounter1 = new AtomicInteger(0);
        final AtomicInteger indexCounter2 = new AtomicInteger(0);
        final Index index = new Index("foo", "fooUUID");
        final ShardId shardId = new ShardId(index, 0);
        final IndexStoreListener listener1 = new IndexStoreListener() {
            @Override
            public void beforeShardPathDeleted(ShardId inShardId, IndexSettings indexSettings, NodeEnvironment env) {
                assertEquals(shardId, inShardId);
                shardCounter1.incrementAndGet();
            }

            @Override
            public void beforeIndexPathDeleted(Index inIndex, IndexSettings indexSettings, NodeEnvironment env) {
                assertEquals(index, inIndex);
                indexCounter1.incrementAndGet();
            }
        };
        final IndexStoreListener listener2 = new IndexStoreListener() {
            @Override
            public void beforeShardPathDeleted(ShardId inShardId, IndexSettings indexSettings, NodeEnvironment env) {
                assertEquals(shardId, inShardId);
                shardCounter2.incrementAndGet();
            }

            @Override
            public void beforeIndexPathDeleted(Index inIndex, IndexSettings indexSettings, NodeEnvironment env) {
                assertEquals(index, inIndex);
                indexCounter2.incrementAndGet();
            }
        };
        final NodeEnvironment env = newNodeEnvironment(new IndexStoreListener.CompositeIndexStoreListener(List.of(listener1, listener2)));

        for (Path path : env.indexPaths(index)) {
            Files.createDirectories(path.resolve("0"));
        }

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path.resolve("0")));
        }
        assertEquals(0, shardCounter1.get());
        assertEquals(0, shardCounter2.get());

        env.deleteShardDirectorySafe(new ShardId(index, 0), idxSettings);

        for (Path path : env.indexPaths(index)) {
            assertFalse(Files.exists(path.resolve("0")));
        }
        assertEquals(1, shardCounter1.get());
        assertEquals(1, shardCounter2.get());

        for (Path path : env.indexPaths(index)) {
            assertTrue(Files.exists(path));
        }
        assertEquals(0, indexCounter1.get());
        assertEquals(0, indexCounter2.get());

        env.deleteIndexDirectorySafe(index, 5000, idxSettings);

        for (Path path : env.indexPaths(index)) {
            assertFalse(Files.exists(path));
        }
        assertEquals(1, indexCounter1.get());
        assertEquals(1, indexCounter2.get());
        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        env.close();
    }

    public void testStressShardLock() throws IOException, InterruptedException {
        class Int {
            int value = 0;
        }
        final NodeEnvironment env = newNodeEnvironment();
        final int shards = randomIntBetween(2, 10);
        final Int[] counts = new Int[shards];
        final AtomicInteger[] countsAtomic = new AtomicInteger[shards];
        final AtomicInteger[] flipFlop = new AtomicInteger[shards];

        for (int i = 0; i < counts.length; i++) {
            counts[i] = new Int();
            countsAtomic[i] = new AtomicInteger();
            flipFlop[i] = new AtomicInteger();
        }

        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final CountDownLatch latch = new CountDownLatch(1);
        final int iters = scaledRandomIntBetween(10000, 100000);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    }
                    for (int i = 0; i < iters; i++) {
                        int shard = randomIntBetween(0, counts.length - 1);
                        try {
                            try (
                                ShardLock autoCloses = env.shardLock(
                                    new ShardId("foo", "fooUUID", shard),
                                    "1",
                                    scaledRandomIntBetween(0, 10)
                                )
                            ) {
                                counts[shard].value++;
                                countsAtomic[shard].incrementAndGet();
                                assertEquals(flipFlop[shard].incrementAndGet(), 1);
                                assertEquals(flipFlop[shard].decrementAndGet(), 0);
                            }
                        } catch (ShardLockObtainFailedException ex) {
                            // ok
                        }
                    }
                }
            };
            threads[i].start();
        }
        latch.countDown(); // fire the threads up
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        assertTrue("LockedShards: " + env.lockedShards(), env.lockedShards().isEmpty());
        for (int i = 0; i < counts.length; i++) {
            assertTrue(counts[i].value > 0);
            assertEquals(flipFlop[i].get(), 0);
            assertEquals(counts[i].value, countsAtomic[i].get());
        }
        env.close();
    }

    public void testCustomDataPaths() throws Exception {
        String[] dataPaths = tmpPaths();
        NodeEnvironment env = newNodeEnvironment(dataPaths, "/tmp", Settings.EMPTY);

        Index index = new Index("myindex", "myindexUUID");
        ShardId sid = new ShardId(index, 0);

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(
            env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath(),
            equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath())
        );

        assertThat(
            "shard paths with a custom data_path should contain only regular paths",
            env.availableShardPaths(sid),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0"))
        );

        assertThat(
            "index paths uses the regular template",
            env.indexPaths(index),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID()))
        );

        assertThat(env.availableShardPaths(sid), equalTo(env.availableShardPaths(sid)));
        assertThat(
            env.resolveCustomLocation("/tmp/foo", sid).toAbsolutePath(),
            equalTo(PathUtils.get("/tmp/foo/0/" + index.getUUID() + "/0").toAbsolutePath())
        );

        assertThat(
            "shard paths with a custom data_path should contain only regular paths",
            env.availableShardPaths(sid),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID() + "/0"))
        );

        assertThat(
            "index paths uses the regular template",
            env.indexPaths(index),
            equalTo(stringsToPaths(dataPaths, "nodes/0/indices/" + index.getUUID()))
        );

        env.close();
    }

    public void testNodeIdNotPersistedAtInitialization() throws IOException {
        NodeEnvironment env = newNodeEnvironment(
            new String[0],
            nonClusterManagerNode(nonDataNode(Settings.builder().put("node.local_storage", false).build()))
        );
        String nodeID = env.nodeId();
        env.close();
        final String[] paths = tmpPaths();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat("previous node didn't have local storage enabled, id should change", env.nodeId(), not(equalTo(nodeID)));
        nodeID = env.nodeId();
        env.close();
        env = newNodeEnvironment(paths, Settings.EMPTY);
        assertThat(env.nodeId(), not(equalTo(nodeID)));
        env.close();
        env = newNodeEnvironment(Settings.EMPTY);
        assertThat(env.nodeId(), not(equalTo(nodeID)));
        env.close();
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { Node.NODE_LOCAL_STORAGE_SETTING });
    }

    public void testExistingTempFiles() throws IOException {
        String[] paths = tmpPaths();
        // simulate some previous left over temp files
        for (String path : randomSubsetOf(randomIntBetween(1, paths.length), paths)) {
            final Path nodePath = NodeEnvironment.resolveNodePath(PathUtils.get(path), 0);
            Files.createDirectories(nodePath);
            Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME));
            if (randomBoolean()) {
                Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".tmp"));
            }
            if (randomBoolean()) {
                Files.createFile(nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".final"));
            }
        }
        NodeEnvironment env = newNodeEnvironment(paths, Settings.EMPTY);
        env.close();

        // check we clean up
        for (String path : paths) {
            final Path nodePath = NodeEnvironment.resolveNodePath(PathUtils.get(path), 0);
            final Path tempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME);
            assertFalse(tempFile + " should have been cleaned", Files.exists(tempFile));
            final Path srcTempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".src");
            assertFalse(srcTempFile + " should have been cleaned", Files.exists(srcTempFile));
            final Path targetTempFile = nodePath.resolve(NodeEnvironment.TEMP_FILE_NAME + ".target");
            assertFalse(targetTempFile + " should have been cleaned", Files.exists(targetTempFile));
        }
    }

    public void testEnsureNoShardDataOrIndexMetadata() throws IOException {
        Settings settings = buildEnvSettings(Settings.EMPTY);
        Index index = new Index("test", "testUUID");

        // build settings using same path.data as original but without data and cluster-manager roles
        Settings noDataNoClusterManagerSettings = Settings.builder()
            .put(settings)
            .put(
                NodeRoles.removeRoles(
                    settings,
                    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)))
                )
            )
            .build();

        // test that we can create data=false and cluster_manager=false with no meta information
        newNodeEnvironment(noDataNoClusterManagerSettings).close();

        Path indexPath;
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
            }
            indexPath = env.indexPaths(index)[0];
        }

        verifyFailsOnMetadata(noDataNoClusterManagerSettings, indexPath);

        // build settings using same path.data as original but without data role
        Settings noDataSettings = nonDataNode(settings);

        String shardDataDirName = Integer.toString(randomInt(10));

        // test that we can create data=false env with only meta information. Also create shard data for following asserts
        try (NodeEnvironment env = newNodeEnvironment(noDataSettings)) {
            for (Path path : env.indexPaths(index)) {
                Files.createDirectories(path.resolve(shardDataDirName));
            }
        }

        verifyFailsOnShardData(noDataSettings, indexPath, shardDataDirName);

        // assert that we get the stricter message on meta-data when both conditions fail
        verifyFailsOnMetadata(noDataNoClusterManagerSettings, indexPath);

        // build settings using same path.data as original but without cluster-manager role
        Settings noClusterManagerSettings = nonClusterManagerNode(settings);

        // test that we can create cluster_manager=false env regardless of data.
        newNodeEnvironment(noClusterManagerSettings).close();

        // test that we can create data=true, cluster_manager=true env. Also remove state dir to leave only shard data for following asserts
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            for (Path path : env.indexPaths(index)) {
                Files.delete(path.resolve(MetadataStateFormat.STATE_DIR_NAME));
            }
        }

        // assert that we fail on shard data even without the metadata dir.
        verifyFailsOnShardData(noDataSettings, indexPath, shardDataDirName);
        verifyFailsOnShardData(noDataNoClusterManagerSettings, indexPath, shardDataDirName);
    }

    private void verifyFailsOnShardData(Settings settings, Path indexPath, String shardDataDirName) {
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            "Must fail creating NodeEnvironment on a data path that has shard data if node does not have data role",
            () -> newNodeEnvironment(settings).close()
        );

        assertThat(ex.getMessage(), containsString(indexPath.resolve(shardDataDirName).toAbsolutePath().toString()));
        assertThat(ex.getMessage(), startsWith("node does not have the data role but has shard data"));
    }

    private void verifyFailsOnMetadata(Settings settings, Path indexPath) {
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            "Must fail creating NodeEnvironment on a data path that has index metadata if node does not have data and cluster-manager roles",
            () -> newNodeEnvironment(settings).close()
        );

        assertThat(ex.getMessage(), containsString(indexPath.resolve(MetadataStateFormat.STATE_DIR_NAME).toAbsolutePath().toString()));
        assertThat(ex.getMessage(), startsWith("node does not have the data and cluster_manager roles but has index metadata"));
    }

    /** Converts an array of Strings to an array of Paths, adding an additional child if specified */
    private Path[] stringsToPaths(String[] strings, String additional) {
        Path[] locations = new Path[strings.length];
        for (int i = 0; i < strings.length; i++) {
            locations[i] = PathUtils.get(strings[i], additional);
        }
        return locations;
    }

    @Override
    public String[] tmpPaths() {
        final int numPaths = randomIntBetween(1, 3);
        final String[] absPaths = new String[numPaths];
        for (int i = 0; i < numPaths; i++) {
            absPaths[i] = createTempDir().toAbsolutePath().toString();
        }
        return absPaths;
    }

    @Override
    public NodeEnvironment newNodeEnvironment() throws IOException {
        return newNodeEnvironment(Settings.EMPTY);
    }

    public NodeEnvironment newNodeEnvironment(IndexStoreListener listener) throws IOException {
        Settings build = buildEnvSettings(Settings.EMPTY);
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build), listener);
    }

    @Override
    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = buildEnvSettings(settings);
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings)
            .build();
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, Settings settings) throws IOException {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths)
            .build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public NodeEnvironment newNodeEnvironment(String[] dataPaths, String sharedDataPath, Settings settings) throws IOException {
        Settings build = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), sharedDataPath)
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths)
            .build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }
}
