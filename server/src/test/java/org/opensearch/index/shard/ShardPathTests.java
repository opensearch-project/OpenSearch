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

package org.opensearch.index.shard;

import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.opensearch.env.Environment.PATH_SHARED_DATA_SETTING;

public class ShardPathTests extends OpenSearchTestCase {
    public void testLoadShardPath() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            writeShardStateMetadata("0xDEADBEEF", path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, "");
            assertEquals(path, shardPath.getDataPath());
            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
            assertEquals(path.resolve("translog"), shardPath.resolveTranslog());
            assertEquals(path.resolve("index"), shardPath.resolveIndex());
        }
    }

    public void testFailLoadShardPathOnMultiState() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            final String indexUUID = "0xDEADBEEF";
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path[] paths = env.availableShardPaths(shardId);
            assumeTrue("This test tests multi data.path but we only got one", paths.length > 1);
            writeShardStateMetadata(indexUUID, paths);
            Exception e = expectThrows(IllegalStateException.class, () -> ShardPath.loadShardPath(logger, env, shardId, ""));
            assertThat(e.getMessage(), containsString("more than one shard state found"));
        }
    }

    public void testFailLoadShardPathIndexUUIDMissmatch() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment(Settings.builder().build())) {
            ShardId shardId = new ShardId("foo", "foobar", 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            writeShardStateMetadata("0xDEADBEEF", path);
            Exception e = expectThrows(IllegalStateException.class, () -> ShardPath.loadShardPath(logger, env, shardId, ""));
            assertThat(e.getMessage(), containsString("expected: foobar on shard path"));
        }
    }

    public void testIllegalCustomDataPath() {
        Index index = new Index("foo", "foo");
        final Path path = createTempDir().resolve(index.getUUID()).resolve("0");
        Exception e = expectThrows(IllegalArgumentException.class, () -> new ShardPath(true, path, path, new ShardId(index, 0)));
        assertThat(e.getMessage(), is("shard state path must be different to the data path when using custom data paths"));
    }

    public void testValidCtor() {
        Index index = new Index("foo", "foo");
        final Path path = createTempDir().resolve(index.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, path, path, new ShardId(index, 0));
        assertFalse(shardPath.isCustomDataPath());
        assertEquals(shardPath.getDataPath(), path);
        assertEquals(shardPath.getShardStatePath(), path);
    }

    public void testGetRootPaths() throws IOException {
        boolean useCustomDataPath = randomBoolean();
        final Settings nodeSettings;
        final String indexUUID = "0xDEADBEEF";
        final Path customPath;
        final String customDataPath;
        if (useCustomDataPath) {
            final Path path = createTempDir();
            customDataPath = "custom";
            nodeSettings = Settings.builder().put(PATH_SHARED_DATA_SETTING.getKey(), path.toAbsolutePath().toAbsolutePath()).build();
            customPath = path.resolve("custom").resolve("0");
        } else {
            customPath = null;
            customDataPath = "";
            nodeSettings = Settings.EMPTY;
        }
        try (NodeEnvironment env = newNodeEnvironment(nodeSettings)) {
            ShardId shardId = new ShardId("foo", indexUUID, 0);
            Path[] paths = env.availableShardPaths(shardId);
            Path path = randomFrom(paths);
            writeShardStateMetadata(indexUUID, path);
            ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, customDataPath);
            boolean found = false;
            for (Path p : env.nodeDataPaths()) {
                if (p.equals(shardPath.getRootStatePath())) {
                    found = true;
                    break;
                }
            }
            assertTrue("root state paths must be a node path but wasn't: " + shardPath.getRootStatePath(), found);
            found = false;
            if (useCustomDataPath) {
                assertNotEquals(shardPath.getRootDataPath(), shardPath.getRootStatePath());
                assertEquals(customPath, shardPath.getRootDataPath());
            } else {
                assertNull(customPath);
                for (Path p : env.nodeDataPaths()) {
                    if (p.equals(shardPath.getRootDataPath())) {
                        found = true;
                        break;
                    }
                }
                assertTrue("root state paths must be a node path but wasn't: " + shardPath.getRootDataPath(), found);
            }
        }
    }

    public void testLoadFileCachePath() throws IOException {
        Settings searchNodeSettings = Settings.builder().put("node.roles", "search").build();

        try (NodeEnvironment env = newNodeEnvironment(searchNodeSettings)) {
            ShardId shardId = new ShardId("foo", "0xDEADBEEF", 0);
            Path fileCachePath = env.fileCacheNodePath().fileCachePath;
            writeShardStateMetadata("0xDEADBEEF", fileCachePath);
            ShardPath shardPath = ShardPath.loadFileCachePath(env, shardId);

            assertTrue(shardPath.getDataPath().startsWith(fileCachePath));
            assertFalse(shardPath.getShardStatePath().startsWith(fileCachePath));

            assertEquals("0xDEADBEEF", shardPath.getShardId().getIndex().getUUID());
            assertEquals("foo", shardPath.getShardId().getIndexName());
        }
    }

    private static void writeShardStateMetadata(String indexUUID, Path... paths) throws WriteStateException {
        ShardStateMetadata.FORMAT.writeAndCleanup(
            new ShardStateMetadata(true, indexUUID, AllocationId.newInitializing(), ShardStateMetadata.IndexDataLocation.LOCAL),
            paths
        );
    }
}
