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

package org.opensearch.plugins;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.node.MockNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class IndexStorePluginTests extends OpenSearchTestCase {

    public static class BarStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store", new FsDirectoryFactory());
        }

    }

    public static class FooStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store", new FsDirectoryFactory());
        }

    }

    public static class ConflictingStorePlugin extends Plugin implements IndexStorePlugin {

        public static final String TYPE;

        static {
            TYPE = randomFrom(Arrays.asList(IndexModule.Type.values())).getSettingsKey();
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap(TYPE, new FsDirectoryFactory());
        }

    }

    public static class FooCustomRecoveryStore extends Plugin implements IndexStorePlugin {
        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store-a", new FsDirectoryFactory());
        }

        @Override
        public Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
            return Collections.singletonMap("recovery-type", new RecoveryFactory());
        }
    }

    public static class BarCustomRecoveryStore extends Plugin implements IndexStorePlugin {
        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store-b", new FsDirectoryFactory());
        }

        @Override
        public Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
            return Collections.singletonMap("recovery-type", new RecoveryFactory());
        }
    }

    public static class RecoveryFactory implements IndexStorePlugin.RecoveryStateFactory {
        @Override
        public RecoveryState newRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, DiscoveryNode sourceNode) {
            return new RecoveryState(shardRouting, targetNode, sourceNode);
        }
    }

    public static class FooStoreFactoryPlugin extends Plugin implements IndexStorePlugin {
        @Override
        public Map<String, StoreFactory> getStoreFactories() {
            return Collections.singletonMap("foo_store", new TestStoreFactory());
        }
    }

    public static class TestStoreFactory implements IndexStorePlugin.StoreFactory {
        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            org.apache.lucene.store.Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath,
            IndexStorePlugin.DirectoryFactory directoryFactory
        ) throws IOException {
            return new Store(shardId, indexSettings, directory, shardLock, onClose, shardPath, directoryFactory);
        }

        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            org.apache.lucene.store.Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath
        ) throws IOException {
            return new Store(shardId, indexSettings, directory, shardLock, onClose, shardPath);
        }
    }

    public void testIndexStoreFactoryConflictsWithBuiltInIndexStoreType() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new MockNode(settings, Collections.singletonList(ConflictingStorePlugin.class))
        );
        assertThat(
            e,
            hasToString(containsString("registered index store type [" + ConflictingStorePlugin.TYPE + "] conflicts with a built-in type"))
        );
    }

    public void testDuplicateIndexStoreFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new MockNode(settings, Arrays.asList(BarStorePlugin.class, FooStorePlugin.class))
        );
        assertThat(
            e,
            hasToString(
                matches(
                    "java.lang.IllegalStateException: Duplicate key store \\(attempted merging values "
                        + "org.opensearch.index.store.FsDirectoryFactory@[\\w\\d]+ "
                        + "and org.opensearch.index.store.FsDirectoryFactory@[\\w\\d]+\\)"
                )
            )
        );
    }

    public void testDuplicateIndexStoreRecoveryStateFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new MockNode(settings, Arrays.asList(FooCustomRecoveryStore.class, BarCustomRecoveryStore.class))
        );
        assertThat(e.getMessage(), containsString("Duplicate key recovery-type"));
    }
}
