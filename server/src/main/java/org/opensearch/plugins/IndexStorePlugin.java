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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.IndexStoreListener;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A plugin that provides alternative directory implementations.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface IndexStorePlugin {

    /**
     * An interface that describes how to create a new directory instance per shard.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    interface DirectoryFactory {
        /**
         * Creates a new directory per shard. This method is called once per shard on shard creation.
         * @param indexSettings the shards index settings
         * @param shardPath the path the shard is using
         * @return a new lucene directory instance
         * @throws IOException if an IOException occurs while opening the directory
         */
        Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException;

        Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException;
    }

    /**
     * The {@link DirectoryFactory} mappings for this plugin. When an index is created the store type setting
     * {@link org.opensearch.index.IndexModule#INDEX_STORE_TYPE_SETTING} on the index will be examined and either use the default or a
     * built-in type, or looked up among all the directory factories from {@link IndexStorePlugin} plugins.
     *
     * @return a map from store type to an directory factory
     */
    default Map<String, DirectoryFactory> getDirectoryFactories() {
        return Collections.emptyMap();
    }

    /**
     * An interface that describes how to create a new composite directory instance per shard.
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @ExperimentalApi
    interface CompositeDirectoryFactory {
        /**
         * Creates a new composite directory per shard
         * @param indexSettings the shards index settings
         * @param shardPath the path the shard is using
         * @return a new composite directory instance
         * @throws IOException if an IOException occurs while opening the directory
         */
        Directory newDirectory(
            IndexSettings indexSettings,
            ShardPath shardPath,
            DirectoryFactory localDirectoryFactory,
            Directory remoteDirectory,
            FileCache fileCache,
            ThreadPool threadPool
        ) throws IOException;
    }

    /**
     * The {@link CompositeDirectoryFactory} mappings for this plugin. When an index is created the composite store type setting
     * {@link org.opensearch.index.IndexModule#INDEX_COMPOSITE_STORE_TYPE_SETTING} on the index will be examined and either use the default or a
     * built-in type, or looked up among all the composite directory factories from {@link IndexStorePlugin} plugins.
     *
     * @return a map from composite store type to a composite directory factory
     */
    default Map<String, CompositeDirectoryFactory> getCompositeDirectoryFactories() {
        return Collections.emptyMap();
    }

    /**
     * An interface that allows to create a new {@link RecoveryState} per shard.
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @PublicApi(since = "1.0.0")
    interface RecoveryStateFactory {
        /**
         * Creates a new {@link RecoveryState} per shard. This method is called once per shard on shard creation.
         * @return a new RecoveryState instance
         */
        RecoveryState newRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode);
    }

    /**
     * The {@link RecoveryStateFactory} mappings for this plugin. When an index is created the recovery type setting
     * {@link org.opensearch.index.IndexModule#INDEX_RECOVERY_TYPE_SETTING} on the index will be examined and either use the default
     * or looked up among all the recovery state factories from {@link IndexStorePlugin} plugins.
     *
     * @return a map from recovery type to an recovery state factory
     */
    default Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
        return Collections.emptyMap();
    }

    /**
     * The {@link IndexStoreListener}s for this plugin which are triggered upon shard/index path deletion
     */
    default Optional<IndexStoreListener> getIndexStoreListener() {
        return Optional.empty();
    }

    /**
     * An interface that describes how to create a new Store instance per shard.
     *
     * @opensearch.api
     */
    @ExperimentalApi
    interface StoreFactory {
        /**
         * Creates a new Store per shard. This method is called once per shard on shard creation.
         * @param shardId the shard id
         * @param indexSettings the shard's index settings
         * @param directory the Lucene directory selected for this shard
         * @param shardLock the shard lock to associate with the store
         * @param onClose listener invoked on store close
         * @param shardPath the shard path
         * @return a new Store instance
         */
        Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath
        ) throws IOException;

        /**
         * Creates a new Store per shard. This method is called once per shard on shard creation.
         * @param shardId the shard id
         * @param indexSettings the shard's index settings
         * @param directory the Lucene directory selected for this shard
         * @param shardLock the shard lock to associate with the store
         * @param onClose listener invoked on store close
         * @param shardPath the shard path
         * @param directoryFactory the directory path.
         * @return a new Store instance
         */
        Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath,
            DirectoryFactory directoryFactory
        ) throws IOException;
    }

    /**
     * The {@link StoreFactory} mappings for this plugin. When an index is created a custom store factory can be selected via
     * {@code index.store.factory}. If not set, the default store is used.
     *
     * @return a map from store type key to a store factory
     */
    @ExperimentalApi
    default Map<String, StoreFactory> getStoreFactories() {
        return Collections.emptyMap();
    }
}
