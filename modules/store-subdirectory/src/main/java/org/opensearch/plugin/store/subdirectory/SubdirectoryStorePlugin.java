/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.subdirectory;

import org.apache.lucene.store.Directory;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenSearch plugin that provides subdirectory-aware store functionality.
 *
 * This plugin enables OpenSearch to work with stores that organize files
 * in subdirectories within shard data paths. It registers a custom store
 * factory that creates {@link SubdirectoryAwareStore} instances capable
 * of handling nested directory structures during regular operations and
 * peer recovery.
 */
public class SubdirectoryStorePlugin extends Plugin implements IndexStorePlugin {
    /**
     * Creates a new SubdirectoryStorePlugin instance.
     */
    public SubdirectoryStorePlugin() {
        // Default constructor
    }

    /**
     * Returns the store factories provided by this plugin.
     *
     * @return A map containing the "subdirectory_store" factory that creates
     *         {@link SubdirectoryAwareStore} instances
     */
    @Override
    public Map<String, StoreFactory> getStoreFactories() {
        Map<String, StoreFactory> map = new HashMap<>();
        map.put("subdirectory_store", new SubdirectoryStoreFactory());
        return Collections.unmodifiableMap(map);
    }

    /**
     * Factory for creating {@link SubdirectoryAwareStore} instances.
     *
     * This factory creates stores that can handle files organized in
     * subdirectories within shard data paths, with support for
     * peer recovery operations.
     */
    static class SubdirectoryStoreFactory implements StoreFactory {
        /**
         * Creates a new {@link SubdirectoryAwareStore} instance.
         *
         * @param shardId the shard identifier
         * @param indexSettings the index settings
         * @param directory the underlying Lucene directory
         * @param shardLock the shard lock
         * @param onClose callback to execute when the store is closed
         * @param shardPath the path information for the shard
         * @return a new SubdirectoryAwareStore instance
         */
        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath
        ) {
            return new SubdirectoryAwareStore(shardId, indexSettings, directory, shardLock, onClose, shardPath);
        }

        /**
         * Creates a new {@link SubdirectoryAwareStore} instance.
         *
         * @param shardId the shard identifier
         * @param indexSettings the index settings
         * @param directory the underlying Lucene directory
         * @param shardLock the shard lock
         * @param onClose callback to execute when the store is closed
         * @param shardPath the path information for the shard
         * @param directoryFactory the directory factory to create child level directory.
         *                         Used for Context Aware Segments enabled indices.
         * @return a new SubdirectoryAwareStore instance
         */
        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath,
            DirectoryFactory directoryFactory
        ) {
            return new SubdirectoryAwareStore(shardId, indexSettings, directory, shardLock, onClose, shardPath, directoryFactory);
        }
    }
}
