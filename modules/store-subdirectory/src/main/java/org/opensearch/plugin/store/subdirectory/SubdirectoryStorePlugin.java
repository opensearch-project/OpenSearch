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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SubdirectoryStorePlugin extends Plugin implements IndexStorePlugin {
    /**
     * Creates a new SubdirectoryStorePlugin instance.
     */
    public SubdirectoryStorePlugin() {
        // Default constructor
    }

    @Override
    public Map<String, StoreFactory> getStoreFactories() {
        Map<String, StoreFactory> map = new HashMap<>();
        map.put("subdirectory_store", new SubdirectoryStoreFactory());
        return Collections.unmodifiableMap(map);
    }

    static class SubdirectoryStoreFactory implements StoreFactory {
        @Override
        public Store newStore(
            ShardId shardId,
            IndexSettings indexSettings,
            Directory directory,
            ShardLock shardLock,
            Store.OnClose onClose,
            ShardPath shardPath
        ) throws IOException {
            return new SubdirectoryAwareStore(shardId, indexSettings, directory, shardLock, onClose, shardPath);
        }
    }
}
