/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.ShardNativeStore;

import java.util.Optional;

/**
 * Initialization parameters for creating an {@link org.opensearch.index.engine.exec.EngineReaderManager} via
 * {@link org.opensearch.plugins.SearchBackEndPlugin#createReaderManager}. Bundling parameters in a record
 * avoids breaking the plugin SPI when new context is needed.
 *
 * @param indexStoreProvider the store provider, or empty if not available
 * @param format the data format to create a reader manager for
 * @param shardPath the shard path for file storage
 * @param shardNativeStore the shard-scoped native object store, or {@link ShardNativeStore#EMPTY}
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record ReaderManagerConfig(Optional<IndexStoreProvider> indexStoreProvider, DataFormat format, ShardPath shardPath,
    ShardNativeStore shardNativeStore) {
}
