/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.NativeStoreHandle;

import java.util.Map;
import java.util.Optional;

/**
 * Initialization parameters for creating an {@link org.opensearch.index.engine.exec.EngineReaderManager} via
 * {@link org.opensearch.plugins.SearchBackEndPlugin#createReaderManager}. Bundling parameters in a record
 * avoids breaking the plugin SPI when new context is needed.
 *
 * @param indexStoreProvider the store provider, or empty if not available
 * @param format the data format to create a reader manager for
 * @param registry the data format registry it can use to wire any data format specific details.
 * @param shardPath the shard path for file storage
 * @param dataformatAwareStoreHandles per-format native store handles for reads.
 *                                    Empty map if no native stores are available.
 *                                    Plugins should resolve their own handle via {@link #storeHandle()}
 *                                    rather than indexing this map directly.
 * @param indexSettings the index settings (carries {@code IndexSortConfig} so backends can declare
 *                      file sort order to their query optimizers).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record ReaderManagerConfig(Optional<IndexStoreProvider> indexStoreProvider, DataFormat format, DataFormatRegistry registry,
    ShardPath shardPath, Map<DataFormat, NativeStoreHandle> dataformatAwareStoreHandles, IndexSettings indexSettings) {

    /**
     * Resolves the native store handle for {@link #format()}, or {@code null} when this shard has no
     * native store for it (e.g. a hot-tier shard with no remote store, where callers fall back to
     * the local file system).
     *
     * <p>Resolved through {@link DataFormat#storageFormat()}, so an
     * {@linkplain AuxiliaryDataFormat auxiliary} format gets its <em>delegate's</em> handle. A handle
     * is a physical resource — it names a per-shard native file registry seeded from the store
     * strategy that owns the files — and a side table's files are, physically, the delegate's:
     * {@code StoreStrategy#owns} matches on the path prefix, and a child row's parquet file is at
     * {@code parquet/…}, so only the parquet strategy can ever claim it. Giving the side table a
     * strategy of its own would therefore produce a live-but-empty registry, which is worse than no
     * handle at all: the reader would take the with-store read path against a registry that has no
     * entry for the file, instead of falling back to the local file system.
     *
     * @return the handle backing this format's files, or null if none
     */
    public NativeStoreHandle storeHandle() {
        return dataformatAwareStoreHandles.get(format.storageFormat());
    }
}
