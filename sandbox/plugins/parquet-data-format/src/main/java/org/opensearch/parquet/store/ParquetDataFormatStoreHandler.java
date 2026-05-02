/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandler;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;

import java.io.IOException;
import java.util.Map;

/**
 * Per-shard native file registry for parquet files.
 *
 * <p>Owns a Rust {@code TieredObjectStore} via FFM. All calls delegate to
 * {@link TieredStorageBridge} which invokes the Rust {@code ts_*} functions:
 * <ul>
 *   <li>{@code seed} → {@code ts_register_files} (batch, per-file location)</li>
 *   <li>{@code onUploaded} → {@code ts_register_files} (single file, REMOTE)</li>
 *   <li>{@code onRemoved} → {@code ts_remove_file}</li>
 *   <li>{@code close} → {@code ts_destroy_tiered_object_store}</li>
 * </ul>
 *
 * <p><b>Read-only warm (current):</b> all parquet files are REMOTE. The registry
 * is seeded from remote metadata at shard open. No local files, no eviction.
 *
 * <p><b>TODO (writable warm):</b> add getFileLocation, acquireRead, releaseRead
 * when LOCAL parquet files exist and eviction is enabled. Wire
 * {@code ts_get_file_location} FFM call for LOCAL/REMOTE routing.
 */
public class ParquetDataFormatStoreHandler implements DataFormatStoreHandler {

    private static final Logger logger = LogManager.getLogger(ParquetDataFormatStoreHandler.class);
    private final NativeStoreHandle storeHandle;

    /**
     * Creates a per-shard native file registry.
     * On warm nodes with a live native store, creates a Rust TieredObjectStore via FFM.
     * On hot nodes (or when native store is unavailable), creates an empty handle (no-op).
     *
     * @param shardId the shard id (for logging)
     * @param isWarm  true if the shard is on a warm node
     * @param repo    the native remote store, or {@code NativeStoreRepository.EMPTY}
     */
    public ParquetDataFormatStoreHandler(ShardId shardId, boolean isWarm, NativeStoreRepository repo) {
        if (isWarm) {
            long remotePtr = (repo != null && repo.isLive()) ? repo.getPointer() : 0L;
            long ptr = TieredStorageBridge.createTieredObjectStore(0L, remotePtr);
            this.storeHandle = new NativeStoreHandle(ptr, TieredStorageBridge::destroyTieredObjectStore);
            logger.debug("[{}] Created ParquetDataFormatStoreHandler with native store, ptr={}", shardId, ptr);
        } else {
            this.storeHandle = NativeStoreHandle.EMPTY;
        }
    }

    @Override
    public void seed(Map<String, FileEntry> files) {
        if (storeHandle.isLive() == false) {
            return;
        }
        for (Map.Entry<String, FileEntry> entry : files.entrySet()) {
            TieredStorageBridge.registerFile(
                storeHandle.getPointer(),
                entry.getKey(),
                entry.getValue().path(),
                entry.getValue().location(),
                entry.getValue().size()
            );
        }
        logger.trace("seed: {} files registered", files.size());
    }

    @Override
    public void onUploaded(String file, String remotePath, long size) {
        if (storeHandle.isLive() == false) {
            return;
        }
        TieredStorageBridge.registerFile(storeHandle.getPointer(), file, remotePath, REMOTE, size);
        logger.trace("onUploaded: file=[{}], remotePath=[{}], size={}", file, remotePath, size);
    }

    @Override
    public void onRemoved(String file) {
        if (storeHandle.isLive() == false) {
            return;
        }
        TieredStorageBridge.removeFile(storeHandle.getPointer(), file);
        logger.trace("onRemoved: file=[{}]", file);
    }

    @Override
    public NativeStoreHandle getFormatStoreHandle() {
        return storeHandle;
    }

    @Override
    public void close() throws IOException {
        storeHandle.close();
    }
}
