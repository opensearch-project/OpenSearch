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
import org.opensearch.index.engine.dataformat.NativeFileRegistry;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;

import java.io.IOException;
import java.util.Map;

/**
 * Per-shard native file registry for the parquet format.
 *
 * <p>Wraps the Rust {@code TieredObjectStore} pointer that the parquet native
 * reader consults to resolve file identifiers to blob paths. The store layer
 * owns the lifecycle — this class is just the FFM boundary.
 *
 * <p>Read-only warm (current): every parquet file is REMOTE; the Java directory
 * always reads from the remote store. The registry still receives
 * seed/upload/remove events so the Rust {@code FileRegistry} stays consistent
 * with the Java view.
 */
public final class ParquetNativeFileRegistry implements NativeFileRegistry {

    private static final Logger logger = LogManager.getLogger(ParquetNativeFileRegistry.class);

    private final ShardId shardId;
    private NativeStoreHandle storeHandle = NativeStoreHandle.EMPTY;

    public ParquetNativeFileRegistry(ShardId shardId, boolean isWarm, NativeStoreRepository repoStore) {
        this.shardId = shardId;
        if (isWarm && repoStore != null && repoStore.isLive()) {
            // TODO: FFM — storePtr = ts_create_tiered_object_store(localPath)
            // TODO: FFM — ts_set_remote_store(storePtr, repoStore.getPointer())
            // this.storeHandle = new NativeStoreHandle(storePtr, TieredStorageNative::destroy);
            logger.debug("Created ParquetNativeFileRegistry for shard [{}]", shardId);
        }
    }

    @Override
    public void seed(Map<String, String> fileToRemotePath) {
        if (storeHandle.isLive() == false) {
            return;
        }
        // TODO: FFM — ts_seed_file_locations(storeHandle.getPointer(), fileToRemotePath)
        logger.trace("seed: shard=[{}], count={}", shardId, fileToRemotePath.size());
    }

    @Override
    public void onUploaded(String file, String remotePath) {
        if (storeHandle.isLive() == false) {
            return;
        }
        // TODO: FFM — ts_after_sync_to_remote(storeHandle.getPointer(), file, remotePath)
        logger.trace("onUploaded: shard=[{}], file=[{}], remotePath=[{}]", shardId, file, remotePath);
    }

    @Override
    public void onRemoved(String file) {
        if (storeHandle.isLive() == false) {
            return;
        }
        // TODO: FFM — ts_remove_file(storeHandle.getPointer(), file)
        logger.trace("onRemoved: shard=[{}], file=[{}]", shardId, file);
    }

    @Override
    public void close() throws IOException {
        storeHandle.close();
    }
}
