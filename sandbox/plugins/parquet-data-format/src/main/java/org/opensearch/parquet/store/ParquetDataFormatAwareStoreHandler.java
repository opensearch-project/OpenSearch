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
import org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;

import java.io.IOException;
import java.util.Map;

/**
 * Parquet implementation of {@link DataFormatAwareStoreHandler}.
 * Owns the native TieredObjectStore per-shard, created via {@link #create(boolean, NativeStoreRepository)}.
 *
 * <p><b>Read-only warm (current):</b> all parquet files are REMOTE. The handler tracks
 * file locations for the Rust FileRegistry but the Java directory always reads from remote.
 *
 * <p><b>TODO (writable warm):</b> add getFileLocation, acquireRead, releaseRead when
 * LOCAL parquet files exist and eviction is enabled.
 */
public class ParquetDataFormatAwareStoreHandler implements DataFormatAwareStoreHandler {

    private static final Logger logger = LogManager.getLogger(ParquetDataFormatAwareStoreHandler.class);
    private NativeStoreHandle storeHandle = NativeStoreHandle.EMPTY;

    @Override
    public void create(boolean isWarm, NativeStoreRepository repoStore) {
        if (isWarm && repoStore != null && repoStore.isLive()) {
            // TODO: FFM — storePtr = ts_create_tiered_object_store(localPath)
            // TODO: FFM — ts_set_remote_store(storePtr, repoStore.getPointer())
            // this.storeHandle = new NativeStoreHandle(storePtr, TieredStorageNative::destroy);
            logger.debug("Created ParquetDataFormatAwareStoreHandler with native store for warm node");
        }
    }

    @Override
    public void seedFileLocations(Map<String, String> fileToPath, FileLocation location) {
        if (storeHandle.isLive() == false) {
            return;
        }
        // TODO: FFM — batch ts_seed_file_locations(storeHandle.getPointer(), fileToPath, location)
        logger.trace("seedFileLocations: {} files, location={}", fileToPath.size(), location);
    }

    @Override
    public void removeFile(String file) {
        if (storeHandle.isLive() == false) {
            return;
        }
        // TODO: FFM — ts_remove_file(storeHandle.getPointer(), file)
        logger.trace("removeFile: file=[{}]", file);
    }

    @Override
    public void afterSyncToRemote(String file, String remotePath) {
        if (storeHandle.isLive() == false) {
            return;
        }
        // TODO: FFM — ts_after_sync_to_remote(storeHandle.getPointer(), file, remotePath)
        logger.trace("afterSyncToRemote: file=[{}], remotePath=[{}]", file, remotePath);
    }

    @Override
    public void close() throws IOException {
        storeHandle.close();
    }
}
