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
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheConstants;
import org.opensearch.plugins.BlockCacheRegistry;
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
    /** Cached native object store handle for DataFusion readers — created lazily, closed with the handler. */
    private volatile NativeStoreHandle nativeStoreForReader;

    /**
     * Creates a per-shard native file registry.
     *
     * <p>On warm nodes, creates a native store via FFM. The handler resolves
     * {@link BlockCacheConstants#FOYER} from {@code cacheRegistry} and wires it into the
     * store if available.
     *
     * <p>On hot nodes (or when the native store is unavailable), creates an empty
     * handle (all operations are no-ops).
     *
     * @param shardId        the shard id (for logging)
     * @param isWarm         true if the shard is on a warm node
     * @param repo           the native remote store, or {@code NativeStoreRepository.EMPTY}
     * @param cacheRegistry  registry for resolving the preferred block cache by name,
     *                       or {@code null} if no block cache support is available
     */
    public ParquetDataFormatStoreHandler(ShardId shardId, boolean isWarm, NativeStoreRepository repo, BlockCacheRegistry cacheRegistry) {
        if (isWarm) {
            long remotePtr = (repo != null && repo.isLive()) ? repo.getPointer() : 0L;

            // Resolve preferred cache by name; EMPTY if unavailable (hot nodes or no matching cache).
            // owned by NodeCacheOrchestrator and must not be freed here.
            NativeStoreHandle cacheHandle = (cacheRegistry != null)
                ? cacheRegistry.get(BlockCacheConstants.DISK_CACHE).map(BlockCache::nativeCacheHandle).orElse(NativeStoreHandle.EMPTY)
                : NativeStoreHandle.EMPTY;
            long cachePtr = cacheHandle.isLive() ? cacheHandle.getPointer() : 0L;

            long ptr = TieredStorageBridge.createTieredObjectStore(0L, remotePtr, cachePtr);
            this.storeHandle = new NativeStoreHandle(ptr, TieredStorageBridge::destroyTieredObjectStore);

            logger.debug(
                "[{}] ParquetDataFormatStoreHandler created: cache={}",
                shardId,
                cacheHandle.isLive() ? BlockCacheConstants.DISK_CACHE : "none"
            );
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
        if (storeHandle.isLive() == false) {
            return NativeStoreHandle.EMPTY;
        }
        // Lazily create the boxed pointer once — same lifetime as the handler (shard lifetime).
        // The box holds an Arc clone of the TieredObjectStore, keeping it alive independently.
        if (nativeStoreForReader == null) {
            synchronized (this) {
                if (nativeStoreForReader == null) {
                    try {
                        long boxPtr = TieredStorageBridge.getObjectStoreBoxPtr(storeHandle.getPointer());
                        if (boxPtr > 0) {
                            nativeStoreForReader = new NativeStoreHandle(boxPtr, TieredStorageBridge::destroyObjectStoreBoxPtr);
                        }
                    } catch (Exception e) {
                        logger.error("getFormatStoreHandle: failed to get object store box ptr", e);
                    }
                }
            }
        }
        return nativeStoreForReader != null ? nativeStoreForReader : NativeStoreHandle.EMPTY;
    }

    @Override
    public void close() throws IOException {
        // Close box handle first (decrements Arc refcount), then the store handle (frees TieredObjectStore).
        if (nativeStoreForReader != null) {
            nativeStoreForReader.close();
        }
        storeHandle.close();
    }
}
