/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.repositories.NativeStoreRepository;

import java.io.Closeable;
import java.util.Map;

/**
 * Per-format store handler for tiered storage — manages file location tracking,
 * native store lifecycle, and sync notifications.
 *
 * <p>Each data format plugin (e.g., parquet) provides its own implementation.
 * The handler is created per-shard via {@link #create(boolean, NativeStoreRepository)}
 * and owns the native store (e.g., Rust TieredObjectStore).
 *
 * <p><b>Read-only warm (current scope):</b> all format files are REMOTE. The directory
 * routes all reads to {@code RemoteSegmentStoreDirectory}. No local copies, no eviction.
 *
 * <p><b>Lifecycle:</b>
 * <ol>
 *   <li>Plugin returns handler via {@code DataFormatPlugin.getDataFormatAwareStoreHandlers()}</li>
 *   <li>Caller initializes per-shard native resources via {@link #create(boolean, NativeStoreRepository)}</li>
 *   <li>Factory seeds handler from remote metadata via {@link #seedFileLocations}</li>
 *   <li>Directory calls {@link #afterSyncToRemote} after each file upload</li>
 *   <li>DataFusion reads use the native TieredObjectStore created by the handler</li>
 *   <li>{@link #close()} destroys native resources at shard close</li>
 * </ol>
 *
 * <p><b>TODO (writable warm):</b> when LOCAL format files exist, add:
 * <ul>
 *   <li>{@code getFileLocation(file)} — returns LOCAL/REMOTE/UNKNOWN for routing</li>
 *   <li>{@code acquireRead(file)} / {@code releaseRead(file)} — ref counting to prevent
 *       eviction while a reader holds a LOCAL file open</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatAwareStoreHandler extends Closeable {

    /**
     * Location of a file tracked by the handler.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    enum FileLocation {
        /** File exists on local disk (hot node write, or cached on warm). */
        LOCAL,
        /** File exists only on remote object store (warm node, post-eviction). */
        REMOTE
    }

    /**
     * Initializes per-shard native resources. Called once per shard at shard open time.
     *
     * <p>On warm nodes with a live native store, this creates the Rust TieredObjectStore
     * via FFM. On hot nodes (or when native store is unavailable), this is a no-op.
     *
     * <p>Must be called before {@link #seedFileLocations} or any other method.
     *
     * @param isWarm              true if the shard is on a warm node
     * @param nativeStoreRepository the native remote store for the index's repository,
     *                              or {@code NativeStoreRepository.EMPTY} if not available
     */
    default void create(boolean isWarm, NativeStoreRepository nativeStoreRepository) {}

    /**
     * Seeds file locations at shard open time. Called by the factory to register
     * format files discovered from remote metadata (as {@link FileLocation#REMOTE})
     * or from local disk (as {@link FileLocation#LOCAL}).
     *
     * <p>Implementations can override for a single batch FFM call instead of per-file.
     *
     * @param fileToPath map of file name (e.g., "parquet/seg_0.parquet") to path
     *                   (remote blob path when REMOTE, local path when LOCAL)
     * @param location   the location for all files in the batch
     */
    void seedFileLocations(Map<String, String> fileToPath, FileLocation location);

    /**
     * Removes a file from tracking. Called during cleanup and merge.
     *
     * @param file the file name
     */
    void removeFile(String file);

    /**
     * Called after a file has been successfully uploaded to the remote store.
     *
     * @param file       the file name that was synced
     * @param remotePath the remote blob path (format/blobKey)
     */
    void afterSyncToRemote(String file, String remotePath);

    // close() inherited from Closeable
}
