/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugins.NativeStoreHandle;

import java.io.Closeable;
import java.util.Map;

/**
 * Per-shard handler for a data format's store lifecycle.
 *
 * <p>Data format plugins that use a native (e.g. Rust) reader return one of
 * these via {@link StoreStrategy#storeHandler()}. The store layer owns
 * the instance, drives its lifecycle, and forwards file events (seed, upload,
 * remove) that originate in the Java directory.
 *
 * <p>Formats without a native reader return {@link java.util.Optional#empty()}
 * from the strategy and never produce an instance of this interface.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatStoreHandler extends Closeable {

    /**
     * File location constants matching the Rust {@code FileLocation} enum.
     * <ul>
     *   <li>{@code LOCAL}  — file exists only on local disk</li>
     *   <li>{@code REMOTE} — file exists only on a remote object store</li>
     * </ul>
     */
    int LOCAL = 0;
    int REMOTE = 1;

    /**
     * A file entry carrying the blob path, location, and size.
     *
     * @param path     fully-qualified blob path (local path for LOCAL, remote blob path for REMOTE)
     * @param location one of {@link #LOCAL} or {@link #REMOTE}
     * @param size     file size in bytes (0 if unknown)
     */
    @ExperimentalApi
    record FileEntry(String path, int location, long size) {
    }

    /**
     * Seeds the handler with a batch of files and their locations.
     * Called once per shard at open time.
     *
     * @param files map of file identifier (e.g. {@code "parquet/seg_0.parquet"})
     *              to {@link FileEntry} carrying the blob path and location
     */
    void seed(Map<String, FileEntry> files);

    /**
     * Called after a file has been uploaded to the remote store.
     *
     * @param file       the file identifier (absolute path)
     * @param remotePath the remote blob path (base path + format prefix + blob key)
     * @param size       file size in bytes
     */
    void onUploaded(String file, String remotePath, long size);

    /**
     * Called after a file has been removed from tracking.
     *
     * @param file the file identifier
     */
    void onRemoved(String file);

    /**
     * Returns the native store handle wrapping the Rust object store pointer,
     * or {@code null} if this handler does not manage a native store.
     *
     * <p>The reader manager uses this to register the native object store
     * in the DataFusion runtime environment.
     */
    default NativeStoreHandle getFormatStoreHandle() {
        return null;
    }
}
