/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Lazy file resolver for handling native registry misses at query time.
 *
 * <p>When the Rust {@code TieredObjectStore} encounters a file not in its registry,
 * it calls back into Java via an FFM upcall. The upcall target uses this resolver
 * to look up the file's remote path from {@code RemoteSegmentStoreDirectory} metadata
 * and register it in the native registry.
 *
 * <p>This interface lives in the server SPI layer so that format plugins (e.g., parquet)
 * never depend on {@code RemoteSegmentStoreDirectory} directly. The store layer provides
 * the concrete implementation as a closure over the remote directory.
 *
 * @opensearch.experimental
 */
@FunctionalInterface
@ExperimentalApi
public interface FileResolver {

    /**
     * Resolves the remote path for a file not present in the native registry.
     *
     * <p>Called lazily on registry miss during query execution. The implementation
     * should look up the file in remote metadata, compute the blob path, and return
     * a {@link DataFormatStoreHandler.FileEntry} containing the remote path and size.
     *
     * @param absoluteKey the absolute file path (same key format used in
     *                    {@link DataFormatStoreHandler#seed} and
     *                    {@link DataFormatStoreHandler#onUploaded})
     * @return a {@link DataFormatStoreHandler.FileEntry} with the remote path,
     *         location ({@link DataFormatStoreHandler#REMOTE}), and size;
     *         or {@code null} if the file cannot be resolved
     */
    DataFormatStoreHandler.FileEntry resolve(String absoluteKey);
}
