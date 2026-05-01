/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * Strategy describing how a data format participates in the tiered store.
 *
 * <p>Data format plugins return an implementation from
 * {@link DataFormatPlugin#getStoreStrategy()} to tell the store layer three
 * things:
 * <ul>
 *   <li>the format name (used to correlate with {@link DataFormat#name()})</li>
 *   <li>which files in the directory belong to this format ({@link #owns})</li>
 *   <li>the layout the format uses on the remote store ({@link #remotePath})</li>
 *   <li>optionally, a per-shard {@link NativeFileRegistryFactory} for formats
 *       with a native reader</li>
 * </ul>
 *
 * <p>All cross-cutting work (per-shard lifecycle, seeding from remote metadata,
 * directory routing, close ordering, sync notifications) is handled by the
 * store layer, not by the plugin. A plugin supplies behavior; the layer
 * supplies state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StoreStrategy {

    /**
     * The data format name, e.g. {@code "parquet"}. Must match the name used by
     * {@link DataFormat#name()} so the store layer can correlate strategies
     * with formats resolved elsewhere.
     */
    String name();

    /**
     * Returns true if the given file identifier belongs to this format.
     *
     * <p>The default convention is that format files live under a subdirectory
     * whose prefix is the format name (e.g. {@code "parquet/seg_0.parquet"}).
     * Implementations may override to use a different layout.
     *
     * @param file file identifier as produced by the directory layer
     */
    default boolean owns(String file) {
        if (file == null) {
            return false;
        }
        return file.startsWith(name() + "/");
    }

    /**
     * Returns the fully-qualified remote blob path for a file owned by this format.
     *
     * <p>The default convention places blobs at
     * {@code basePath + name() + "/" + blobKey}. Implementations may override
     * when the format uses a different layout on the remote store.
     *
     * @param basePath the repository base path (may be empty)
     * @param file     the file identifier (unused by the default layout)
     * @param blobKey  the uploaded blob key returned by
     *                 {@link org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata#getUploadedFilename()}
     * @return the remote blob path
     */
    default String remotePath(String basePath, String file, String blobKey) {
        StringBuilder sb = new StringBuilder();
        if (basePath != null && basePath.isEmpty() == false) {
            sb.append(basePath);
        }
        sb.append(name()).append('/').append(blobKey);
        return sb.toString();
    }

    /**
     * Returns an optional factory that produces a per-shard native file
     * registry. Formats without a native reader return
     * {@link Optional#empty()}.
     */
    default Optional<NativeFileRegistryFactory> nativeFileRegistry() {
        return Optional.empty();
    }
}
