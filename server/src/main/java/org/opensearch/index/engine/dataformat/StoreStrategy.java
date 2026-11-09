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
 * <p>Returned by {@link DataFormatPlugin#getStoreStrategies} keyed by the
 * format name. The strategy itself is stateless regarding its name — the map
 * key supplies the identity — and the store layer passes the name into
 * {@link #owns} and {@link #remotePath} whenever behaviour depends on it.
 *
 * <p>A strategy contributes three pieces of behaviour:
 * <ul>
 *   <li>{@link #owns} — which files in the directory belong to this format</li>
 *   <li>{@link #remotePath} — how the format lays out blobs on the remote store</li>
 *   <li>optionally, {@link #storeHandler()} for formats with a native reader</li>
 * </ul>
 *
 * <p>All cross-cutting work (per-shard lifecycle, seeding from remote metadata,
 * directory routing, close ordering, sync notifications) is handled by the
 * store layer, not by the plugin.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class StoreStrategy {

    /**
     * Returns true if the given file identifier belongs to this format.
     *
     * <p>The default convention is that format files live under a subdirectory
     * whose prefix is the format name (e.g. {@code "parquet/seg_0.parquet"}).
     *
     * @param name the format name the store layer associated with this
     *             strategy (the key it was registered under)
     * @param file file identifier as produced by the directory layer
     */
    public final boolean owns(String name, String file) {
        if (file == null) {
            return false;
        }
        return file.startsWith(name + "/");
    }

    /**
     * Returns the fully-qualified remote blob path for a file owned by this format.
     *
     * <p>The default convention places blobs at
     * {@code basePath + name + "/" + blobKey}.
     *
     * @param name     the format name the store layer associated with this
     *                 strategy (the key it was registered under)
     * @param basePath the repository base path (may be empty)
     * @param file     the file identifier (unused by the default layout)
     * @param blobKey  the uploaded blob key returned by
     *                 {@link org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata#getUploadedFilename()}
     * @return the remote blob path
     */
    public final String remotePath(String name, String basePath, String file, String blobKey) {
        StringBuilder sb = new StringBuilder();
        if (basePath != null && basePath.isEmpty() == false) {
            sb.append(basePath);
        }
        sb.append(blobKey);
        return sb.toString();
    }

    /**
     * Returns an optional factory that produces a per-shard native file
     * registry. Formats without a native reader return
     * {@link Optional#empty()}.
     */
    public abstract Optional<DataFormatStoreHandlerFactory> storeHandler();
}
