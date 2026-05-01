/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.util.Map;

/**
 * Per-shard native file registry for a single data format.
 *
 * <p>Data format plugins that use a native (e.g. Rust) reader return one of
 * these via {@link StoreStrategy#nativeFileRegistry()}. The store layer owns
 * the instance, drives its lifecycle, and forwards file events (seed, upload,
 * remove) that originate in the Java directory.
 *
 * <p>Formats without a native reader return {@link java.util.Optional#empty()}
 * from the strategy and never produce an instance of this interface.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface NativeFileRegistry extends Closeable {

    /**
     * Seeds the registry with a batch of files already present on the remote store.
     * Called once per shard at open time.
     *
     * @param fileToRemotePath map of file identifier (e.g. {@code "parquet/seg_0.parquet"})
     *                         to fully-qualified remote blob path
     */
    void seed(Map<String, String> fileToRemotePath);

    /**
     * Called after a file has been uploaded to the remote store.
     *
     * @param file       the file identifier
     * @param remotePath the remote blob path (base path + format prefix + blob key)
     */
    void onUploaded(String file, String remotePath);

    /**
     * Called after a file has been removed from tracking.
     *
     * @param file the file identifier
     */
    void onRemoved(String file);
}
