/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;

/**
 * Listener notified by the engine when data-format files gain their first catalog
 * reference (i.e. a completed writer or merge output becomes visible at refresh).
 *
 * <p>Implemented by the warm-tier directory to register locally-written format files
 * in the native tiered-store registry and to account their bytes in the file cache
 * until the upload flip releases them. The native writer bypasses
 * {@code Directory.createOutput}, so this catalog-entry event is the earliest point
 * the store layer can observe a completed native file.
 *
 * @opensearch.experimental
 */
@FunctionalInterface
@ExperimentalApi
public interface FormatFilesAddedListener {

    /**
     * Called when {@code files} (bare file names, relative to the format subdirectory)
     * of data format {@code format} gain their first catalog reference.
     *
     * @param format the data format name (e.g. {@code "parquet"})
     * @param files  bare file names within the format subdirectory
     */
    void onFormatFilesAdded(String format, Collection<String> files);
}
