/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provides access to index readers for search operations across data formats.
 * Implementations manage the lifecycle of readers, ensuring they remain valid
 * for the duration of a search operation via {@link GatedCloseable}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexReaderProvider {

    /**
     * Acquires a point-in-time {@link Reader} wrapped in a {@link GatedCloseable}.
     * The caller must close the returned {@link GatedCloseable} when the reader is no longer needed
     * to release the underlying resources.
     *
     * @return a gated closeable wrapping the acquired reader
     * @throws IOException if an I/O error occurs while acquiring the reader
     */
    GatedCloseable<Reader> acquireReader() throws IOException;

    /**
     * A point-in-time reader over the index state, providing access to the
     * {@link CatalogSnapshot} and format-specific readers.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    interface Reader extends Closeable {

        /**
         * Returns the {@link CatalogSnapshot} representing the index state at the time this reader was acquired.
         *
         * @return the catalog snapshot
         */
        CatalogSnapshot catalogSnapshot();

        /**
         * Returns the format-specific reader for the given {@link DataFormat}.
         * The returned object type depends on the data format implementation
         * (e.g., a Lucene {@code DirectoryReader} or a native reader handle).
         *
         * @param format the data format to get the reader for
         * @return the format-specific reader object
         */
        Object reader(DataFormat format);
    }
}
