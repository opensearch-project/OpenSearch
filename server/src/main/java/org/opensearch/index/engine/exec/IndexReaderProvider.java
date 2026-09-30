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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

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
     * Acquires a point-in-time {@link Engine.SearcherSupplier} for the standard {@code _search} path.
     * Throws by default; engine-backed and data-format-aware implementations override it.
     *
     * @param wrapper applied to each acquired {@link Engine.Searcher} (e.g. reader wrapping)
     * @param scope   the searcher scope
     * @return a searcher supplier whose {@code close()} releases the underlying reader resources
     */
    default Engine.SearcherSupplier acquireSearcherSupplier(
        Function<Engine.Searcher, Engine.Searcher> wrapper,
        Engine.SearcherScope scope
    ) {
        throw new UnsupportedOperationException("acquireSearcherSupplier not supported by " + getClass().getName());
    }

    /**
     * Acquires a single {@link Engine.Searcher}. By default acquires one from
     * {@link #acquireSearcherSupplier} and closes the supplier when the searcher is closed.
     *
     * @param source  description of why the searcher is being acquired
     * @param scope   the searcher scope
     * @param wrapper applied to the acquired {@link Engine.Searcher}
     * @return a searcher whose {@code close()} releases the supplier and the underlying reader resources
     */
    default Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope, Function<Engine.Searcher, Engine.Searcher> wrapper) {
        Engine.SearcherSupplier supplier = acquireSearcherSupplier(wrapper, scope);
        try {
            Engine.Searcher searcher = supplier.acquireSearcher(source);
            final Engine.SearcherSupplier toClose = supplier;
            Engine.Searcher wrapped = new Engine.Searcher(
                searcher.source(),
                searcher.getDirectoryReader(),
                searcher.getSimilarity(),
                searcher.getQueryCache(),
                searcher.getQueryCachingPolicy(),
                () -> IOUtils.close(searcher, toClose)
            );
            supplier = null; // ownership transferred to the returned searcher
            return wrapped;
        } finally {
            IOUtils.closeWhileHandlingException(supplier);
        }
    }

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

        <R> R getReader(DataFormat format, Class<R> readerType);
    }
}
