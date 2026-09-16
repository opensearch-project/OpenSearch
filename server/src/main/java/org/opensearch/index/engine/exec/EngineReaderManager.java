/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Engine-agnostic reader manager.
 * <p>
 * For Lucene, wraps {@code ReferenceManager<OpenSearchDirectoryReader>}.
 * For pluggable engines, wraps the engine-specific reader lifecycle.
 *
 * @param <T> the reader type managed by this instance
 * @opensearch.experimental
 */
@ExperimentalApi
public interface EngineReaderManager<T> extends CatalogSnapshotLifecycleListener, FilesListener, Closeable {
    T getReader(CatalogSnapshot catalogSnapshot) throws IOException;

    /**
     * Live and deleted document counts for the given snapshot, keyed by writer generation.
     * Must not throw when the snapshot has no reader registered yet: stats calls happen at
     * arbitrary times and must never fail. Return an empty map instead.
     *
     * @param catalogSnapshot the snapshot to report on
     * @return per-generation counts, or an empty map if this format cannot report liveness
     * @throws IOException if reading the underlying reader fails
     */
    default Map<Long, DocCounts> docCountsByGeneration(CatalogSnapshot catalogSnapshot) throws IOException {
        return Map.of();
    }

    /**
     * Asks each reader manager in turn for {@link #docCountsByGeneration} and returns the first
     * non-empty answer.
     *
     * @param readerManagers the shard's reader managers, in any order
     * @param catalogSnapshot the snapshot to report on
     * @return per-generation counts, or an empty map if no manager reports liveness
     * @throws IOException if reading the underlying reader fails
     */
    static Map<Long, DocCounts> firstReportedDocCounts(
        Collection<? extends EngineReaderManager<?>> readerManagers,
        CatalogSnapshot catalogSnapshot
    ) throws IOException {
        for (EngineReaderManager<?> readerManager : readerManagers) {
            Map<Long, DocCounts> counts = readerManager.docCountsByGeneration(catalogSnapshot);
            if (counts.isEmpty() == false) {
                return counts;
            }
        }
        return Map.of();
    }
}
