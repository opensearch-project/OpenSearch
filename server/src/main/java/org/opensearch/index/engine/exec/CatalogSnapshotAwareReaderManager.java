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

import java.io.IOException;

/**
 * Engine-agnostic reader manager that is catalog-snapshot aware.
 * <p>
 * For Lucene, wraps {@code ReferenceManager<OpenSearchDirectoryReader>}.
 * For pluggable engines, wraps the engine-specific reader lifecycle.
 *
 * @param <T> the reader type managed by this instance
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogSnapshotAwareReaderManager<T> extends CatalogSnapshotLifecycleListener, FilesListener {
    T getReader(CatalogSnapshot catalogSnapshot) throws IOException;
}
