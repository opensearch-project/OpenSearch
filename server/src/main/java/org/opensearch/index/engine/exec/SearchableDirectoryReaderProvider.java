/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Marker interface for format-specific reader objects that can provide a Lucene
 * {@link DirectoryReader} for search operations. Implementations live in format plugins
 * (e.g. the Lucene backend plugin's {@code LuceneReader}), while this interface lives in
 * the server module so that {@code DataFormatAwareEngine} can obtain a {@link DirectoryReader}
 * without depending on any specific format plugin at compile time.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchableDirectoryReaderProvider {

    /**
     * Returns the {@link DirectoryReader} that can be used to build an
     * {@link org.opensearch.index.engine.Engine.Searcher} for the standard search path.
     *
     * @return a non-null directory reader
     */
    DirectoryReader directoryReader();
}
