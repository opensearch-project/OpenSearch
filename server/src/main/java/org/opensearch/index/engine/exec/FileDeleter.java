/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Functional interface for deleting index files.
 * Implementations are provided by the engine layer.
 *
 * @opensearch.experimental
 */
@FunctionalInterface
@ExperimentalApi
public interface FileDeleter {
    /** A no-op deleter that discards all delete requests. */
    FileDeleter NOOP = filesToDelete -> {};

    /**
     * @param filesToDelete map of data format name to collection of file names to delete
     */
    void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;
}
