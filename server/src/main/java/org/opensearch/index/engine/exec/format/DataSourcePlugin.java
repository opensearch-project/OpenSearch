/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.format;

import org.opensearch.index.engine.exec.engine.IndexingConfiguration;
import org.opensearch.index.engine.exec.engine.IndexingExecutionEngine;

/**
 * Plugin for data source implementations.
 */
public interface DataSourcePlugin {

    /**
     * Creates an indexing execution engine.
     * @param configuration the indexing configuration
     * @param <T> the data format type
     * @return the indexing execution engine
     */
    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(IndexingConfiguration configuration);

    /**
     * Gets the data format.
     * @return the data format
     */
    DataFormat getDataFormat();
}
