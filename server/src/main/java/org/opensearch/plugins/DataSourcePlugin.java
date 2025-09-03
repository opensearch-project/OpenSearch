/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.util.Map;
import java.util.Optional;

public interface DataSourcePlugin {
    default Optional<Map<org.opensearch.vectorized.execution.search.DataFormat, DataSourceCodec>> getDataSourceCodecs() {
        return Optional.empty();
    }

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine();

    DataFormat getDataFormat();
}
