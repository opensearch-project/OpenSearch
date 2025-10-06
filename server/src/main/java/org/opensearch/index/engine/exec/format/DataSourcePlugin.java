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

public interface DataSourcePlugin {

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(IndexingConfiguration configuration);

    DataFormat getDataFormat();
}
