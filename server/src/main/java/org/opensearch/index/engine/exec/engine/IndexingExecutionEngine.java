/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.format.DataFormat;

import java.io.IOException;

/**
 * An engine responsible for writing data in a given data format. This is the core interface for basic write
 * operations which we will have to expose for a data format.
 * @param <T> the dataformat this engine will perform writes for.
 */
@ExperimentalApi
public interface IndexingExecutionEngine<T extends DataFormat> {

    /**
     * Provides an instance of writer which can be used to perform actual writes using the configuration of the current
     * engine instance.
     * @throws IOException
     */
    Writer<? extends DocumentInput<?>> createWriter() throws IOException; // A writer responsible for data format vended by this engine.

    /**
     * Makes data available for search for a given data format.
     */
    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    DataFormat getDataFormat();

    IndexingConfiguration indexingConfiguration();
}
