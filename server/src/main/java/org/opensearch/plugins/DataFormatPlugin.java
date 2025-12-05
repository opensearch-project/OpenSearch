/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.execution.search.spi.DataFormatCodec;
import org.opensearch.index.engine.exec.format.DataFormat;

import java.util.Map;
import java.util.Optional;

/**
 * Base data format plugin interface to extend query and writer capabilities to any data format such as parquet
 */
public interface DataFormatPlugin {
    default Optional<Map<DataFormat, DataFormatCodec>> getDataFormatCodecs() {
        return Optional.empty();
    }

    DataFormat getDataFormat();
}
