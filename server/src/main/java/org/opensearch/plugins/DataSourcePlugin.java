/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.vectorized.execution.spi.DataSourceCodec;

import java.util.Map;
import java.util.Optional;

public interface DataSourcePlugin {
    // TODO : move to vectorized exec specific plugin
    default Optional<Map<String, DataSourceCodec>> getDataSourceCodecs() {
        return Optional.empty();
    }
}
