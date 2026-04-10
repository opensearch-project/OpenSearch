/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.plugins;

import org.opensearch.parquet.fields.ParquetField;

import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for registering custom Parquet field implementations.
 */
public interface ParquetFieldPlugin {

    /**
     * Returns the Parquet field implementations provided by this plugin.
     * @return map of field type names to ParquetField implementations
     */
    default Map<String, ParquetField> getParquetFields() {
        return Collections.emptyMap();
    }
}
