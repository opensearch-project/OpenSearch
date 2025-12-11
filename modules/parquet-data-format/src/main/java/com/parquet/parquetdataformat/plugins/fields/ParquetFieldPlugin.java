/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.plugins.fields;

import com.parquet.parquetdataformat.fields.ParquetField;

import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for registering custom Parquet field implementations.
 * Plugins implementing this interface can register their field types with the ArrowFieldRegistry.
 */
@FunctionalInterface
public interface ParquetFieldPlugin {

    /**
     * Returns additional Parquet field implementations added by this plugin.
     *
     * @return a map where keys are OpenSearch field type names and values are ParquetField instances
     */
    Map<String, ParquetField> getParquetFields();
}
