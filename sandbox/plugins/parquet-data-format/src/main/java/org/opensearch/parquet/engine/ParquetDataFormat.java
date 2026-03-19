/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.Set;

/**
 * Parquet data format implementation.
 */
public class ParquetDataFormat implements DataFormat {

    static final String PARQUET_DATA_FORMAT_NAME = "parquet";

    @Override
    public String name() {
        return PARQUET_DATA_FORMAT_NAME;
    }

    @Override
    public long priority() {
        return 0;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return Set.of();
    }
}
