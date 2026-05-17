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
import org.opensearch.parquet.fields.ArrowFieldRegistry;

import java.util.Set;

/**
 * Data format descriptor for the Parquet format.
 *
 * <p>Declares the format name ({@code "parquet"}), priority, and supported field type
 * capabilities. Registered with OpenSearch's data format framework via
 * {@link org.opensearch.parquet.ParquetDataFormatPlugin#getDataFormat()}.
 */
public class ParquetDataFormat extends DataFormat {

    /** Creates a new ParquetDataFormat. */
    public ParquetDataFormat() {}

    /** The parquet data format name constant. */
    public static final String PARQUET_DATA_FORMAT_NAME = "parquet";

    @Override
    public String name() {
        return PARQUET_DATA_FORMAT_NAME;
    }

    @Override
    public long priority() {
        return 0;
    }

    /**
     * Returns the set of field type capabilities supported by the Parquet data format.
     * Delegates to {@link ArrowFieldRegistry#getSupportedFieldCapabilities()}.
     *
     * @return unmodifiable set of {@link FieldTypeCapabilities}
     */
    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return ArrowFieldRegistry.getSupportedFieldCapabilities();
    }
}
