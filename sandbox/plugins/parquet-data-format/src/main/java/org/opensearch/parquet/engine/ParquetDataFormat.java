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
import java.util.stream.Collectors;

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

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        // TODO - Override FieldRegistry to return capability for each field
        return ArrowFieldRegistry.getRegisteredFields()
            .keySet()
            .stream()
            .map(
                type -> new FieldTypeCapabilities(
                    type,
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER)
                )
            )
            .collect(Collectors.toUnmodifiableSet());
    }
}
