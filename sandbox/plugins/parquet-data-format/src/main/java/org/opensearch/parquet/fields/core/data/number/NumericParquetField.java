/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.parquet.fields.ParquetField;

import java.util.Set;

/**
 * Parquet field for numeric values. Declares common capabilities supported for numeric field in parquet.
 */
public abstract class NumericParquetField extends ParquetField {

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(
            FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.BLOOM_FILTER,
            FieldTypeCapabilities.Capability.POINT_RANGE
        );
    }
}
