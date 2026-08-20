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
        // No POINT_RANGE: the pluggable data format writes no BKD points for numeric fields, so the
        // field is not point-searchable. A mapping that requests search on a numeric field (index:true)
        // therefore cannot be covered by any configured format and is rejected via the capability path.
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER);
    }
}
