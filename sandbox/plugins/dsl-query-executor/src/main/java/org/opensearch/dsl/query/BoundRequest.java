/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;

/**
 * Immutable request carrying all parameters for a single range bound translation.
 *
 * @param value the bound value (already processed through date/numeric coercion)
 * @param isLower true for a lower bound, false for an upper bound
 * @param inclusive the RAW inclusivity from the query (never pre-adjusted)
 * @param format optional date format from the range query
 * @param timeZone optional time zone from the range query
 * @param field the target field definition
 * @param ctx the conversion context
 */
public record BoundRequest(Object value, boolean isLower, boolean inclusive, String format, String timeZone, RelDataTypeField field,
    ConversionContext ctx) {

    /** Lower bounds round up when exclusive; upper bounds round up when inclusive. Reproduces DateFieldMapper semantics. */
    public boolean roundUp() {
        return isLower ? !inclusive : inclusive;
    }

    /** Returns the Calcite type of the target field. */
    public RelDataType fieldType() {
        return field.getType();
    }

    /** Returns the SqlTypeName of the target field. */
    public SqlTypeName sqlTypeName() {
        return field.getType().getSqlTypeName();
    }

    /** Returns the name of the target field. */
    public String fieldName() {
        return field.getName();
    }
}
