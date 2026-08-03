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
 * Immutable request carrying all parameters for a single term value translation.
 *
 * @param value the term value to translate
 * @param field the target field definition
 * @param ctx the conversion context
 */
public record TermRequest(Object value, RelDataTypeField field, ConversionContext ctx) {

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
