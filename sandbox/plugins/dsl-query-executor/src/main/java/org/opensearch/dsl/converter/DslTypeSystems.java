/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/** Shared type-system constants: default Calcite clamps TIMESTAMP to precision 3, date_nanos needs 9. */
public final class DslTypeSystems {

    /** TIMESTAMP max precision raised to 9 so date_nanos fields are not silently clamped. */
    public static final RelDataTypeSystem NANO_TIMESTAMP = new RelDataTypeSystemImpl() {
        @Override
        public int getMaxPrecision(SqlTypeName typeName) {
            if (typeName == SqlTypeName.TIMESTAMP) {
                return 9;
            }
            return super.getMaxPrecision(typeName);
        }
    };

    private DslTypeSystems() {}
}
