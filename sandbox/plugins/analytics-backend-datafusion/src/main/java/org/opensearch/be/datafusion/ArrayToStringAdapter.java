/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

import java.util.List;

/**
 * Rename adapter for Calcite's {@code ARRAY_JOIN(arr, sep)} — used by PPL's
 * {@code mvjoin} via {@code SqlLibraryOperators.ARRAY_JOIN}. DataFusion's native
 * equivalent is named {@code array_to_string} (same semantics: join array
 * elements with a separator). Rewrites to a locally-declared {@link SqlFunction}
 * named {@code array_to_string}; isthmus emits a Substrait scalar function call
 * with that name and DataFusion's substrait consumer resolves it natively.
 *
 * @opensearch.internal
 */
class ArrayToStringAdapter extends AbstractNameMappingAdapter {

    static final SqlOperator LOCAL_ARRAY_TO_STRING_OP = new SqlFunction(
        "array_to_string",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.SYSTEM
    );

    ArrayToStringAdapter() {
        super(LOCAL_ARRAY_TO_STRING_OP, List.of(), List.of());
    }
}
