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

/** Routes PPL's {@code PATTERN_PARSER} onto the local {@code pattern_parser} operator. */
class PatternParserAdapter extends AbstractNameMappingAdapter {

    static final SqlOperator LOCAL_PATTERN_PARSER_OP = new SqlFunction(
        "pattern_parser",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    PatternParserAdapter() {
        super(LOCAL_PATTERN_PARSER_OP, List.of(), List.of());
    }
}
