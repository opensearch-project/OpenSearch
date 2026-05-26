/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * PPL {@code dayofweek}/{@code day_of_week} → {@code CAST(date_part('dow', x) + 1 AS <retType>)}:
 * MySQL/PPL uses 1=Sun..7=Sat but DataFusion/Postgres {@code date_part('dow')} returns 0..6, so we
 * add 1 and cast back to the original call's return type.
 *
 * @opensearch.internal
 */
class DayOfWeekAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode partLiteral = rexBuilder.makeLiteral("dow", varchar, true);
        RexNode datePart = rexBuilder.makeCall(SqlLibraryOperators.DATE_PART, partLiteral, original.getOperands().get(0));
        RexNode sum = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, datePart, rexBuilder.makeExactLiteral(BigDecimal.ONE));
        return rexBuilder.makeCast(original.getType(), sum);
    }
}
