/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * PPL {@code GET_FORMAT(<type>, <region>)} — returns the MySQL format string for a given temporal
 * type ({@code DATE}/{@code TIME}/{@code TIMESTAMP}/{@code DATETIME}) and region
 * ({@code USA}/{@code JIS}/{@code ISO}/{@code EUR}/{@code INTERNAL}). Both operands are string
 * literals, so the adapter folds the call to the resolved format-string literal at plan time —
 * there is nothing to evaluate at runtime. The result feeds {@code date_format(value, <fmt>)}.
 *
 * <p>The table mirrors the SQL plugin's {@code DateTimeFunctions} GET_FORMAT map. {@code DATETIME}
 * is an alias of {@code TIMESTAMP}. An unknown {@code (type, region)} pair leaves the call
 * unchanged so the downstream planner surfaces a loud error rather than a wrong format.
 *
 * @opensearch.internal
 */
class GetFormatAdapter implements ScalarFunctionAdapter {

    private static final Map<String, String> FORMATS = Map.ofEntries(
        Map.entry("date|usa", "%m.%d.%Y"),
        Map.entry("date|jis", "%Y-%m-%d"),
        Map.entry("date|iso", "%Y-%m-%d"),
        Map.entry("date|eur", "%d.%m.%Y"),
        Map.entry("date|internal", "%Y%m%d"),
        Map.entry("time|usa", "%h:%i:%s %p"),
        Map.entry("time|jis", "%H:%i:%s"),
        Map.entry("time|iso", "%H:%i:%s"),
        Map.entry("time|eur", "%H.%i.%s"),
        Map.entry("time|internal", "%H%i%s"),
        Map.entry("timestamp|usa", "%Y-%m-%d %H.%i.%s"),
        Map.entry("timestamp|jis", "%Y-%m-%d %H:%i:%s"),
        Map.entry("timestamp|iso", "%Y-%m-%d %H:%i:%s"),
        Map.entry("timestamp|eur", "%Y-%m-%d %H.%i.%s"),
        Map.entry("timestamp|internal", "%Y%m%d%H%i%s")
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        String type = stringLiteral(original.getOperands().get(0));
        String region = stringLiteral(original.getOperands().get(1));
        if (type == null || region == null) {
            return original;
        }
        // DATETIME is an alias of TIMESTAMP.
        String typeKey = type.toLowerCase(Locale.ROOT);
        if (typeKey.equals("datetime")) {
            typeKey = "timestamp";
        }
        String format = FORMATS.get(typeKey + "|" + region.toLowerCase(Locale.ROOT));
        if (format == null) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeLiteral(format, original.getType(), true);
    }

    private static String stringLiteral(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) {
            return null;
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
            return null;
        }
        return literal.getValueAs(String.class);
    }
}
