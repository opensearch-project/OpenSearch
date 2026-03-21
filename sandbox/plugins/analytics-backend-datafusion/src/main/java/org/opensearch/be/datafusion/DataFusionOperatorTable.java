/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.List;

/**
 * Declares the aggregate functions that {@link SandboxDataFusionBridge} can convert to Substrait.
 *
 * <p>This list MUST stay in sync with {@code aggSigs} in {@link SandboxDataFusionBridge}.
 * Only aggregate functions that the bridge can actually serialize are declared here —
 * declaring more would cause false capability claims in the {@code BackendCapabilityRegistry}.
 *
 * <p>Requirements: 7.5
 */
public final class DataFusionOperatorTable implements SqlOperatorTable {

    private static final List<SqlOperator> AGG_OPERATORS = List.of(
        SqlStdOperatorTable.COUNT,
        SqlStdOperatorTable.SUM,
        SqlStdOperatorTable.SUM0,
        SqlStdOperatorTable.MIN,
        SqlStdOperatorTable.MAX,
        SqlStdOperatorTable.AVG,
        SqlStdOperatorTable.STDDEV,
        SqlStdOperatorTable.STDDEV_POP,
        SqlStdOperatorTable.STDDEV_SAMP,
        SqlStdOperatorTable.VARIANCE,
        SqlStdOperatorTable.VAR_POP,
        SqlStdOperatorTable.VAR_SAMP
    );

    private final SqlOperatorTable delegate = SqlOperatorTables.of(AGG_OPERATORS);

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        SqlFunctionCategory category,
                                        SqlSyntax syntax,
                                        List<SqlOperator> operatorList,
                                        SqlNameMatcher nameMatcher) {
        delegate.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        return delegate.getOperatorList();
    }
}
