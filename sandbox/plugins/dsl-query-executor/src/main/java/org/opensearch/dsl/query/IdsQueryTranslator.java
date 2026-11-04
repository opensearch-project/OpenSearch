/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Converts an {@link IdsQueryBuilder} to a fieldless {@code IDS(MAP('values.N', id), ...)} RexCall.
 * One indexed MAP operand per id because an {@code _id} may contain a comma.
 */
public class IdsQueryTranslator implements QueryTranslator {

    /** Operator registered for IDS delegated calls. Name-based resolution via ScalarFunction.IDS. */
    private static final SqlFunction IDS_OPERATOR = new SqlFunction(
        "IDS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return IdsQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        IdsQueryBuilder idsQuery = (IdsQueryBuilder) query;

        // Audit: ids(), boost(), queryName() accounted for; types rejected by strict parser before reaching here.
        if (idsQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Ids query does not support non-default boost");
        }
        if (idsQuery.queryName() != null) {
            throw new ConversionException("Ids query does not support _name");
        }

        // Mirrors IdsQueryBuilder.doRewrite rewriting empty ids to MatchNoneQueryBuilder.
        Set<String> ids = idsQuery.ids();
        if (ids.isEmpty()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        RexBuilder rex = ctx.getRexBuilder();

        // Indexed MAP keys because comma-joining is lossy for ids containing commas.
        List<RexNode> operands = new ArrayList<>(ids.size());

        // Sort for deterministic plan output (IdsQueryBuilder.ids() is a HashSet with no order guarantee).
        List<String> sortedIds = ids.stream().sorted().toList();
        int index = 0;
        for (String id : sortedIds) {
            operands.add(rex.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rex.makeLiteral("values." + index), rex.makeLiteral(id)));
            index++;
        }

        return rex.makeCall(IDS_OPERATOR, operands);
    }
}
