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
 * Converts an {@link IdsQueryBuilder} to a Calcite RexCall suitable for Lucene delegation.
 *
 * <p>Produces a FIELDLESS call shaped as {@code IDS(MAP('values.0', 'id0'), MAP('values.1', 'id1'), ...)}
 * where each id is a separate indexed MAP key to preserve lossless multi-value encoding
 * (ids may legally contain commas). No field operand is emitted because the ids query operates
 * on the implicit _id metadata field which is not part of the user-visible row type.
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

        // Reject unsupported parameters.
        // Audit: IdsQueryBuilder exposes ids(), boost(), queryName(). No types field exists
        // (removed in OpenSearch 2.0; strict ObjectParser rejects it at parse time).
        if (idsQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Ids query does not support non-default boost");
        }
        if (idsQuery.queryName() != null) {
            throw new ConversionException("Ids query does not support _name");
        }

        // Empty values → match-nothing.
        // WHY: mirrors IdsQueryBuilder.doRewrite line 157 which replaces with MatchNoneQueryBuilder.
        Set<String> ids = idsQuery.ids();
        if (ids.isEmpty()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        RexBuilder rex = ctx.getRexBuilder();

        // Build RexCall: IDS(MAP('values.0', 'id0'), MAP('values.1', 'id1'), ...)
        // WHY indexed MAP keys: the only lossless multi-value encoding that travels through
        // the existing extractOptionalParams contract (string keys + string values in MAP pairs).
        // Comma-joining is lossy for ids containing commas.
        // WHY no field operand: the ids query targets the implicit _id metadata field which is
        // not part of the user-visible Calcite row type. OpenSearchFilterRule's FULL_TEXT
        // no-field-reference path routes this to capability matching against FieldType.TEXT.
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
