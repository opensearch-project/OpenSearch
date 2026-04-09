/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;

/**
 * Handles text equality predicates: {@code field = 'value'} → {@link TermQueryBuilder}.
 */
public class EqualsPredicateHandler implements PredicateHandler {

    @Override
    public SqlKind sqlKind() {
        return SqlKind.EQUALS;
    }

    @Override
    public SqlOperator sqlOperator() {
        return SqlStdOperatorTable.EQUALS;
    }

    @Override
    public boolean canHandle(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        if (call.getKind() != SqlKind.EQUALS) {
            return false;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        RexInputRef fieldRef;
        if (left instanceof RexInputRef && right instanceof RexLiteral) {
            fieldRef = (RexInputRef) left;
        } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
            fieldRef = (RexInputRef) right;
        } else {
            return false;
        }

        SqlTypeName fieldType = inputRowType.getFieldList().get(fieldRef.getIndex()).getType().getSqlTypeName();
        return fieldType == SqlTypeName.VARCHAR || fieldType == SqlTypeName.CHAR;
    }

    @Override
    public QueryBuilder convert(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        RexInputRef fieldRef;
        RexLiteral literal;
        if (left instanceof RexInputRef && right instanceof RexLiteral) {
            fieldRef = (RexInputRef) left;
            literal = (RexLiteral) right;
        } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
            fieldRef = (RexInputRef) right;
            literal = (RexLiteral) left;
        } else {
            throw new IllegalArgumentException(
                "EQUALS operands must be a field reference and a literal, got: "
                    + left.getClass().getSimpleName() + ", " + right.getClass().getSimpleName()
            );
        }

        String fieldName = inputRowType.getFieldList().get(fieldRef.getIndex()).getName();
        String value = literal.getValueAs(String.class);
        return new TermQueryBuilder(fieldName, value);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> namedWriteableEntries() {
        return List.of(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
        );
    }
}
