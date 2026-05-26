/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;

/**
 * Serializer for the EQUALS operator on a single column with a literal value
 * (e.g. {@code tag = 'hello'} or {@code status = 200}). Used by performance-delegation:
 * dual-viable EQUALS predicates wrap with {@code delegation_possible(...)} and the peer
 * (Lucene) needs to be able to compile them into a {@link TermQueryBuilder}.
 *
 * <p>Expected RexCall shape: {@code =($colIdx, literal)}. Either operand order is
 * accepted ({@code literal = $colIdx} also works).
 *
 * <p>The literal value is passed through to {@link TermQueryBuilder} as {@code Object}.
 * The field's mapper at {@code toQuery(qsc)} time handles type coercion (Number → numeric
 * point query, String → keyword/text term, Date string → date range, etc.) — duplicating
 * that dispatch here would be wrong.
 */
public class EqualsSerializer extends AbstractQuerySerializer {

    @Override
    protected QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("EQUALS expects 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        // Either (column, literal) or (literal, column).
        RexInputRef columnRef;
        RexLiteral valueLit;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            valueLit = r;
        } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            columnRef = r;
            valueLit = l;
        } else {
            throw new IllegalArgumentException(
                "EQUALS performance-delegation requires (RexInputRef, RexLiteral); got " + left + " = " + right
            );
        }

        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        // Calcite stores literals in canonical types (BigDecimal for ints, NlsString for
        // strings, etc.) that the OpenSearch Mapper can't parse directly. Convert at the
        // Calcite ↔ OpenSearch boundary so the mapper sees plain Java types.
        Object value = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(valueLit);
        return new TermQueryBuilder(fieldName, value);
    }
}
