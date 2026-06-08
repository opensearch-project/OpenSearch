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
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for SQL {@code IN (...)} predicates ({@code col IN (a, b, c)}). Compiles to a Lucene
 * {@link TermsQueryBuilder} which executes as a single TermsQuery — much cheaper than the disjunction
 * of TermQueries the convertor would otherwise produce on {@code col = a OR col = b OR col = c}.
 *
 * <p>Expected RexCall shape: {@code IN($colIdx, lit_1, lit_2, ...)}. Calcite folds large IN-lists
 * into Sarg/SEARCH; those go through the planner's expansion path before reaching here.
 */
public class InSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() < 2) {
            throw new IllegalArgumentException("IN expects column + at least one literal, got " + call.getOperands().size() + " operands");
        }
        RexNode head = call.getOperands().get(0);
        if (!(head instanceof RexInputRef columnRef)) {
            throw new IllegalArgumentException("IN performance-delegation requires column on the left, got " + head);
        }
        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        List<Object> values = new ArrayList<>(call.getOperands().size() - 1);
        for (int i = 1; i < call.getOperands().size(); i++) {
            RexNode op = call.getOperands().get(i);
            if (!(op instanceof RexLiteral lit)) {
                throw new IllegalArgumentException("IN performance-delegation requires literal operands, got " + op);
            }
            values.add(CalciteToOSMapperConversionUtils.literalToOpenSearchValue(lit));
        }
        return new TermsQueryBuilder(fieldName, values);
    }
}
