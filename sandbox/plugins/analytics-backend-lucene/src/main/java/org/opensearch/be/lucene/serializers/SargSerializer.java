/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.util.Sarg;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Serializer for {@code SEARCH(col, Sarg[...])} — Calcite's fold of {@code IN}, {@code NOT IN},
 * {@code BETWEEN} and same-field range unions. The query must be exact: when Lucene owns the
 * predicate its result is authoritative. Field targets follow vanilla OpenSearch's pushdown:
 *
 * <ul>
 *   <li><b>Points</b> ({@code col IN (a, b)}) → {@link TermsQueryBuilder} on the exact-match field
 *       (a text field's keyword multifield).</li>
 *   <li><b>Complemented points</b> ({@code col NOT IN (a, b)}) → {@code exists(col)} plus
 *       {@code must_not terms}, the same shape as {@code !=}.</li>
 *   <li><b>Intervals</b> ({@code BETWEEN}, range unions) → a {@link RangeQueryBuilder} per range on
 *       the field itself (several ranges wrapped in a {@code should} bool).</li>
 * </ul>
 *
 * <p>When the Sarg treats NULL as a match ({@code nullAs = TRUE}), documents without the field are
 * OR-ed in. {@link Sarg#isAll()} / {@link Sarg#isNone()} are refused; Calcite folds them away.
 */
public class SargSerializer extends AbstractQuerySerializer {

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2
            || !(operands.get(0) instanceof RexInputRef columnRef)
            || !(operands.get(1) instanceof RexLiteral sargLit)) {
            throw new IllegalArgumentException("SEARCH delegation requires SEARCH($colIdx, Sarg literal); got " + call);
        }
        if (!(sargLit.getValue() instanceof Sarg sarg)) {
            throw new IllegalArgumentException("SEARCH second operand is not a Sarg literal: " + sargLit);
        }
        if (sarg.isAll() || sarg.isNone()) {
            throw new IllegalArgumentException("Sarg shape not delegatable: " + sarg);
        }
        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        RelDataType type = sargLit.getType();
        QueryBuilder query;
        if (sarg.isPoints()) {
            query = new TermsQueryBuilder(resolveFieldName(field), pointValues(sarg.rangeSet.asRanges(), type));
        } else if (sarg.isComplementedPoints()) {
            query = new BoolQueryBuilder().filter(new ExistsQueryBuilder(field.getFieldName()))
                .mustNot(new TermsQueryBuilder(resolveFieldName(field), pointValues(sarg.rangeSet.complement().asRanges(), type)));
        } else {
            List<Range> ranges = new ArrayList<>(sarg.rangeSet.asRanges());
            if (ranges.size() == 1) {
                query = rangeQuery(field.getFieldName(), ranges.get(0), type);
            } else {
                BoolQueryBuilder bool = new BoolQueryBuilder();
                for (Range r : ranges) {
                    bool.should(rangeQuery(field.getFieldName(), r, type));
                }
                query = bool;
            }
        }
        if (sarg.nullAs == RexUnknownAs.TRUE) {
            return new BoolQueryBuilder().should(query)
                .should(new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(field.getFieldName())));
        }
        return query;
    }

    @SuppressWarnings("rawtypes")
    private static List<Object> pointValues(Set<Range> points, RelDataType type) {
        List<Object> values = new ArrayList<>(points.size());
        for (Range r : points) {
            values.add(convert(r.lowerEndpoint(), type));
        }
        return values;
    }

    @SuppressWarnings("rawtypes")
    private static RangeQueryBuilder rangeQuery(String fieldName, Range range, RelDataType type) {
        RangeQueryBuilder qb = new RangeQueryBuilder(fieldName);
        if (range.hasLowerBound()) {
            qb.from(convert(range.lowerEndpoint(), type), range.lowerBoundType() == BoundType.CLOSED);
        }
        if (range.hasUpperBound()) {
            qb.to(convert(range.upperEndpoint(), type), range.upperBoundType() == BoundType.CLOSED);
        }
        return qb;
    }

    private static Object convert(Comparable<?> endpoint, RelDataType type) {
        return CalciteToOSMapperConversionUtils.sargEndpointToOpenSearchValue(endpoint, type);
    }
}
