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
import org.apache.calcite.util.Sarg;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@code SEARCH(col, Sarg[...])} — Calcite's fold of {@code IN}, {@code BETWEEN}, and
 * same-field range unions. Delegated as a <em>performance</em> pre-filter, so the Lucene query need
 * only be a <b>superset</b> of matches (DataFusion re-verifies the exact predicate).
 *
 * <ul>
 *   <li><b>Points</b> ({@code col IN (a, b, c)}) → {@link TermsQueryBuilder} — exact set match.</li>
 *   <li><b>Intervals</b> ({@code col BETWEEN x AND y}, range unions) → a {@link RangeQueryBuilder} per
 *       range (multiple ranges wrapped in a {@code should} bool), open/closed bounds preserved.</li>
 * </ul>
 *
 * <p>Numeric/date fields can't reach here — the Lucene secondary only indexes keyword/text, so the
 * capability check never marks Lucene viable for a Sarg on a numeric field.
 *
 * <p>Refuses (throws → falls back to native DataFusion) the cases that aren't a safe standalone
 * superset: {@code NOT IN} ({@link Sarg#isComplementedPoints()}), {@link Sarg#isAll()},
 * {@link Sarg#isNone()}. The performance-delegation path tolerates a throwing serializer by leaving
 * the predicate on the driving engine.
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
        if (sarg.isComplementedPoints() || sarg.isAll() || sarg.isNone()) {
            // NOT IN / all / none aren't a safe standalone superset — leave on DataFusion.
            throw new IllegalArgumentException("Sarg shape not delegatable as a superset pre-filter: " + sarg);
        }

        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        RelDataType type = sargLit.getType();
        List<Range> ranges = new ArrayList<>(sarg.rangeSet.asRanges());

        if (sarg.isPoints()) {
            List<Object> values = new ArrayList<>(ranges.size());
            for (Range r : ranges) {
                values.add(convert(r.lowerEndpoint(), type));
            }
            return new TermsQueryBuilder(fieldName, values);
        }

        if (ranges.size() == 1) {
            return rangeQuery(fieldName, ranges.get(0), type);
        }
        BoolQueryBuilder bool = new BoolQueryBuilder();
        for (Range r : ranges) {
            bool.should(rangeQuery(fieldName, r, type));
        }
        return bool;
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
