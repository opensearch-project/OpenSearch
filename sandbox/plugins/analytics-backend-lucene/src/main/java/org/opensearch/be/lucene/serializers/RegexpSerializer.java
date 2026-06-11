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
import org.apache.calcite.util.NlsString;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;

import java.util.List;

/**
 * Serializer for the REGEXP full-text function ({@code regexp(field, pattern)}).
 * Produces a {@link RegexpQueryBuilder} against the field (or its keyword exact-match subfield).
 *
 * <p>Expected RexCall shape: {@code REGEXP($colIdx, patternLiteral)}.
 */
public class RegexpSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() < 2) {
            throw new IllegalArgumentException("REGEXP expects at least 2 operands (field, pattern), got " + call.getOperands().size());
        }
        RexNode fieldNode = call.getOperands().get(0);
        RexNode patternNode = call.getOperands().get(1);

        if (!(fieldNode instanceof RexInputRef columnRef)) {
            throw new IllegalArgumentException("REGEXP first operand must be a column reference, got " + fieldNode);
        }
        if (!(patternNode instanceof RexLiteral patternLit)) {
            throw new IllegalArgumentException("REGEXP second operand must be a literal pattern, got " + patternNode);
        }

        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        String fieldName = field.getExactMatchSubfield() != null
            ? field.getFieldName() + "." + field.getExactMatchSubfield()
            : field.getFieldName();

        Object raw = patternLit.getValue2();
        String pattern;
        if (raw instanceof NlsString nls) {
            pattern = nls.getValue();
        } else if (raw instanceof String s) {
            pattern = s;
        } else {
            pattern = raw != null ? raw.toString() : "";
        }

        return new RegexpQueryBuilder(fieldName, pattern);
    }
}
