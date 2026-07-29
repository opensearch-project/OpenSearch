/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;

/**
 * Base class for relevance function serializers ({@code query_string}, {@code simple_query_string},
 * {@code multi_match}). Handles the common pattern of extracting operands, validating, creating the
 * query builder, and applying optional parameters.
 *
 * <p>It also implements {@link org.opensearch.analytics.spi.DelegatedPredicateSerializer#referencedFields}
 * for these functions — the serializer is the single place that understands the predicate's operand
 * shape — but only orchestrates: it extracts the operands and delegates the actual field parsing and
 * literal/pattern classification to {@link RelevanceFieldExtractor} (which decides per function whether
 * to parse the query string for in-string fields). Both {@link #buildQueryBuilder} and
 * {@code referencedFields} read operands through {@link ConversionUtils#extractRelevanceOperands}, so
 * plan-time field extraction can never drift from execution.
 */
public abstract class AbstractRelevanceSerializer extends AbstractQuerySerializer {

    @Override
    public final QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        validate(operands);
        QueryBuilder qb = createQueryBuilder(operands);
        Map<String, String> params = ConversionUtils.extractOptionalParams(call, optionalParamsStartIndex());
        applyParams(qb, params);
        return qb;
    }

    @Override
    public final FieldReferences referencedFields(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        Map<String, String> params = ConversionUtils.extractOptionalParams(call, optionalParamsStartIndex());
        return RelevanceFieldExtractor.referencedFields(functionName(), operands, params);
    }

    protected abstract QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands);

    protected abstract String functionName();

    protected void applyParams(QueryBuilder qb, Map<String, String> params) {}

    protected void validate(ConversionUtils.RelevanceOperands operands) {
        if (operands.fieldName() == null || operands.query() == null) {
            throw new IllegalArgumentException(functionName() + " requires 'field' and 'query' parameters");
        }
    }

    protected int optionalParamsStartIndex() {
        return 2;
    }
}
