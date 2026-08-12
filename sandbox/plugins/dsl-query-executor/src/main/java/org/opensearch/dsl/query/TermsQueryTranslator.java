/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a {@link TermsQueryBuilder} to a Calcite IN RexNode.
 */
public class TermsQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return TermsQueryBuilder.class;
    }

    @Override
    public ValidationResult validate(QueryBuilder query) {
        TermsQueryBuilder termsQuery = (TermsQueryBuilder) query;

        if (termsQuery.termsLookup() != null) {
            return ValidationResult.rejected("terms.terms_lookup", "Terms query does not support terms lookup");
        }
        if (termsQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return ValidationResult.rejected("terms.boost", "Terms query does not support non-default boost");
        }
        if (termsQuery.queryName() != null) {
            return ValidationResult.rejected("terms.name", "Terms query does not support _name");
        }
        if (termsQuery.valueType() != TermsQueryBuilder.ValueType.DEFAULT) {
            return ValidationResult.rejected(
                "terms.value_type:" + termsQuery.valueType(),
                "Terms query does not support non-default value_type"
            );
        }

        List<?> values = termsQuery.values();
        if (values == null || values.isEmpty()) {
            return ValidationResult.rejected("terms.no_values", "Terms query must have values");
        }

        return ValidationResult.accepted();
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) query;
        ValidationResult validationResult = validate(termsQuery);
        if (!validationResult.isAccepted()) {
            throw new ConversionException(validationResult.message());
        }

        String fieldName = termsQuery.fieldName();
        List<?> values = termsQuery.values();

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        List<RexNode> literals = values.stream()
            .map(value -> ctx.getRexBuilder().makeLiteral(value, field.getType(), true))
            .collect(Collectors.toList());

        return ctx.getRexBuilder().makeIn(fieldRef, literals);
    }
}
