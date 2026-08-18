/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.opensearch.core.ParseField;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.List;
import java.util.Optional;

/**
 * Converts a {@link TermsQueryBuilder} to a Calcite IN RexNode.
 *
 * <p>For {@code scaled_float} fields, each value is scaled via {@code Math.round(value * factor)}
 * before the IN comparison — mirroring
 * {@code ScaledFloatFieldMapper.ScaledFloatFieldType.termsQuery}.
 */
public class TermsQueryTranslator implements QueryTranslator {

    private static final TranslatorMapperRegistry REGISTRY = TranslatorMapperRegistry.INSTANCE;

    /** Reason-code prefix, derived from the query name so it tracks {@link TermsQueryBuilder#NAME}. */
    private static final String REASON_PREFIX = TermsQueryBuilder.NAME + ".";

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return TermsQueryBuilder.class;
    }

    @Override
    public ValidationResult validate(QueryBuilder query) {
        TermsQueryBuilder termsQuery = (TermsQueryBuilder) query;

        if (termsQuery.termsLookup() != null) {
            return ValidationResult.rejected(reasonCode(TermsQueryBuilder.TERMS_LOOKUP_FIELD), "Terms query does not support terms lookup");
        }
        if (termsQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return ValidationResult.rejected(
                reasonCode(AbstractQueryBuilder.BOOST_FIELD),
                "Terms query does not support non-default boost"
            );
        }
        if (termsQuery.queryName() != null) {
            return ValidationResult.rejected(reasonCode(AbstractQueryBuilder.NAME_FIELD), "Terms query does not support _name");
        }
        if (termsQuery.valueType() != TermsQueryBuilder.ValueType.DEFAULT) {
            return ValidationResult.rejected(
                reasonCode(TermsQueryBuilder.VALUE_TYPE_FIELD) + ":" + termsQuery.valueType(),
                "Terms query does not support non-default value_type"
            );
        }

        List<?> values = termsQuery.values();
        if (values == null || values.isEmpty()) {
            return ValidationResult.rejected(REASON_PREFIX + "no_values", "Terms query must have values");
        }

        return ValidationResult.accepted();
    }

    /** Builds a stable reason code {@code terms.<field>} from a query-parameter {@link ParseField}. */
    private static String reasonCode(ParseField field) {
        return REASON_PREFIX + field.getPreferredName();
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

        RelDataType fieldType = field.getType();
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(fieldType, field.getIndex());

        BaseTranslatorMapper mapper = REGISTRY.resolve(fieldType);
        List<RexNode> literals = new java.util.ArrayList<>();
        for (Object value : values) {
            Optional<RexNode> literal = mapper.toTermLiteral(value, field, ctx);
            literal.ifPresent(literals::add);
        }
        if (literals.isEmpty()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }
        return ctx.getRexBuilder().makeIn(fieldRef, literals);
    }

}
