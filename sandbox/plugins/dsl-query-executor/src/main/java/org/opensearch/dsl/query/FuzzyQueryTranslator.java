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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link FuzzyQueryBuilder} to a FUZZY RexCall with MAP operands.
 */
public class FuzzyQueryTranslator implements QueryTranslator {

    private static final SqlFunction FUZZY_FUNCTION = new SqlFunction(
        "FUZZY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** Creates a new fuzzy query translator. */
    public FuzzyQueryTranslator() {}

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return FuzzyQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        FuzzyQueryBuilder fuzzyQuery = (FuzzyQueryBuilder) query;

        ValidatedPayload payload = validateQuery(fuzzyQuery, ctx);

        // Build the RexCall: FUZZY(MAP('field',$ref), MAP('query',literal), [MAP(param,value)]...)
        RelDataTypeField field = payload.field();
        String value = payload.value();
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        List<RexNode> operands = new ArrayList<>();
        // Operand 0: MAP('field', $inputRef)
        operands.add(
            ctx.getRexBuilder().makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, ctx.getRexBuilder().makeLiteral("field"), fieldRef)
        );
        // Operand 1: MAP('query', value)
        operands.add(
            ctx.getRexBuilder()
                .makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    ctx.getRexBuilder().makeLiteral("query"),
                    ctx.getRexBuilder().makeLiteral(value)
                )
        );

        // Optional params — emit only non-defaults
        Fuzziness fuzziness = fuzzyQuery.fuzziness();
        String fuzzinessStr = fuzziness.asString();
        if (!fuzziness.equals(FuzzyQueryBuilder.DEFAULT_FUZZINESS)) {
            operands.add(makeParamMap(ctx, "fuzziness", fuzzinessStr));
        }
        int prefixLength = fuzzyQuery.prefixLength();
        if (prefixLength != FuzzyQueryBuilder.DEFAULT_PREFIX_LENGTH) {
            operands.add(makeParamMap(ctx, "prefix_length", String.valueOf(prefixLength)));
        }
        int maxExpansions = fuzzyQuery.maxExpansions();
        if (maxExpansions != FuzzyQueryBuilder.DEFAULT_MAX_EXPANSIONS) {
            operands.add(makeParamMap(ctx, "max_expansions", String.valueOf(maxExpansions)));
        }
        if (fuzzyQuery.transpositions() != FuzzyQueryBuilder.DEFAULT_TRANSPOSITIONS) {
            operands.add(makeParamMap(ctx, "transpositions", String.valueOf(fuzzyQuery.transpositions())));
        }
        if (fuzzyQuery.rewrite() != null) {
            operands.add(makeParamMap(ctx, "rewrite", fuzzyQuery.rewrite()));
        }

        return ctx.getRexBuilder().makeCall(FUZZY_FUNCTION, operands);
    }

    /** Holds the field reference and query value resolved during validation. */
    private record ValidatedPayload(RelDataTypeField field, String value) {
    }

    private ValidatedPayload validateQuery(FuzzyQueryBuilder fuzzyQuery, ConversionContext ctx) throws ConversionException {
        // Reject unsupported params — FuzzyQueryBuilder.boost() (scoring-only in delegated predicate)
        if (fuzzyQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Fuzzy query does not support non-default boost");
        }
        // Reject _name — diagnostic metadata only (TermsQueryTranslator.java:43-44)
        if (fuzzyQuery.queryName() != null) {
            throw new ConversionException("Fuzzy query does not support _name");
        }

        // Extract and validate payload
        String fieldName = fuzzyQuery.fieldName();
        Object valueObj = fuzzyQuery.value();
        if (valueObj == null) {
            throw new ConversionException("Fuzzy query value must not be null");
        }
        String value = valueObj.toString();
        if (value.isEmpty()) {
            throw new ConversionException("Fuzzy query value must not be empty");
        }

        // Resolve field and gate on type
        RelDataTypeField field = ctx.getField(fieldName);
        SqlTypeName typeName = field.getType().getSqlTypeName();
        if (typeName != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Fuzzy query requires a keyword or text field, got " + typeName + " for field '" + fieldName + "'"
            );
        }

        // Validate numeric params fail-fast
        int prefixLength = fuzzyQuery.prefixLength();
        if (prefixLength < 0) {
            throw new ConversionException("Fuzzy query prefix_length must not be negative, got " + prefixLength);
        }
        int maxExpansions = fuzzyQuery.maxExpansions();
        if (maxExpansions < 1) {
            throw new ConversionException("Fuzzy query max_expansions must be at least 1, got " + maxExpansions);
        }

        // Validate fuzziness fail-fast by delegating to Fuzziness.build() so we stay in sync
        // with server semantics (Fuzziness.build at Fuzziness.java:131), then verify asDistance()
        // succeeds since non-numeric values like "abc" pass build() but fail at query time
        // (StringFieldType.fuzzyQuery at StringFieldType.java:103).
        Fuzziness fuzziness = fuzzyQuery.fuzziness();
        String fuzzinessStr = fuzziness.asString();
        try {
            Fuzziness validated = Fuzziness.build(fuzzinessStr);
            validated.asDistance(value);
        } catch (IllegalArgumentException e) {
            throw new ConversionException("Invalid fuzziness value '" + fuzzinessStr + "': " + e.getMessage());
        }

        return new ValidatedPayload(field, value);
    }

    private RexNode makeParamMap(ConversionContext ctx, String key, String value) {
        return ctx.getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                ctx.getRexBuilder().makeLiteral(key),
                ctx.getRexBuilder().makeLiteral(value)
            );
    }
}
