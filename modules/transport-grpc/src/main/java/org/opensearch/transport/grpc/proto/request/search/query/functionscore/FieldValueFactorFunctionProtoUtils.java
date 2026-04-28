/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.FieldValueFactorModifier;
import org.opensearch.protobufs.FieldValueFactorScoreFunction;

/**
 * Utility class for converting Protocol Buffer FieldValueFactorScoreFunction to OpenSearch objects.
 * This utility handles the transformation of Protocol Buffer FieldValueFactorScoreFunction objects
 * into OpenSearch FieldValueFactorFunctionBuilder instances.
 */
class FieldValueFactorFunctionProtoUtils {

    private FieldValueFactorFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FieldValueFactorScoreFunction to an OpenSearch ScoreFunctionBuilder.
     * Similar to {@link FieldValueFactorFunctionBuilder#fromXContent(XContentParser)}, this method
     * parses the field, factor, missing value, and modifier parameters.
     *
     * @param fieldValueFactor the Protocol Buffer FieldValueFactorScoreFunction
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the fieldValueFactor is null
     */
    static ScoreFunctionBuilder<?> fromProto(FieldValueFactorScoreFunction fieldValueFactor) {
        if (fieldValueFactor == null) {
            throw new IllegalArgumentException("FieldValueFactorScoreFunction cannot be null");
        }

        FieldValueFactorFunctionBuilder builder = new FieldValueFactorFunctionBuilder(fieldValueFactor.getField());

        if (fieldValueFactor.hasFactor()) {
            builder.factor(fieldValueFactor.getFactor());
        }

        if (fieldValueFactor.hasMissing()) {
            builder.missing(fieldValueFactor.getMissing());
        }

        if (fieldValueFactor.getModifier() != FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_NONE) {
            builder.modifier(parseFieldValueFactorModifier(fieldValueFactor.getModifier()));
        }

        return builder;
    }

    /**
     * Parses a protobuf FieldValueFactorModifier and returns the corresponding OpenSearch modifier.
     *
     * @param modifier the protobuf FieldValueFactorModifier
     * @return the corresponding OpenSearch FieldValueFactorFunction.Modifier
     */
    private static org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier parseFieldValueFactorModifier(
        FieldValueFactorModifier modifier
    ) {
        return switch (modifier) {
            case FIELD_VALUE_FACTOR_MODIFIER_NONE -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.NONE;
            case FIELD_VALUE_FACTOR_MODIFIER_LOG -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.LOG;
            case FIELD_VALUE_FACTOR_MODIFIER_LOG1P -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.LOG1P;
            case FIELD_VALUE_FACTOR_MODIFIER_LOG2P -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.LOG2P;
            case FIELD_VALUE_FACTOR_MODIFIER_LN -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.LN;
            case FIELD_VALUE_FACTOR_MODIFIER_LN1P -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.LN1P;
            case FIELD_VALUE_FACTOR_MODIFIER_LN2P -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.LN2P;
            case FIELD_VALUE_FACTOR_MODIFIER_SQUARE ->
                org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.SQUARE;
            case FIELD_VALUE_FACTOR_MODIFIER_SQRT -> org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.SQRT;
            case FIELD_VALUE_FACTOR_MODIFIER_RECIPROCAL ->
                org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.RECIPROCAL;
            case FIELD_VALUE_FACTOR_MODIFIER_UNSPECIFIED ->
                org.opensearch.common.lucene.search.function.FieldValueFactorFunction.Modifier.NONE;
            default -> throw new IllegalArgumentException("Unknown FieldValueFactorModifier: " + modifier);
        };
    }
}
