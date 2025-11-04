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
import org.opensearch.protobufs.FunctionScoreContainer;

/**
 * Protocol Buffer converter for FieldValueFactorScoreFunction.
 * This converter handles the transformation of Protocol Buffer FieldValueFactorScoreFunction objects
 * into OpenSearch FieldValueFactorFunctionBuilder instances.
 */
public class FieldValueFactorFunctionProtoConverter {

    /**
     * Default constructor for FieldValueFactorFunctionProtoConverter.
     */
    public FieldValueFactorFunctionProtoConverter() {
        // Default constructor
    }

    /**
     * Returns the function score container case that this converter handles.
     *
     * @return the FIELD_VALUE_FACTOR function score container case
     */
    public FunctionScoreContainer.FunctionScoreContainerCase getHandledFunctionCase() {
        return FunctionScoreContainer.FunctionScoreContainerCase.FIELD_VALUE_FACTOR;
    }

    /**
     * Converts a Protocol Buffer FunctionScoreContainer containing a field value factor function
     * to an OpenSearch ScoreFunctionBuilder.
     *
     * @param container the Protocol Buffer FunctionScoreContainer containing the field value factor function
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the container is null or doesn't contain a FIELD_VALUE_FACTOR function
     */
    public ScoreFunctionBuilder<?> fromProto(FunctionScoreContainer container) {
        if (container == null
            || container.getFunctionScoreContainerCase() != FunctionScoreContainer.FunctionScoreContainerCase.FIELD_VALUE_FACTOR) {
            throw new IllegalArgumentException("FunctionScoreContainer must contain a FieldValueFactorScoreFunction");
        }

        return parseFieldValueFactorFunction(container.getFieldValueFactor());
    }

    /**
     * Parses a FieldValueFactorScoreFunction and creates a FieldValueFactorFunctionBuilder.
     *
     * @param fieldValueFactor the protobuf FieldValueFactorScoreFunction
     * @return the corresponding OpenSearch FieldValueFactorFunctionBuilder
     */
    private static FieldValueFactorFunctionBuilder parseFieldValueFactorFunction(FieldValueFactorScoreFunction fieldValueFactor) {
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
