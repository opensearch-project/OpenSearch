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
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class FieldValueFactorFunctionProtoConverterTests extends OpenSearchTestCase {

    private FieldValueFactorFunctionProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new FieldValueFactorFunctionProtoConverter();
    }

    public void testGetHandledFunctionCase() {
        assertEquals(FunctionScoreContainer.FunctionScoreContainerCase.FIELD_VALUE_FACTOR, converter.getHandledFunctionCase());
    }

    public void testFromProtoWithValidFieldValueFactorFunction() {
        // Create a field value factor function
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("popularity")
            .setFactor(1.2f)
            .setMissing(1.0f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setFieldValueFactor(fieldValueFactor)
            .setWeight(2.0f)
            .build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithMinimalParameters() {
        // Create a minimal field value factor function (only field required)
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder().setField("score").build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setFieldValueFactor(fieldValueFactor).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithAllParameters() {
        // Create a field value factor function with all parameters
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("rating")
            .setFactor(2.5f)
            .setMissing(0.5f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_SQRT)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setFieldValueFactor(fieldValueFactor)
            .setWeight(1.5f)
            .build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;
    }

    public void testFromProtoWithDifferentModifiers() {
        // Test LOG modifier
        FieldValueFactorScoreFunction logFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("views")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        FunctionScoreContainer logContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(logFunction).build();

        ScoreFunctionBuilder<?> logResult = converter.fromProto(logContainer);
        assertThat(logResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LOG1P modifier
        FieldValueFactorScoreFunction log1pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("clicks")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG1P)
            .build();

        FunctionScoreContainer log1pContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(log1pFunction).build();

        ScoreFunctionBuilder<?> log1pResult = converter.fromProto(log1pContainer);
        assertThat(log1pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LOG2P modifier
        FieldValueFactorScoreFunction log2pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("likes")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG2P)
            .build();

        FunctionScoreContainer log2pContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(log2pFunction).build();

        ScoreFunctionBuilder<?> log2pResult = converter.fromProto(log2pContainer);
        assertThat(log2pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LN modifier
        FieldValueFactorScoreFunction lnFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("shares")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LN)
            .build();

        FunctionScoreContainer lnContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(lnFunction).build();

        ScoreFunctionBuilder<?> lnResult = converter.fromProto(lnContainer);
        assertThat(lnResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LN1P modifier
        FieldValueFactorScoreFunction ln1pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("comments")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LN1P)
            .build();

        FunctionScoreContainer ln1pContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(ln1pFunction).build();

        ScoreFunctionBuilder<?> ln1pResult = converter.fromProto(ln1pContainer);
        assertThat(ln1pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LN2P modifier
        FieldValueFactorScoreFunction ln2pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("downloads")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LN2P)
            .build();

        FunctionScoreContainer ln2pContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(ln2pFunction).build();

        ScoreFunctionBuilder<?> ln2pResult = converter.fromProto(ln2pContainer);
        assertThat(ln2pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test SQUARE modifier
        FieldValueFactorScoreFunction squareFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("revenue")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_SQUARE)
            .build();

        FunctionScoreContainer squareContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(squareFunction).build();

        ScoreFunctionBuilder<?> squareResult = converter.fromProto(squareContainer);
        assertThat(squareResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test SQRT modifier
        FieldValueFactorScoreFunction sqrtFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("distance")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_SQRT)
            .build();

        FunctionScoreContainer sqrtContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(sqrtFunction).build();

        ScoreFunctionBuilder<?> sqrtResult = converter.fromProto(sqrtContainer);
        assertThat(sqrtResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test RECIPROCAL modifier
        FieldValueFactorScoreFunction reciprocalFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("time")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_RECIPROCAL)
            .build();

        FunctionScoreContainer reciprocalContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(reciprocalFunction).build();

        ScoreFunctionBuilder<?> reciprocalResult = converter.fromProto(reciprocalContainer);
        assertThat(reciprocalResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test NONE modifier
        FieldValueFactorScoreFunction noneFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("count")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_NONE)
            .build();

        FunctionScoreContainer noneContainer = FunctionScoreContainer.newBuilder().setFieldValueFactor(noneFunction).build();

        ScoreFunctionBuilder<?> noneResult = converter.fromProto(noneContainer);
        assertThat(noneResult, instanceOf(FieldValueFactorFunctionBuilder.class));
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a FieldValueFactorScoreFunction"));
    }

    public void testFromProtoWithWrongFunctionType() {
        // Create a container with a different function type
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setWeight(1.0f) // Only weight, no specific function
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a FieldValueFactorScoreFunction"));
    }

    public void testFromProtoWithEmptyFieldName() {
        // Create a field value factor function with empty field name
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("") // Empty field name
            .setFactor(1.0f)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setFieldValueFactor(fieldValueFactor).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithZeroFactor() {
        // Test with zero factor
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("zero_field")
            .setFactor(0.0f)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setFieldValueFactor(fieldValueFactor).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;
    }

    public void testFromProtoWithNegativeFactor() {
        // Test with negative factor
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("negative_field")
            .setFactor(-1.5f)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setFieldValueFactor(fieldValueFactor).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithZeroMissing() {
        // Test with zero missing value
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("missing_zero")
            .setMissing(0.0f)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setFieldValueFactor(fieldValueFactor).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithNegativeMissing() {
        // Test with negative missing value
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("missing_negative")
            .setMissing(-0.5f)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setFieldValueFactor(fieldValueFactor).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }
}
