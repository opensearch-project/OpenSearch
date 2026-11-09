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
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class FieldValueFactorFunctionProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithValidFieldValueFactorFunction() {
        // Create a field value factor function
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("popularity")
            .setFactor(1.2f)
            .setMissing(1.0f)
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithMinimalParameters() {
        // Create a minimal field value factor function (only field required)
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder().setField("score").build();

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

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

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;
    }

    public void testFromProtoWithDifferentModifiers() {
        // Test LOG modifier
        FieldValueFactorScoreFunction logFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("views")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG)
            .build();

        ScoreFunctionBuilder<?> logResult = FieldValueFactorFunctionProtoUtils.fromProto(logFunction);
        assertThat(logResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LOG1P modifier
        FieldValueFactorScoreFunction log1pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("clicks")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG1P)
            .build();

        ScoreFunctionBuilder<?> log1pResult = FieldValueFactorFunctionProtoUtils.fromProto(log1pFunction);
        assertThat(log1pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LOG2P modifier
        FieldValueFactorScoreFunction log2pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("likes")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LOG2P)
            .build();

        ScoreFunctionBuilder<?> log2pResult = FieldValueFactorFunctionProtoUtils.fromProto(log2pFunction);
        assertThat(log2pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LN modifier
        FieldValueFactorScoreFunction lnFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("shares")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LN)
            .build();

        ScoreFunctionBuilder<?> lnResult = FieldValueFactorFunctionProtoUtils.fromProto(lnFunction);
        assertThat(lnResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LN1P modifier
        FieldValueFactorScoreFunction ln1pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("comments")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LN1P)
            .build();

        ScoreFunctionBuilder<?> ln1pResult = FieldValueFactorFunctionProtoUtils.fromProto(ln1pFunction);
        assertThat(ln1pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test LN2P modifier
        FieldValueFactorScoreFunction ln2pFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("downloads")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_LN2P)
            .build();

        ScoreFunctionBuilder<?> ln2pResult = FieldValueFactorFunctionProtoUtils.fromProto(ln2pFunction);
        assertThat(ln2pResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test SQUARE modifier
        FieldValueFactorScoreFunction squareFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("revenue")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_SQUARE)
            .build();

        ScoreFunctionBuilder<?> squareResult = FieldValueFactorFunctionProtoUtils.fromProto(squareFunction);
        assertThat(squareResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test SQRT modifier
        FieldValueFactorScoreFunction sqrtFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("distance")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_SQRT)
            .build();

        ScoreFunctionBuilder<?> sqrtResult = FieldValueFactorFunctionProtoUtils.fromProto(sqrtFunction);
        assertThat(sqrtResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test RECIPROCAL modifier
        FieldValueFactorScoreFunction reciprocalFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("time")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_RECIPROCAL)
            .build();

        ScoreFunctionBuilder<?> reciprocalResult = FieldValueFactorFunctionProtoUtils.fromProto(reciprocalFunction);
        assertThat(reciprocalResult, instanceOf(FieldValueFactorFunctionBuilder.class));

        // Test NONE modifier
        FieldValueFactorScoreFunction noneFunction = FieldValueFactorScoreFunction.newBuilder()
            .setField("count")
            .setModifier(FieldValueFactorModifier.FIELD_VALUE_FACTOR_MODIFIER_NONE)
            .build();

        ScoreFunctionBuilder<?> noneResult = FieldValueFactorFunctionProtoUtils.fromProto(noneFunction);
        assertThat(noneResult, instanceOf(FieldValueFactorFunctionBuilder.class));
    }

    public void testFromProtoWithNullFieldValueFactor() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FieldValueFactorFunctionProtoUtils.fromProto(null)
        );

        assertThat(exception.getMessage(), containsString("FieldValueFactorScoreFunction cannot be null"));
    }

    public void testFromProtoWithZeroFactor() {
        // Test with zero factor
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("zero_field")
            .setFactor(0.0f)
            .build();

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;
    }

    public void testFromProtoWithNegativeFactor() {
        // Test with negative factor
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("negative_field")
            .setFactor(-1.5f)
            .build();

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithZeroMissing() {
        // Test with zero missing value
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("missing_zero")
            .setMissing(0.0f)
            .build();

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }

    public void testFromProtoWithNegativeMissing() {
        // Test with negative missing value
        FieldValueFactorScoreFunction fieldValueFactor = FieldValueFactorScoreFunction.newBuilder()
            .setField("missing_negative")
            .setMissing(-0.5f)
            .build();

        ScoreFunctionBuilder<?> result = FieldValueFactorFunctionProtoUtils.fromProto(fieldValueFactor);

        assertThat(result, instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunction = (FieldValueFactorFunctionBuilder) result;

    }
}
