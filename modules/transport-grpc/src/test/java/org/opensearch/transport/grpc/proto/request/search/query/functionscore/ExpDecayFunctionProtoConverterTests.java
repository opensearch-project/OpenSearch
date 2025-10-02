/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.ExponentialDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.DecayFunction;
import org.opensearch.protobufs.DecayPlacement;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ExpDecayFunctionProtoConverterTests extends OpenSearchTestCase {

    private ExpDecayFunctionProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new ExpDecayFunctionProtoConverter();
    }

    public void testGetHandledFunctionCase() {
        assertEquals(FunctionScoreContainer.FunctionScoreContainerCase.EXP, converter.getHandledFunctionCase());
    }

    public void testFromProtoWithValidExponentialDecayFunction() {
        // Create a numeric decay placement
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(10.0)
            .setScale(5.0)
            .setOffset(1.0)
            .setDecay(0.3)
            .build();

        // Create a decay placement
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        // Create a decay function with the placement
        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("price", decayPlacement).build();

        // Create a function score container with the exponential decay function
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(1.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        // Verify the field name
        assertEquals("price", expFunction.getFieldName());
    }

    public void testFromProtoWithMultiplePlacements() {
        // Create numeric decay placements for different fields
        NumericDecayPlacement pricePlacement = NumericDecayPlacement.newBuilder().setOrigin(100.0).setScale(50.0).setDecay(0.5).build();

        NumericDecayPlacement ratingPlacement = NumericDecayPlacement.newBuilder().setOrigin(5.0).setScale(2.0).setDecay(0.3).build();

        // Create decay placements
        DecayPlacement priceDecayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(pricePlacement).build();

        DecayPlacement ratingDecayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(ratingPlacement).build();

        // Create a decay function with multiple placements
        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("price", priceDecayPlacement)
            .putPlacement("rating", ratingDecayPlacement)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(2.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        // Should use the first placement (price)
        assertEquals("price", expFunction.getFieldName());
    }

    public void testFromProtoWithMinimalParameters() {
        // Create a minimal numeric decay placement (only origin and scale required)
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(20.0).setScale(10.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("distance", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("distance", expFunction.getFieldName());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain an ExpDecayFunction"));
    }

    public void testFromProtoWithWrongFunctionType() {
        // Create a container with a different function type
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setWeight(1.0f) // Only weight, no specific function
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain an ExpDecayFunction"));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        // Create an empty decay function (no placements)
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(1.0f).build();

        // This should throw an exception because decay function has no placements
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithUnsetDecayPlacement() {
        // Create a decay placement with no specific type set
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(1.0f).build();

        // This should throw an exception because decay placement has no valid type
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("Unsupported decay placement type"));
    }

    public void testFromProtoWithAllNumericParameters() {
        // Create a numeric decay placement with all parameters
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(50.0)
            .setScale(25.0)
            .setOffset(5.0)
            .setDecay(0.4)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("score", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setExp(decayFunction).setWeight(3.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("score", expFunction.getFieldName());
    }
}
