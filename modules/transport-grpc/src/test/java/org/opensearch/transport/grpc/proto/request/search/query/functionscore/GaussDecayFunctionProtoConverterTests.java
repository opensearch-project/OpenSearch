/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.GaussDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.DecayFunction;
import org.opensearch.protobufs.DecayPlacement;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class GaussDecayFunctionProtoConverterTests extends OpenSearchTestCase {

    private GaussDecayFunctionProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new GaussDecayFunctionProtoConverter();
    }

    public void testGetHandledFunctionCase() {
        assertEquals(FunctionScoreContainer.FunctionScoreContainerCase.GAUSS, converter.getHandledFunctionCase());
    }

    public void testFromProtoWithValidGaussianDecayFunction() {
        // Create a numeric decay placement
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(20.0)
            .setScale(10.0)
            .setOffset(2.0)
            .setDecay(0.4)
            .build();

        // Create a decay placement
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        // Create a decay function with the placement
        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("rating", decayPlacement).build();

        // Create a function score container with the gaussian decay function
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(2.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        // Verify the field name
        assertEquals("rating", gaussFunction.getFieldName());
    }

    public void testFromProtoWithMultiplePlacements() {
        // Create numeric decay placements for different fields
        NumericDecayPlacement pricePlacement = NumericDecayPlacement.newBuilder().setOrigin(100.0).setScale(50.0).setDecay(0.5).build();

        NumericDecayPlacement popularityPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(1000.0)
            .setScale(500.0)
            .setDecay(0.3)
            .build();

        // Create decay placements
        DecayPlacement priceDecayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(pricePlacement).build();

        DecayPlacement popularityDecayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(popularityPlacement).build();

        // Create a decay function with multiple placements
        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("price", priceDecayPlacement)
            .putPlacement("popularity", popularityDecayPlacement)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(1.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        // Should use the first placement (price)
        assertEquals("price", gaussFunction.getFieldName());
    }

    public void testFromProtoWithMinimalParameters() {
        // Create a minimal numeric decay placement (only origin and scale required)
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(15.0).setScale(7.5).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("score", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("score", gaussFunction.getFieldName());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a GaussDecayFunction"));
    }

    public void testFromProtoWithWrongFunctionType() {
        // Create a container with a different function type
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setWeight(1.0f) // Only weight, no specific function
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a GaussDecayFunction"));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        // Create an empty decay function (no placements)
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(1.0f).build();

        // This should throw an exception because decay function has no placements
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithUnsetDecayPlacement() {
        // Create a decay placement with no specific type set
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(1.0f).build();

        // This should throw an exception because decay placement has no valid type
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("Unsupported decay placement type"));
    }

    public void testFromProtoWithAllNumericParameters() {
        // Create a numeric decay placement with all parameters
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(75.0)
            .setScale(37.5)
            .setOffset(10.0)
            .setDecay(0.6)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("quality", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(2.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("quality", gaussFunction.getFieldName());
    }

    public void testFromProtoWithZeroValues() {
        // Test with zero values to ensure they're handled correctly
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(0.0)
            .setScale(1.0)
            .setOffset(0.0)
            .setDecay(0.5) // Valid decay value (must be in range 0..1)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("zero_field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(0.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("zero_field", gaussFunction.getFieldName());
    }
}
