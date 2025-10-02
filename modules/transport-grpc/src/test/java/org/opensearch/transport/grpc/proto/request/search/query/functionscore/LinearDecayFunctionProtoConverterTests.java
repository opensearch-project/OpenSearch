/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.LinearDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.DecayFunction;
import org.opensearch.protobufs.DecayPlacement;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class LinearDecayFunctionProtoConverterTests extends OpenSearchTestCase {

    private LinearDecayFunctionProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new LinearDecayFunctionProtoConverter();
    }

    public void testGetHandledFunctionCase() {
        assertEquals(FunctionScoreContainer.FunctionScoreContainerCase.LINEAR, converter.getHandledFunctionCase());
    }

    public void testFromProtoWithValidLinearDecayFunction() {
        // Create a numeric decay placement
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(100.0)
            .setScale(50.0)
            .setOffset(5.0)
            .setDecay(0.2)
            .build();

        // Create a decay placement
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        // Create a decay function with the placement
        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("distance", decayPlacement).build();

        // Create a function score container with the linear decay function
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(1.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        // Verify the field name
        assertEquals("distance", linearFunction.getFieldName());
    }

    public void testFromProtoWithMultiplePlacements() {
        // Create numeric decay placements for different fields
        NumericDecayPlacement timePlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(3600.0) // 1 hour in seconds
            .setScale(1800.0)  // 30 minutes in seconds
            .setDecay(0.1)
            .build();

        NumericDecayPlacement agePlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(30.0)   // 30 days
            .setScale(15.0)    // 15 days
            .setDecay(0.05)
            .build();

        // Create decay placements
        DecayPlacement timeDecayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(timePlacement).build();

        DecayPlacement ageDecayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(agePlacement).build();

        // Create a decay function with multiple placements
        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("timestamp", timeDecayPlacement)
            .putPlacement("age_days", ageDecayPlacement)
            .build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(1.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        // Should use the first placement (timestamp)
        assertEquals("timestamp", linearFunction.getFieldName());
    }

    public void testFromProtoWithMinimalParameters() {
        // Create a minimal numeric decay placement (only origin and scale required)
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(25.0).setScale(12.5).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("value", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("value", linearFunction.getFieldName());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a LinearDecayFunction"));
    }

    public void testFromProtoWithWrongFunctionType() {
        // Create a container with a different function type
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setWeight(1.0f) // Only weight, no specific function
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a LinearDecayFunction"));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        // Create an empty decay function (no placements)
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(1.0f).build();

        // This should throw an exception because decay function has no placements
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithUnsetDecayPlacement() {
        // Create a decay placement with no specific type set
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(1.0f).build();

        // This should throw an exception because decay placement has no valid type
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));

        assertThat(exception.getMessage(), containsString("Unsupported decay placement type"));
    }

    public void testFromProtoWithAllNumericParameters() {
        // Create a numeric decay placement with all parameters
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(200.0)
            .setScale(100.0)
            .setOffset(20.0)
            .setDecay(0.15)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("performance", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(3.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("performance", linearFunction.getFieldName());
    }

    public void testFromProtoWithLargeValues() {
        // Test with large values to ensure they're handled correctly
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(1000000.0)
            .setScale(500000.0)
            .setOffset(100000.0)
            .setDecay(0.01)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("large_field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(0.1f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("large_field", linearFunction.getFieldName());
    }

    public void testFromProtoWithSmallValues() {
        // Test with small values to ensure they're handled correctly
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(0.001)
            .setScale(0.0005)
            .setOffset(0.0001)
            .setDecay(0.8)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("small_field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(5.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("small_field", linearFunction.getFieldName());
    }
}
