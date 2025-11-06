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
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ExpDecayFunctionProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithNumericDecayPlacement() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(100.0)
            .setScale(50.0)
            .setOffset(10.0)
            .setDecay(0.5)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("distance", decayPlacement).build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
    }

    public void testFromProtoWithDecayPlacementWithoutOptionalFields() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(100.0).setScale(50.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("distance", decayPlacement).build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
    }

    public void testFromProtoWithNullDecayFunction() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ExpDecayFunctionProtoUtils.fromProto(null));
        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithEmptyPlacements() {
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ExpDecayFunctionProtoUtils.fromProto(decayFunction)
        );
        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }
}
