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

    public void testFromProtoWithNumericPlacement() {
        // Test with numeric decay placement
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(20.0)
            .setScale(10.0)
            .setOffset(2.0)
            .setDecay(0.4)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("rating", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).setWeight(2.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("rating", gaussFunction.getFieldName());
    }

    public void testFromProtoWithDatePlacement() {
        // Test with DateDecayPlacement with all parameters
        org.opensearch.protobufs.DateDecayPlacement datePlacement = org.opensearch.protobufs.DateDecayPlacement.newBuilder()
            .setOrigin("2024-06-15")
            .setScale("60d")
            .setOffset("10d")
            .setDecay(0.55)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setDateDecayPlacement(datePlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("created_date", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("created_date", gaussFunction.getFieldName());
    }

    public void testFromProtoWithDatePlacementWithoutDecay() {
        // Test with DateDecayPlacement without decay parameter
        org.opensearch.protobufs.DateDecayPlacement datePlacement = org.opensearch.protobufs.DateDecayPlacement.newBuilder()
            .setOrigin("now")
            .setScale("7d")
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setDateDecayPlacement(datePlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("date_field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("date_field", gaussFunction.getFieldName());
    }

    public void testFromProtoWithGeoPlacement() {
        // Test with GeoDecayPlacement
        org.opensearch.protobufs.LatLonGeoLocation latLonLocation = org.opensearch.protobufs.LatLonGeoLocation.newBuilder()
            .setLat(40.7128)
            .setLon(-74.0060)
            .build();

        org.opensearch.protobufs.GeoLocation geoLocation = org.opensearch.protobufs.GeoLocation.newBuilder()
            .setLatlon(latLonLocation)
            .build();

        org.opensearch.protobufs.GeoDecayPlacement geoPlacement = org.opensearch.protobufs.GeoDecayPlacement.newBuilder()
            .setOrigin(geoLocation)
            .setScale("10km")
            .setOffset("1km")
            .setDecay(0.5)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setGeoDecayPlacement(geoPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("location", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("location", gaussFunction.getFieldName());
    }

    public void testFromProtoWithGeoPlacementWithoutDecay() {
        // Test with GeoDecayPlacement without decay parameter
        org.opensearch.protobufs.LatLonGeoLocation latLonLocation = org.opensearch.protobufs.LatLonGeoLocation.newBuilder()
            .setLat(51.5074)
            .setLon(-0.1278)
            .build();

        org.opensearch.protobufs.GeoLocation geoLocation = org.opensearch.protobufs.GeoLocation.newBuilder()
            .setLatlon(latLonLocation)
            .build();

        org.opensearch.protobufs.GeoDecayPlacement geoPlacement = org.opensearch.protobufs.GeoDecayPlacement.newBuilder()
            .setOrigin(geoLocation)
            .setScale("5km")
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setGeoDecayPlacement(geoPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("geo_field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setGauss(decayFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("geo_field", gaussFunction.getFieldName());
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
}
