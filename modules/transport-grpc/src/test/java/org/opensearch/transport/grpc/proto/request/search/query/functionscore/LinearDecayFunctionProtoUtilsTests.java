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

public class LinearDecayFunctionProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithNumericPlacement() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(100.0)
            .setScale(50.0)
            .setOffset(5.0)
            .setDecay(0.2)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("distance", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).setWeight(1.0f).build();

        ScoreFunctionBuilder<?> result = LinearDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("distance", linearFunction.getFieldName());
    }

    public void testFromProtoWithDatePlacement() {
        org.opensearch.protobufs.DateDecayPlacement datePlacement = org.opensearch.protobufs.DateDecayPlacement.newBuilder()
            .setOrigin("2024-01-01")
            .setScale("30d")
            .setOffset("5d")
            .setDecay(0.35)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setDateDecayPlacement(datePlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("timestamp", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).build();

        ScoreFunctionBuilder<?> result = LinearDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("timestamp", linearFunction.getFieldName());
    }

    public void testFromProtoWithDatePlacementWithoutDecay() {
        org.opensearch.protobufs.DateDecayPlacement datePlacement = org.opensearch.protobufs.DateDecayPlacement.newBuilder()
            .setOrigin("now")
            .setScale("7d")
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setDateDecayPlacement(datePlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("date_field", decayPlacement).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).build();

        ScoreFunctionBuilder<?> result = LinearDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("date_field", linearFunction.getFieldName());
    }

    public void testFromProtoWithNullDecayFunction() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> LinearDecayFunctionProtoUtils.fromProto(null)
        );

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> LinearDecayFunctionProtoUtils.fromProto(decayFunction)
        );

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithUnsetDecayPlacement() {
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("field", decayPlacement).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> LinearDecayFunctionProtoUtils.fromProto(decayFunction)
        );

        assertThat(exception.getMessage(), containsString("Unsupported decay placement type"));
    }

    public void testFromProtoWithGeoPlacement() {
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

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).build();

        ScoreFunctionBuilder<?> result = LinearDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("location", linearFunction.getFieldName());
    }

    public void testFromProtoWithGeoPlacementWithoutDecay() {
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

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setLinear(decayFunction).build();

        ScoreFunctionBuilder<?> result = LinearDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("geo_field", linearFunction.getFieldName());
    }

    public void testFromProtoWithNumericPlacementWithoutDecay() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(75.0).setScale(25.0).setOffset(5.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("price", decayPlacement).build();

        ScoreFunctionBuilder<?> result = LinearDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(LinearDecayFunctionBuilder.class));
        LinearDecayFunctionBuilder linearFunction = (LinearDecayFunctionBuilder) result;

        assertEquals("price", linearFunction.getFieldName());
    }

}
