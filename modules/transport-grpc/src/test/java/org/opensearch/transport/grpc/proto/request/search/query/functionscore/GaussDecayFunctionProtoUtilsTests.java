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
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class GaussDecayFunctionProtoUtilsTests extends OpenSearchTestCase {

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

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("rating", gaussFunction.getFieldName());
    }

    public void testFromProtoWithNumericPlacementWithoutDecay() {
        // Test with numeric decay placement without decay parameter
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(50.0).setScale(25.0).setOffset(5.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("price", decayPlacement).build();

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("price", gaussFunction.getFieldName());
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

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

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

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

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

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

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

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("geo_field", gaussFunction.getFieldName());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GaussDecayFunctionProtoUtils.fromProto(null)
        );

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        // Create an empty decay function (no placements)
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        // This should throw an exception because decay function has no placements
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GaussDecayFunctionProtoUtils.fromProto(decayFunction)
        );

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithUnsetDecayPlacement() {
        // Create a decay placement with no specific type set
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("field", decayPlacement).build();

        // This should throw an exception because decay placement has no valid type
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GaussDecayFunctionProtoUtils.fromProto(decayFunction)
        );

        assertThat(exception.getMessage(), containsString("Unsupported decay placement type"));
    }

    public void testFromProtoWithMultiValueModeMin() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(20.0).setScale(10.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("rating", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_MIN)
            .build();

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("rating", gaussFunction.getFieldName());
        assertEquals(org.opensearch.search.MultiValueMode.MIN, gaussFunction.getMultiValueMode());
    }

    public void testFromProtoWithMultiValueModeMax() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(50.0).setScale(25.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("price", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_MAX)
            .build();

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("price", gaussFunction.getFieldName());
        assertEquals(org.opensearch.search.MultiValueMode.MAX, gaussFunction.getMultiValueMode());
    }

    public void testFromProtoWithMultiValueModeAvg() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(100.0).setScale(10.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("score", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_AVG)
            .build();

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("score", gaussFunction.getFieldName());
        assertEquals(org.opensearch.search.MultiValueMode.AVG, gaussFunction.getMultiValueMode());
    }

    public void testFromProtoWithMultiValueModeUnspecified() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(20.0).setScale(10.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("rating", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_UNSPECIFIED)
            .build();

        ScoreFunctionBuilder<?> result = GaussDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(GaussDecayFunctionBuilder.class));
        GaussDecayFunctionBuilder gaussFunction = (GaussDecayFunctionBuilder) result;

        assertEquals("rating", gaussFunction.getFieldName());
        // When UNSPECIFIED, multi_value_mode should remain at default (MIN)
        assertEquals(org.opensearch.search.MultiValueMode.MIN, gaussFunction.getMultiValueMode());
    }
}
