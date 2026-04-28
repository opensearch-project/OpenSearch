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

    public void testFromProtoWithNumericPlacement() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder()
            .setOrigin(10.0)
            .setScale(5.0)
            .setOffset(1.0)
            .setDecay(0.3)
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("price", decayPlacement).build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("price", expFunction.getFieldName());
    }

    public void testFromProtoWithNumericPlacementWithoutDecay() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(100.0).setScale(50.0).setOffset(5.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("distance", decayPlacement).build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("distance", expFunction.getFieldName());
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

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("timestamp", expFunction.getFieldName());
    }

    public void testFromProtoWithDatePlacementWithoutDecay() {
        org.opensearch.protobufs.DateDecayPlacement datePlacement = org.opensearch.protobufs.DateDecayPlacement.newBuilder()
            .setOrigin("now")
            .setScale("7d")
            .build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setDateDecayPlacement(datePlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("date_field", decayPlacement).build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("date_field", expFunction.getFieldName());
    }

    public void testFromProtoWithNullDecayFunction() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ExpDecayFunctionProtoUtils.fromProto(null));

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithEmptyDecayFunction() {
        DecayFunction decayFunction = DecayFunction.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ExpDecayFunctionProtoUtils.fromProto(decayFunction)
        );

        assertThat(exception.getMessage(), containsString("DecayFunction must have at least one placement"));
    }

    public void testFromProtoWithUnsetDecayPlacement() {
        DecayPlacement decayPlacement = DecayPlacement.newBuilder().build();

        DecayFunction decayFunction = DecayFunction.newBuilder().putPlacement("field", decayPlacement).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ExpDecayFunctionProtoUtils.fromProto(decayFunction)
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

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("location", expFunction.getFieldName());
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

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("geo_field", expFunction.getFieldName());
    }

    public void testFromProtoWithMultiValueModeMin() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(10.0).setScale(5.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("price", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_MIN)
            .build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("price", expFunction.getFieldName());
        assertEquals(org.opensearch.search.MultiValueMode.MIN, expFunction.getMultiValueMode());
    }

    public void testFromProtoWithMultiValueModeMax() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(50.0).setScale(25.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("rating", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_MAX)
            .build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("rating", expFunction.getFieldName());
        assertEquals(org.opensearch.search.MultiValueMode.MAX, expFunction.getMultiValueMode());
    }

    public void testFromProtoWithMultiValueModeAvg() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(100.0).setScale(10.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("score", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_AVG)
            .build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("score", expFunction.getFieldName());
        assertEquals(org.opensearch.search.MultiValueMode.AVG, expFunction.getMultiValueMode());
    }

    public void testFromProtoWithMultiValueModeUnspecified() {
        NumericDecayPlacement numericPlacement = NumericDecayPlacement.newBuilder().setOrigin(10.0).setScale(5.0).build();

        DecayPlacement decayPlacement = DecayPlacement.newBuilder().setNumericDecayPlacement(numericPlacement).build();

        DecayFunction decayFunction = DecayFunction.newBuilder()
            .putPlacement("price", decayPlacement)
            .setMultiValueMode(org.opensearch.protobufs.MultiValueMode.MULTI_VALUE_MODE_UNSPECIFIED)
            .build();

        ScoreFunctionBuilder<?> result = ExpDecayFunctionProtoUtils.fromProto(decayFunction);

        assertThat(result, instanceOf(ExponentialDecayFunctionBuilder.class));
        ExponentialDecayFunctionBuilder expFunction = (ExponentialDecayFunctionBuilder) result;

        assertEquals("price", expFunction.getFieldName());
        // When UNSPECIFIED, multi_value_mode should remain at default (MIN)
        assertEquals(org.opensearch.search.MultiValueMode.MIN, expFunction.getMultiValueMode());
    }
}
