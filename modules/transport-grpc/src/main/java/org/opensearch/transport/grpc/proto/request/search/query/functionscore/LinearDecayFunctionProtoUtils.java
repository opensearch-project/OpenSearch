/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.index.query.functionscore.LinearDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.DateDecayPlacement;
import org.opensearch.protobufs.DecayFunction;
import org.opensearch.protobufs.DecayPlacement;
import org.opensearch.protobufs.GeoDecayPlacement;
import org.opensearch.protobufs.MultiValueMode;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;

import java.util.Map;

/**
 * Utility class for converting Protocol Buffer DecayFunction to OpenSearch LinearDecayFunctionBuilder.
 * This utility handles the transformation of Protocol Buffer DecayFunction objects
 * into OpenSearch LinearDecayFunctionBuilder instances.
 */
class LinearDecayFunctionProtoUtils {

    private LinearDecayFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer DecayFunction to an OpenSearch ScoreFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.DecayFunctionParser#fromXContent(org.opensearch.core.xcontent.XContentParser)},
     * this method parses the Protocol Buffer representation and creates a properly configured
     * LinearDecayFunctionBuilder with decay placement parameters (numeric, geo, or date).
     *
     * @param decayFunction the Protocol Buffer DecayFunction
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the decayFunction is null or doesn't contain placements
     */
    static ScoreFunctionBuilder<?> fromProto(DecayFunction decayFunction) {
        if (decayFunction == null || decayFunction.getPlacementCount() == 0) {
            throw new IllegalArgumentException("DecayFunction must have at least one placement");
        }

        Map.Entry<String, DecayPlacement> entry = decayFunction.getPlacementMap().entrySet().iterator().next();
        String fieldName = entry.getKey();
        DecayPlacement decayPlacement = entry.getValue();

        LinearDecayFunctionBuilder builder;
        if (decayPlacement.hasNumericDecayPlacement()) {
            builder = parseNumericLinearDecay(fieldName, decayPlacement.getNumericDecayPlacement());
        } else if (decayPlacement.hasGeoDecayPlacement()) {
            builder = parseGeoLinearDecay(fieldName, decayPlacement.getGeoDecayPlacement());
        } else if (decayPlacement.hasDateDecayPlacement()) {
            builder = parseDateLinearDecay(fieldName, decayPlacement.getDateDecayPlacement());
        } else {
            throw new IllegalArgumentException("Unsupported decay placement type");
        }

        // Set multi_value_mode if present
        if (decayFunction.hasMultiValueMode() && decayFunction.getMultiValueMode() != MultiValueMode.MULTI_VALUE_MODE_UNSPECIFIED) {
            builder.setMultiValueMode(DecayFunctionProtoUtils.parseMultiValueMode(decayFunction.getMultiValueMode()));
        }

        return builder;
    }

    /**
     * Parses a numeric decay placement and creates a LinearDecayFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.DecayFunctionParser},
     * this method constructs a decay function for numeric field values.
     *
     * @param fieldName the field name to apply linear decay
     * @param numericPlacement the protobuf numeric decay placement containing origin, scale, offset, and decay
     * @return the corresponding OpenSearch LinearDecayFunctionBuilder
     */
    private static LinearDecayFunctionBuilder parseNumericLinearDecay(String fieldName, NumericDecayPlacement numericPlacement) {
        LinearDecayFunctionBuilder builder;
        if (numericPlacement.hasDecay()) {
            builder = new LinearDecayFunctionBuilder(
                fieldName,
                numericPlacement.getOrigin(),
                numericPlacement.getScale(),
                numericPlacement.hasOffset() ? numericPlacement.getOffset() : null,
                numericPlacement.getDecay()
            );
        } else {
            builder = new LinearDecayFunctionBuilder(
                fieldName,
                numericPlacement.getOrigin(),
                numericPlacement.getScale(),
                numericPlacement.hasOffset() ? numericPlacement.getOffset() : null
            );
        }

        return builder;
    }

    /**
     * Parses a geo decay placement and creates a LinearDecayFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.DecayFunctionParser},
     * this method constructs a decay function for geographic field values.
     *
     * @param fieldName the field name to apply linear decay
     * @param geoPlacement the protobuf geo decay placement containing origin (lat/lon), scale, offset, and decay
     * @return the corresponding OpenSearch LinearDecayFunctionBuilder
     */
    private static LinearDecayFunctionBuilder parseGeoLinearDecay(String fieldName, GeoDecayPlacement geoPlacement) {
        GeoPoint geoPoint = GeoPointProtoUtils.parseGeoPoint(geoPlacement.getOrigin());

        LinearDecayFunctionBuilder builder;
        if (geoPlacement.hasDecay()) {
            builder = new LinearDecayFunctionBuilder(
                fieldName,
                geoPoint,
                geoPlacement.getScale(),
                geoPlacement.hasOffset() ? geoPlacement.getOffset() : null,
                geoPlacement.getDecay()
            );
        } else {
            builder = new LinearDecayFunctionBuilder(
                fieldName,
                geoPoint,
                geoPlacement.getScale(),
                geoPlacement.hasOffset() ? geoPlacement.getOffset() : null
            );
        }

        return builder;
    }

    /**
     * Parses a date decay placement and creates a LinearDecayFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.DecayFunctionParser},
     * this method constructs a decay function for date field values.
     *
     * @param fieldName the field name to apply linear decay
     * @param datePlacement the protobuf date decay placement containing origin (date), scale, offset, and decay
     * @return the corresponding OpenSearch LinearDecayFunctionBuilder
     */
    private static LinearDecayFunctionBuilder parseDateLinearDecay(String fieldName, DateDecayPlacement datePlacement) {
        Object origin = datePlacement.hasOrigin() ? datePlacement.getOrigin() : null;

        LinearDecayFunctionBuilder builder;
        if (datePlacement.hasDecay()) {
            builder = new LinearDecayFunctionBuilder(
                fieldName,
                origin,
                datePlacement.getScale(),
                datePlacement.hasOffset() ? datePlacement.getOffset() : null,
                datePlacement.getDecay()
            );
        } else {
            builder = new LinearDecayFunctionBuilder(
                fieldName,
                origin,
                datePlacement.getScale(),
                datePlacement.hasOffset() ? datePlacement.getOffset() : null
            );
        }

        return builder;
    }
}
