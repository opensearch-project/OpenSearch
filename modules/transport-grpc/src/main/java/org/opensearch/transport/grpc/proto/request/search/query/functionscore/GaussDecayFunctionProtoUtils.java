/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.index.query.functionscore.GaussDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.DateDecayPlacement;
import org.opensearch.protobufs.DecayFunction;
import org.opensearch.protobufs.DecayPlacement;
import org.opensearch.protobufs.GeoDecayPlacement;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;

import java.util.Map;

/**
 * Utility class for converting Protocol Buffer DecayFunction to OpenSearch GaussDecayFunctionBuilder.
 * This utility handles the transformation of Protocol Buffer DecayFunction objects
 * into OpenSearch GaussDecayFunctionBuilder instances.
 */
class GaussDecayFunctionProtoUtils {

    private GaussDecayFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer DecayFunction to an OpenSearch ScoreFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.DecayFunctionParser#fromXContent(org.opensearch.core.xcontent.XContentParser)},
     * this method parses the Protocol Buffer representation and creates a properly configured
     * GaussDecayFunctionBuilder with decay placement parameters (numeric, geo, or date).
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

        if (decayPlacement.hasNumericDecayPlacement()) {
            return parseNumericGaussDecay(fieldName, decayPlacement.getNumericDecayPlacement());
        } else if (decayPlacement.hasGeoDecayPlacement()) {
            return parseGeoGaussDecay(fieldName, decayPlacement.getGeoDecayPlacement());
        } else if (decayPlacement.hasDateDecayPlacement()) {
            return parseDateGaussDecay(fieldName, decayPlacement.getDateDecayPlacement());
        } else {
            throw new IllegalArgumentException("Unsupported decay placement type");
        }
    }

    /**
     * Parses a numeric decay placement for Gaussian decay.
     */
    private static ScoreFunctionBuilder<?> parseNumericGaussDecay(String fieldName, NumericDecayPlacement numericPlacement) {
        GaussDecayFunctionBuilder builder;
        if (numericPlacement.hasDecay()) {
            builder = new GaussDecayFunctionBuilder(
                fieldName,
                numericPlacement.getOrigin(),
                numericPlacement.getScale(),
                numericPlacement.hasOffset() ? numericPlacement.getOffset() : null,
                numericPlacement.getDecay()
            );
        } else {
            builder = new GaussDecayFunctionBuilder(
                fieldName,
                numericPlacement.getOrigin(),
                numericPlacement.getScale(),
                numericPlacement.hasOffset() ? numericPlacement.getOffset() : null
            );
        }

        return builder;
    }

    /**
     * Parses a geo decay placement for Gaussian decay.
     */
    private static ScoreFunctionBuilder<?> parseGeoGaussDecay(String fieldName, GeoDecayPlacement geoPlacement) {
        GeoPoint geoPoint = GeoPointProtoUtils.parseGeoPoint(geoPlacement.getOrigin());

        GaussDecayFunctionBuilder builder;
        if (geoPlacement.hasDecay()) {
            builder = new GaussDecayFunctionBuilder(
                fieldName,
                geoPoint,
                geoPlacement.getScale(),
                geoPlacement.hasOffset() ? geoPlacement.getOffset() : null,
                geoPlacement.getDecay()
            );
        } else {
            builder = new GaussDecayFunctionBuilder(
                fieldName,
                geoPoint,
                geoPlacement.getScale(),
                geoPlacement.hasOffset() ? geoPlacement.getOffset() : null
            );
        }

        return builder;
    }

    /**
     * Parses a date decay placement for Gaussian decay.
     */
    private static ScoreFunctionBuilder<?> parseDateGaussDecay(String fieldName, DateDecayPlacement datePlacement) {
        Object origin = datePlacement.hasOrigin() ? datePlacement.getOrigin() : null;

        GaussDecayFunctionBuilder builder;
        if (datePlacement.hasDecay()) {
            builder = new GaussDecayFunctionBuilder(
                fieldName,
                origin,
                datePlacement.getScale(),
                datePlacement.hasOffset() ? datePlacement.getOffset() : null,
                datePlacement.getDecay()
            );
        } else {
            builder = new GaussDecayFunctionBuilder(
                fieldName,
                origin,
                datePlacement.getScale(),
                datePlacement.hasOffset() ? datePlacement.getOffset() : null
            );
        }

        return builder;
    }
}
