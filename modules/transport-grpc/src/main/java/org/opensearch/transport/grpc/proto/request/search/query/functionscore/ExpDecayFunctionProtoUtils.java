/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.index.query.functionscore.ExponentialDecayFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.DateDecayPlacement;
import org.opensearch.protobufs.DecayFunction;
import org.opensearch.protobufs.DecayPlacement;
import org.opensearch.protobufs.GeoDecayPlacement;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;

import java.util.Map;

/**
 * Utility class for converting Protocol Buffer DecayFunction to OpenSearch ExponentialDecayFunctionBuilder.
 * This utility handles the transformation of Protocol Buffer DecayFunction objects
 * into OpenSearch ExponentialDecayFunctionBuilder instances.
 */
class ExpDecayFunctionProtoUtils {

    private ExpDecayFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer DecayFunction to an OpenSearch ScoreFunctionBuilder.
     * Similar to {@link org.opensearch.index.query.functionscore.DecayFunctionParser#fromXContent(org.opensearch.core.xcontent.XContentParser)},
     * this method parses the Protocol Buffer representation and creates a properly configured
     * ExponentialDecayFunctionBuilder with decay placement parameters (numeric, geo, or date).
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
            return parseNumericExpDecay(fieldName, decayPlacement.getNumericDecayPlacement());
        } else if (decayPlacement.hasGeoDecayPlacement()) {
            return parseGeoExpDecay(fieldName, decayPlacement.getGeoDecayPlacement());
        } else if (decayPlacement.hasDateDecayPlacement()) {
            return parseDateExpDecay(fieldName, decayPlacement.getDateDecayPlacement());
        } else {
            throw new IllegalArgumentException("Unsupported decay placement type");
        }
    }

    /**
     * Parses a numeric decay placement for exponential decay.
     */
    private static ScoreFunctionBuilder<?> parseNumericExpDecay(String fieldName, NumericDecayPlacement numericPlacement) {
        ExponentialDecayFunctionBuilder builder;
        if (numericPlacement.hasDecay()) {
            builder = new ExponentialDecayFunctionBuilder(
                fieldName,
                numericPlacement.getOrigin(),
                numericPlacement.getScale(),
                numericPlacement.hasOffset() ? numericPlacement.getOffset() : null,
                numericPlacement.getDecay()
            );
        } else {
            builder = new ExponentialDecayFunctionBuilder(
                fieldName,
                numericPlacement.getOrigin(),
                numericPlacement.getScale(),
                numericPlacement.hasOffset() ? numericPlacement.getOffset() : null
            );
        }

        return builder;
    }

    /**
     * Parses a geo decay placement for exponential decay.
     */
    private static ScoreFunctionBuilder<?> parseGeoExpDecay(String fieldName, GeoDecayPlacement geoPlacement) {
        GeoPoint geoPoint = GeoPointProtoUtils.parseGeoPoint(geoPlacement.getOrigin());

        ExponentialDecayFunctionBuilder builder;
        if (geoPlacement.hasDecay()) {
            builder = new ExponentialDecayFunctionBuilder(
                fieldName,
                geoPoint,
                geoPlacement.getScale(),
                geoPlacement.hasOffset() ? geoPlacement.getOffset() : null,
                geoPlacement.getDecay()
            );
        } else {
            builder = new ExponentialDecayFunctionBuilder(
                fieldName,
                geoPoint,
                geoPlacement.getScale(),
                geoPlacement.hasOffset() ? geoPlacement.getOffset() : null
            );
        }

        return builder;
    }

    /**
     * Parses a date decay placement for exponential decay.
     */
    private static ScoreFunctionBuilder<?> parseDateExpDecay(String fieldName, DateDecayPlacement datePlacement) {
        Object origin = datePlacement.hasOrigin() ? datePlacement.getOrigin() : null;

        ExponentialDecayFunctionBuilder builder;
        if (datePlacement.hasDecay()) {
            builder = new ExponentialDecayFunctionBuilder(
                fieldName,
                origin,
                datePlacement.getScale(),
                datePlacement.hasOffset() ? datePlacement.getOffset() : null,
                datePlacement.getDecay()
            );
        } else {
            builder = new ExponentialDecayFunctionBuilder(
                fieldName,
                origin,
                datePlacement.getScale(),
                datePlacement.hasOffset() ? datePlacement.getOffset() : null
            );
        }

        return builder;
    }
}
