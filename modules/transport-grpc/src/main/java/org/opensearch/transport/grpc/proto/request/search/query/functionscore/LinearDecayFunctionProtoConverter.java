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
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.GeoDecayPlacement;
import org.opensearch.protobufs.NumericDecayPlacement;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;

/**
 * Protocol Buffer converter for LinearDecayFunction.
 * This converter handles the transformation of Protocol Buffer LinearDecayFunction objects
 * into OpenSearch LinearDecayFunctionBuilder instances.
 */
public class LinearDecayFunctionProtoConverter {

    /**
     * Default constructor for LinearDecayFunctionProtoConverter.
     */
    public LinearDecayFunctionProtoConverter() {
        // Default constructor
    }

    /**
     * Returns the function score container case that this converter handles.
     *
     * @return the LINEAR function score container case
     */
    public FunctionScoreContainer.FunctionScoreContainerCase getHandledFunctionCase() {
        return FunctionScoreContainer.FunctionScoreContainerCase.LINEAR;
    }

    /**
     * Converts a Protocol Buffer FunctionScoreContainer containing a linear decay function
     * to an OpenSearch ScoreFunctionBuilder.
     *
     * @param container the Protocol Buffer FunctionScoreContainer containing the linear decay function
     * @return the corresponding OpenSearch ScoreFunctionBuilder
     * @throws IllegalArgumentException if the container is null or doesn't contain a LINEAR function
     */
    public ScoreFunctionBuilder<?> fromProto(FunctionScoreContainer container) {
        if (container == null || container.getFunctionScoreContainerCase() != FunctionScoreContainer.FunctionScoreContainerCase.LINEAR) {
            throw new IllegalArgumentException("FunctionScoreContainer must contain a LinearDecayFunction");
        }

        return parseLinearDecayFunction(container.getLinear());
    }

    /**
     * Parses a DecayFunction and creates a LinearDecayFunctionBuilder.
     *
     * @param decayFunction the protobuf DecayFunction
     * @return the corresponding OpenSearch LinearDecayFunctionBuilder
     */
    private static ScoreFunctionBuilder<?> parseLinearDecayFunction(DecayFunction decayFunction) {
        if (decayFunction.getPlacementCount() == 0) {
            throw new IllegalArgumentException("DecayFunction must have at least one placement");
        }

        var entry = decayFunction.getPlacementMap().entrySet().iterator().next();
        String fieldName = entry.getKey();
        DecayPlacement decayPlacement = entry.getValue();

        if (decayPlacement.hasNumericDecayPlacement()) {
            return parseNumericLinearDecay(fieldName, decayPlacement.getNumericDecayPlacement());
        } else if (decayPlacement.hasGeoDecayPlacement()) {
            return parseGeoLinearDecay(fieldName, decayPlacement.getGeoDecayPlacement());
        } else if (decayPlacement.hasDateDecayPlacement()) {
            return parseDateLinearDecay(fieldName, decayPlacement.getDateDecayPlacement());
        } else {
            throw new IllegalArgumentException("Unsupported decay placement type");
        }
    }

    /**
     * Parses a numeric decay placement for linear decay.
     */
    private static ScoreFunctionBuilder<?> parseNumericLinearDecay(String fieldName, NumericDecayPlacement numericPlacement) {
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
     * Parses a geo decay placement for linear decay.
     */
    private static ScoreFunctionBuilder<?> parseGeoLinearDecay(String fieldName, GeoDecayPlacement geoPlacement) {
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
     * Parses a date decay placement for linear decay.
     */
    private static ScoreFunctionBuilder<?> parseDateLinearDecay(String fieldName, DateDecayPlacement datePlacement) {
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
