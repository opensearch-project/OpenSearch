/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.ShapeType;
import org.opensearch.geometry.utils.StandardValidator;
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.index.query.GeoExecType;
import org.opensearch.index.query.GeoValidationMethod;
import org.opensearch.protobufs.GeoBoundingBoxQuery;
import org.opensearch.protobufs.GeoBounds;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;

import java.util.Locale;

/**
 * Utility class for converting GeoBoundingBoxQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of geo bounding box queries
 * into their corresponding OpenSearch GeoBoundingBoxQueryBuilder implementations for search operations.
 *
 */
class GeoBoundingBoxQueryBuilderProtoUtils {

    private static final Logger logger = LogManager.getLogger(GeoBoundingBoxQueryBuilderProtoUtils.class);

    private GeoBoundingBoxQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer GeoBoundingBoxQuery to an OpenSearch GeoBoundingBoxQueryBuilder.
     * Similar to {@link GeoBoundingBoxQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * GeoBoundingBoxQueryBuilder with the appropriate field name, bounding box, and other parameters.
     *
     * @param geoBoundingBoxQueryProto The Protocol Buffer GeoBoundingBoxQuery to convert
     * @return A configured GeoBoundingBoxQueryBuilder instance
     */
    static GeoBoundingBoxQueryBuilder fromProto(GeoBoundingBoxQuery geoBoundingBoxQueryProto) {
        if (geoBoundingBoxQueryProto == null) {
            throw new IllegalArgumentException("GeoBoundingBoxQuery cannot be null");
        }

        if (geoBoundingBoxQueryProto.getBoundingBoxMap().isEmpty()) {
            throw new IllegalArgumentException("GeoBoundingBoxQuery must have at least one bounding box");
        }
        String fieldName = geoBoundingBoxQueryProto.getBoundingBoxMap().keySet().iterator().next();
        GeoBounds geoBounds = geoBoundingBoxQueryProto.getBoundingBoxMap().get(fieldName);

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        GeoValidationMethod validationMethod = null;
        boolean ignoreUnmapped = GeoBoundingBoxQueryBuilder.DEFAULT_IGNORE_UNMAPPED;
        GeoBoundingBox bbox = null;
        GeoExecType type = GeoExecType.MEMORY;

        bbox = parseBoundingBox(geoBounds);

        if (geoBoundingBoxQueryProto.hasXName()) {
            queryName = geoBoundingBoxQueryProto.getXName();
        }

        if (geoBoundingBoxQueryProto.hasBoost()) {
            boost = geoBoundingBoxQueryProto.getBoost();
        }

        if (geoBoundingBoxQueryProto.hasValidationMethod()) {
            validationMethod = parseValidationMethod(geoBoundingBoxQueryProto.getValidationMethod());
        }

        if (geoBoundingBoxQueryProto.hasIgnoreUnmapped()) {
            ignoreUnmapped = geoBoundingBoxQueryProto.getIgnoreUnmapped();
        }

        if (geoBoundingBoxQueryProto.hasType()) {
            type = parseExecutionType(geoBoundingBoxQueryProto.getType());
        }
        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        builder.setCorners(bbox.topLeft(), bbox.bottomRight());
        builder.queryName(queryName);
        builder.boost(boost);
        builder.type(type);
        builder.ignoreUnmapped(ignoreUnmapped);
        if (validationMethod != null) {
            builder.setValidationMethod(validationMethod);
        }

        return builder;
    }

    /**
     * Parses a GeoBounds protobuf into a GeoBoundingBox object.
     *
     * @param geoBounds The Protocol Buffer GeoBounds to parse
     * @return A GeoBoundingBox object
     */
    private static GeoBoundingBox parseBoundingBox(GeoBounds geoBounds) {
        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;

        Rectangle envelope = null;

        // Create GeoPoint instances for reuse (matching GeoBoundingBox.parseBoundingBox pattern)
        GeoPoint sparse = new GeoPoint();

        if (geoBounds.hasCoords()) {
            org.opensearch.protobufs.CoordsGeoBounds coords = geoBounds.getCoords();
            top = coords.getTop();
            bottom = coords.getBottom();
            left = coords.getLeft();
            right = coords.getRight();

        } else if (geoBounds.hasTlbr()) {
            org.opensearch.protobufs.TopLeftBottomRightGeoBounds tlbr = geoBounds.getTlbr();
            GeoPointProtoUtils.parseGeoPoint(tlbr.getTopLeft(), sparse, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            top = sparse.getLat();
            left = sparse.getLon();
            GeoPointProtoUtils.parseGeoPoint(tlbr.getBottomRight(), sparse, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            bottom = sparse.getLat();
            right = sparse.getLon();

        } else if (geoBounds.hasTrbl()) {
            org.opensearch.protobufs.TopRightBottomLeftGeoBounds trbl = geoBounds.getTrbl();
            GeoPointProtoUtils.parseGeoPoint(trbl.getTopRight(), sparse, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            top = sparse.getLat();
            right = sparse.getLon();
            GeoPointProtoUtils.parseGeoPoint(trbl.getBottomLeft(), sparse, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
            bottom = sparse.getLat();
            left = sparse.getLon();

        } else if (geoBounds.hasWkt()) {
            // WKT format bounds
            org.opensearch.protobufs.WktGeoBounds wkt = geoBounds.getWkt();
            try {
                // Parse WKT using the same approach as GeoBoundingBox.parseBoundingBox
                WellKnownText wktParser = new WellKnownText(true, new StandardValidator(true));
                Geometry geometry = wktParser.fromWKT(wkt.getWkt());
                if (!ShapeType.ENVELOPE.equals(geometry.type())) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, GeoUtils.WKT_BOUNDING_BOX_TYPE_ERROR, geometry.type(), ShapeType.ENVELOPE)
                    );
                }
                envelope = (Rectangle) geometry;
            } catch (Exception e) {
                throw new IllegalArgumentException(GeoUtils.WKT_BOUNDING_BOX_PARSE_ERROR + ": " + wkt.getWkt(), e);
            }

        } else {
            throw new IllegalArgumentException("GeoBounds must have one of: coords, tlbr, trbl, or wkt");
        }
        if (envelope != null) {
            if (Double.isNaN(top) == false
                || Double.isNaN(bottom) == false
                || Double.isNaN(left) == false
                || Double.isNaN(right) == false) {
                throw new IllegalArgumentException(
                    "failed to parse bounding box. Conflicting definition found " + "using well-known text and explicit corners."
                );
            }
            GeoPoint topLeft = new GeoPoint(envelope.getMaxLat(), envelope.getMinLon());
            GeoPoint bottomRight = new GeoPoint(envelope.getMinLat(), envelope.getMaxLon());
            return new GeoBoundingBox(topLeft, bottomRight);
        }

        GeoPoint topLeft = new GeoPoint(top, left);
        GeoPoint bottomRight = new GeoPoint(bottom, right);
        return new GeoBoundingBox(topLeft, bottomRight);
    }

    /**
     * Converts protobuf GeoExecution to OpenSearch GeoExecType.
     *
     * @param executionTypeProto the protobuf GeoExecution
     * @return the converted OpenSearch GeoExecType
     */
    private static GeoExecType parseExecutionType(org.opensearch.protobufs.GeoExecution executionTypeProto) {
        switch (executionTypeProto) {
            case GEO_EXECUTION_MEMORY:
                return GeoExecType.MEMORY;
            case GEO_EXECUTION_INDEXED:
                return GeoExecType.INDEXED;
            default:
                return GeoExecType.MEMORY; // Default value
        }
    }

    /**
     * Converts protobuf GeoValidationMethod to OpenSearch GeoValidationMethod.
     *
     * @param validationMethodProto the protobuf GeoValidationMethod
     * @return the converted OpenSearch GeoValidationMethod
     */
    private static GeoValidationMethod parseValidationMethod(org.opensearch.protobufs.GeoValidationMethod validationMethodProto) {
        switch (validationMethodProto) {
            case GEO_VALIDATION_METHOD_COERCE:
                return GeoValidationMethod.COERCE;
            case GEO_VALIDATION_METHOD_IGNORE_MALFORMED:
                return GeoValidationMethod.IGNORE_MALFORMED;
            case GEO_VALIDATION_METHOD_STRICT:
                return GeoValidationMethod.STRICT;
            default:
                return GeoValidationMethod.STRICT; // Default value
        }
    }

}
