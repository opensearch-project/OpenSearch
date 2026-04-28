/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.common.geo.GeoDistance;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.GeoDistanceQueryBuilder;
import org.opensearch.index.query.GeoValidationMethod;
import org.opensearch.protobufs.GeoDistanceQuery;
import org.opensearch.protobufs.GeoDistanceType;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;

/**
 * Utility class for converting GeoDistanceQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of geo distance queries
 * into their corresponding OpenSearch GeoDistanceQueryBuilder implementations for search operations.
 */
class GeoDistanceQueryBuilderProtoUtils {

    private GeoDistanceQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer GeoDistanceQuery to an OpenSearch GeoDistanceQueryBuilder.
     * Similar to {@link GeoDistanceQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * GeoDistanceQueryBuilder with the appropriate field name, location, distance, and other parameters.
     *
     * @param geoDistanceQueryProto The Protocol Buffer GeoDistanceQuery to convert
     * @return A configured GeoDistanceQueryBuilder instance
     */
    static GeoDistanceQueryBuilder fromProto(GeoDistanceQuery geoDistanceQueryProto) {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        GeoPoint point = new GeoPoint(Double.NaN, Double.NaN);
        String fieldName = null;
        Object vDistance = null;
        DistanceUnit unit = GeoDistanceQueryBuilder.DEFAULT_DISTANCE_UNIT;
        GeoDistance geoDistance = GeoDistanceQueryBuilder.DEFAULT_GEO_DISTANCE;
        GeoValidationMethod validationMethod = null;
        boolean ignoreUnmapped = GeoDistanceQueryBuilder.DEFAULT_IGNORE_UNMAPPED;

        if (geoDistanceQueryProto.getLocationMap().isEmpty()) {
            throw new IllegalArgumentException("GeoDistanceQuery must have at least one location");
        }
        fieldName = geoDistanceQueryProto.getLocationMap().keySet().iterator().next();
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name is required for GeoDistance query");
        }
        GeoLocation geoLocation = geoDistanceQueryProto.getLocationMap().get(fieldName);

        GeoPointProtoUtils.parseGeoPoint(geoLocation, point, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);

        String distance = geoDistanceQueryProto.getDistance();
        if (distance == null || distance.isEmpty()) {
            throw new IllegalArgumentException("geo_distance requires 'distance' to be specified");
        }
        vDistance = distance;

        if (geoDistanceQueryProto.hasUnit()) {
            unit = parseDistanceUnit(geoDistanceQueryProto.getUnit());
        }

        if (geoDistanceQueryProto.hasDistanceType()) {
            geoDistance = parseDistanceType(geoDistanceQueryProto.getDistanceType());
        }

        if (geoDistanceQueryProto.hasValidationMethod()) {
            validationMethod = parseValidationMethod(geoDistanceQueryProto.getValidationMethod());
        }

        if (geoDistanceQueryProto.hasIgnoreUnmapped()) {
            ignoreUnmapped = geoDistanceQueryProto.getIgnoreUnmapped();
        }

        if (geoDistanceQueryProto.hasBoost()) {
            boost = geoDistanceQueryProto.getBoost();
        }

        if (geoDistanceQueryProto.hasXName()) {
            queryName = geoDistanceQueryProto.getXName();
        }

        GeoDistanceQueryBuilder qb = new GeoDistanceQueryBuilder(fieldName);
        if (vDistance instanceof Number number) {
            qb.distance(number.doubleValue(), unit);
        } else {
            qb.distance((String) vDistance, unit);
        }
        qb.point(point);
        if (validationMethod != null) {
            qb.setValidationMethod(validationMethod);
        }
        qb.geoDistance(geoDistance);
        qb.boost(boost);
        qb.queryName(queryName);
        qb.ignoreUnmapped(ignoreUnmapped);

        return qb;
    }

    /**
     * Converts protobuf GeoDistanceType to OpenSearch GeoDistance.
     *
     * @param distanceTypeProto the protobuf GeoDistanceType
     * @return the converted OpenSearch GeoDistance
     */
    private static GeoDistance parseDistanceType(GeoDistanceType distanceTypeProto) {
        switch (distanceTypeProto) {
            case GEO_DISTANCE_TYPE_PLANE:
                return GeoDistance.PLANE;
            case GEO_DISTANCE_TYPE_ARC:
                return GeoDistance.ARC;
            default:
                return GeoDistance.ARC; // Default value
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

    /**
     * Converts protobuf DistanceUnit to OpenSearch DistanceUnit.
     *
     * @param distanceUnitProto the protobuf DistanceUnit
     * @return the converted OpenSearch DistanceUnit
     */
    private static DistanceUnit parseDistanceUnit(org.opensearch.protobufs.DistanceUnit distanceUnitProto) {
        switch (distanceUnitProto) {
            case DISTANCE_UNIT_CM:
                return DistanceUnit.CENTIMETERS;
            case DISTANCE_UNIT_FT:
                return DistanceUnit.FEET;
            case DISTANCE_UNIT_IN:
                return DistanceUnit.INCH;
            case DISTANCE_UNIT_KM:
                return DistanceUnit.KILOMETERS;
            case DISTANCE_UNIT_M:
                return DistanceUnit.METERS;
            case DISTANCE_UNIT_MI:
                return DistanceUnit.MILES;
            case DISTANCE_UNIT_MM:
                return DistanceUnit.MILLIMETERS;
            case DISTANCE_UNIT_NMI:
                return DistanceUnit.NAUTICALMILES;
            case DISTANCE_UNIT_YD:
                return DistanceUnit.YARD;
            case DISTANCE_UNIT_UNSPECIFIED:
            default:
                return DistanceUnit.METERS; // Default value
        }
    }

}
