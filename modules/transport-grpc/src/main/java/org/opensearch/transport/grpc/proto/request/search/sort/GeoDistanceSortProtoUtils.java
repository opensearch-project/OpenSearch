/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.common.geo.GeoDistance;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.index.query.GeoValidationMethod;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.GeoDistanceSort;
import org.opensearch.protobufs.GeoDistanceType;
import org.opensearch.protobufs.GeoLocation;
import org.opensearch.protobufs.GeoLocationArray;
import org.opensearch.search.sort.GeoDistanceSortBuilder;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.grpc.proto.request.common.GeoPointProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting GeoDistanceSort Protocol Buffers to OpenSearch GeoDistanceSortBuilder objects.
 * Similar to {@link GeoDistanceSortBuilder#fromXContent}, this class handles the conversion of
 * Protocol Buffer representations to properly configured GeoDistanceSortBuilder objects with
 * geo distance sorting, distance type, units, validation method, and nested sorting settings.
 *
 * @opensearch.internal
 */
public class GeoDistanceSortProtoUtils {

    private GeoDistanceSortProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer GeoDistanceSort to a GeoDistanceSortBuilder.
     * Similar to {@link GeoDistanceSortBuilder#fromXContent}, this method parses the
     * Protocol Buffer representation and creates a properly configured GeoDistanceSortBuilder
     * with the appropriate field name, geo points, distance type, units, validation, and nested sorting settings.
     *
     * @param geoDistanceSort The Protocol Buffer GeoDistanceSort to convert
     * @param registry The registry for query conversion (needed for nested sorts with filters)
     * @return A configured GeoDistanceSortBuilder
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public static GeoDistanceSortBuilder fromProto(GeoDistanceSort geoDistanceSort, QueryBuilderProtoConverterRegistry registry) {
        if (geoDistanceSort == null) {
            throw new IllegalArgumentException("GeoDistanceSort cannot be null");
        }
        String fieldName = null;
        List<GeoPoint> geoPoints = null;
        DistanceUnit unit = DistanceUnit.DEFAULT;
        GeoDistance geoDistance = GeoDistance.ARC;
        SortOrder order = SortOrder.ASC;
        SortMode sortMode = null;
        QueryBuilder nestedFilter = null;
        String nestedPath = null;
        NestedSortBuilder nestedSort = null;
        GeoValidationMethod validation = null;
        boolean ignoreUnmapped = false;

        if (geoDistanceSort.hasOrder()) {
            order = SortOrder.fromString(ProtobufEnumUtils.convertToString(geoDistanceSort.getOrder()));
        }

        if (geoDistanceSort.hasUnit() && geoDistanceSort.getUnit() != org.opensearch.protobufs.DistanceUnit.DISTANCE_UNIT_UNSPECIFIED) {
            unit = DistanceUnit.fromString(ProtobufEnumUtils.convertToString(geoDistanceSort.getUnit()));
        }

        if (geoDistanceSort.hasDistanceType() && geoDistanceSort.getDistanceType() != GeoDistanceType.GEO_DISTANCE_TYPE_UNSPECIFIED) {
            geoDistance = GeoDistance.fromString(ProtobufEnumUtils.convertToString(geoDistanceSort.getDistanceType()));
        }

        if (geoDistanceSort.hasValidationMethod()
            && geoDistanceSort.getValidationMethod() != org.opensearch.protobufs.GeoValidationMethod.GEO_VALIDATION_METHOD_UNSPECIFIED) {
            validation = GeoValidationMethod.fromString(ProtobufEnumUtils.convertToString(geoDistanceSort.getValidationMethod()));
        }

        if (geoDistanceSort.hasMode() && geoDistanceSort.getMode() != org.opensearch.protobufs.SortMode.SORT_MODE_UNSPECIFIED) {
            sortMode = SortMode.fromString(ProtobufEnumUtils.convertToString(geoDistanceSort.getMode()));
        }

        if (geoDistanceSort.hasIgnoreUnmapped()) {
            ignoreUnmapped = geoDistanceSort.getIgnoreUnmapped();
        }

        if (geoDistanceSort.hasNested()) {
            nestedSort = NestedSortProtoUtils.fromProto(geoDistanceSort.getNested(), registry);
        }

        if (geoDistanceSort.getLocationCount() == 0) {
            throw new IllegalArgumentException("GeoDistanceSort must contain at least one geo location");
        }

        if (geoDistanceSort.getLocationCount() > 1) {
            throw new IllegalArgumentException(
                "GeoDistanceSort supports only one field, but found " + geoDistanceSort.getLocationCount() + " fields"
            );
        }

        fieldName = geoDistanceSort.getLocationMap().keySet().iterator().next();
        GeoLocationArray geoLocationArray = geoDistanceSort.getLocationMap().get(fieldName);
        geoPoints = extractGeoPoints(geoLocationArray, fieldName);

        GeoDistanceSortBuilder result = new GeoDistanceSortBuilder(fieldName, geoPoints.toArray(new GeoPoint[0]));
        result.geoDistance(geoDistance);
        result.unit(unit);
        result.order(order);
        if (sortMode != null) {
            result.sortMode(sortMode);
        }
        if (nestedFilter != null) {
            result.setNestedFilter(nestedFilter);
        }
        result.setNestedPath(nestedPath);
        if (nestedSort != null) {
            result.setNestedSort(nestedSort);
        }
        if (validation != null) {
            result.validation(validation);
        }
        result.ignoreUnmapped(ignoreUnmapped);

        return result;
    }

    /**
     * Extracts geo points from a GeoLocationArray.
     * This method uses GeoPointProtoUtils to convert protobuf geo locations to GeoPoint objects.
     *
     * @param geoLocationArray The GeoLocationArray containing the geo locations
     * @param fieldName The field name (used for error messages)
     * @return A list of GeoPoint objects
     * @throws IllegalArgumentException if no valid geo locations are found or parsing fails
     */
    private static List<GeoPoint> extractGeoPoints(GeoLocationArray geoLocationArray, String fieldName) {
        List<GeoPoint> geoPoints = new ArrayList<>();

        for (GeoLocation geoLocation : geoLocationArray.getGeoLocationArrayList()) {
            try {
                GeoPoint point = GeoPointProtoUtils.parseGeoPoint(geoLocation);
                geoPoints.add(point);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid geo location in field '" + fieldName + "': " + e.getMessage(), e);
            }
        }

        if (geoPoints.isEmpty()) {
            throw new IllegalArgumentException("No valid geo locations found in field '" + fieldName + "'");
        }

        return geoPoints;
    }
}
