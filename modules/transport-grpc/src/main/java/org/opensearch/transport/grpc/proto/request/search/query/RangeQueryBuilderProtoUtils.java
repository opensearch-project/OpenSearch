/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.protobufs.DateRangeQuery;
import org.opensearch.protobufs.DateRangeQueryAllOfFrom;
import org.opensearch.protobufs.DateRangeQueryAllOfTo;
import org.opensearch.protobufs.NumberRangeQuery;
import org.opensearch.protobufs.NumberRangeQueryAllOfFrom;
import org.opensearch.protobufs.NumberRangeQueryAllOfTo;
import org.opensearch.protobufs.RangeQuery;
import org.opensearch.protobufs.RangeRelation;

/**
 * Utility class for converting RangeQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of range queries
 * into their corresponding OpenSearch RangeQueryBuilder implementations for search operations.
 */
class RangeQueryBuilderProtoUtils {

    private RangeQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer RangeQuery to an OpenSearch RangeQueryBuilder.
     * Similar to {@link RangeQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * RangeQueryBuilder with the appropriate range parameters, format, time zone,
     * relation, boost, and query name.
     *
     * @param rangeQueryProto The Protocol Buffer RangeQuery object
     * @return A configured RangeQueryBuilder instance
     * @throws IllegalArgumentException if no valid range query is found
     */
    static RangeQueryBuilder fromProto(RangeQuery rangeQueryProto) {
        if (rangeQueryProto == null) {
            throw new IllegalArgumentException("RangeQuery cannot be null");
        }

        if (rangeQueryProto.hasDateRangeQuery()) {
            return fromDateRangeQuery(rangeQueryProto.getDateRangeQuery());
        } else if (rangeQueryProto.hasNumberRangeQuery()) {
            return fromNumberRangeQuery(rangeQueryProto.getNumberRangeQuery());
        } else {
            throw new IllegalArgumentException("RangeQuery must contain either DateRangeQuery or NumberRangeQuery");
        }
    }

    /**
     * Converts a DateRangeQuery to a RangeQueryBuilder.
     */
    private static RangeQueryBuilder fromDateRangeQuery(DateRangeQuery dateRangeQuery) {
        // Extract field name from the protobuf
        String fieldName = dateRangeQuery.getField();
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty for range query");
        }

        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(fieldName);

        String queryName = dateRangeQuery.hasXName() ? dateRangeQuery.getXName() : null;
        float boost = dateRangeQuery.hasBoost() ? dateRangeQuery.getBoost() : AbstractQueryBuilder.DEFAULT_BOOST;
        String format = dateRangeQuery.hasFormat() ? dateRangeQuery.getFormat() : null;
        String timeZone = dateRangeQuery.getTimeZone().isEmpty() ? null : dateRangeQuery.getTimeZone();
        String relation = null;

        boolean includeLower = RangeQueryBuilder.DEFAULT_INCLUDE_LOWER;
        boolean includeUpper = RangeQueryBuilder.DEFAULT_INCLUDE_UPPER;
        Object from = null;
        Object to = null;

        if (dateRangeQuery.hasFrom()) {
            DateRangeQueryAllOfFrom fromObj = dateRangeQuery.getFrom();
            if (fromObj.hasString()) {
                from = fromObj.getString();
            } else if (fromObj.hasNullValue()) {
                from = null;
            }
        }

        if (dateRangeQuery.hasTo()) {
            DateRangeQueryAllOfTo toObj = dateRangeQuery.getTo();
            if (toObj.hasString()) {
                to = toObj.getString();
            } else if (toObj.hasNullValue()) {
                to = null;
            }
        }

        if (dateRangeQuery.hasIncludeLower()) {
            includeLower = dateRangeQuery.getIncludeLower();
        }

        if (dateRangeQuery.hasIncludeUpper()) {
            includeUpper = dateRangeQuery.getIncludeUpper();
        }

        if (dateRangeQuery.hasGt()) {
            from = dateRangeQuery.getGt();
            includeLower = false;
        }

        if (dateRangeQuery.hasGte()) {
            from = dateRangeQuery.getGte();
            includeLower = true;
        }

        if (dateRangeQuery.hasLt()) {
            to = dateRangeQuery.getLt();
            includeUpper = false;
        }

        if (dateRangeQuery.hasLte()) {
            to = dateRangeQuery.getLte();
            includeUpper = true;
        }

        if (from != null) {
            rangeQuery.from(from);
        }
        if (to != null) {
            rangeQuery.to(to);
        }

        rangeQuery.includeLower(includeLower);
        rangeQuery.includeUpper(includeUpper);

        if (dateRangeQuery.hasRelation()) {
            relation = parseRangeRelation(dateRangeQuery.getRelation());
        }

        if (format != null) {
            rangeQuery.format(format);
        }

        if (timeZone != null) {
            rangeQuery.timeZone(timeZone);
        }

        if (relation != null) {
            rangeQuery.relation(relation);
        }

        rangeQuery.boost(boost);

        if (queryName != null) {
            rangeQuery.queryName(queryName);
        }

        return rangeQuery;
    }

    /**
     * Converts a NumberRangeQuery to a RangeQueryBuilder.
     */
    private static RangeQueryBuilder fromNumberRangeQuery(NumberRangeQuery numberRangeQuery) {
        // Extract field name from the protobuf
        String fieldName = numberRangeQuery.getField();
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty for range query");
        }

        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(fieldName);

        String queryName = numberRangeQuery.hasXName() ? numberRangeQuery.getXName() : null;
        float boost = numberRangeQuery.hasBoost() ? numberRangeQuery.getBoost() : AbstractQueryBuilder.DEFAULT_BOOST;
        String relation = null;

        boolean includeLower = RangeQueryBuilder.DEFAULT_INCLUDE_LOWER;
        boolean includeUpper = RangeQueryBuilder.DEFAULT_INCLUDE_UPPER;
        Object from = null;
        Object to = null;

        if (numberRangeQuery.hasFrom()) {
            NumberRangeQueryAllOfFrom fromObj = numberRangeQuery.getFrom();
            if (fromObj.hasDouble()) {
                from = fromObj.getDouble();
            } else if (fromObj.hasString()) {
                from = fromObj.getString();
            } else if (fromObj.hasNullValue()) {
                from = null;
            }
        }

        if (numberRangeQuery.hasTo()) {
            NumberRangeQueryAllOfTo toObj = numberRangeQuery.getTo();
            if (toObj.hasDouble()) {
                to = toObj.getDouble();
            } else if (toObj.hasString()) {
                to = toObj.getString();
            } else if (toObj.hasNullValue()) {
                to = null;
            }
        }

        if (numberRangeQuery.hasIncludeLower()) {
            includeLower = numberRangeQuery.getIncludeLower();
        }

        if (numberRangeQuery.hasIncludeUpper()) {
            includeUpper = numberRangeQuery.getIncludeUpper();
        }

        if (numberRangeQuery.hasGt()) {
            from = numberRangeQuery.getGt();
            includeLower = false;
        }

        if (numberRangeQuery.hasGte()) {
            from = numberRangeQuery.getGte();
            includeLower = true;
        }

        if (numberRangeQuery.hasLt()) {
            to = numberRangeQuery.getLt();
            includeUpper = false;
        }
        if (numberRangeQuery.hasLte()) {
            to = numberRangeQuery.getLte();
            includeUpper = true;
        }

        if (from != null) {
            rangeQuery.from(from);
        }
        if (to != null) {
            rangeQuery.to(to);
        }

        rangeQuery.includeLower(includeLower);
        rangeQuery.includeUpper(includeUpper);

        if (numberRangeQuery.hasRelation()) {
            relation = parseRangeRelation(numberRangeQuery.getRelation());
        }

        if (relation != null) {
            rangeQuery.relation(relation);
        }

        rangeQuery.boost(boost);

        if (queryName != null) {
            rangeQuery.queryName(queryName);
        }

        return rangeQuery;
    }

    /**
     * Parses RangeRelation enum to string.
     *
     * @param rangeRelation The RangeRelation enum value
     * @return The corresponding string representation, or null if unsupported
     */
    private static String parseRangeRelation(RangeRelation rangeRelation) {
        if (rangeRelation == null) {
            return null;
        }

        switch (rangeRelation) {
            case RANGE_RELATION_CONTAINS:
                return "CONTAINS";
            case RANGE_RELATION_INTERSECTS:
                return "INTERSECTS";
            case RANGE_RELATION_WITHIN:
                return "WITHIN";
            default:
                return null;
        }
    }
}
