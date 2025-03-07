/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.Query;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.DistanceFeatureQueryBuilder.Origin;
import org.opensearch.test.AbstractQueryTestCase;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.containsString;

public class DistanceFeatureQueryBuilderTests extends AbstractQueryTestCase<DistanceFeatureQueryBuilder> {
    @Override
    protected DistanceFeatureQueryBuilder doCreateTestQueryBuilder() {
        String field = randomFrom(DATE_FIELD_NAME, DATE_NANOS_FIELD_NAME, GEO_POINT_FIELD_NAME);
        Origin origin;
        String pivot;
        switch (field) {
            case GEO_POINT_FIELD_NAME:
                GeoPoint point = new GeoPoint(randomDouble(), randomDouble());
                origin = randomBoolean() ? new Origin(point) : new Origin(point.geohash());
                pivot = randomFrom(DistanceUnit.values()).toString(randomDouble());
                break;
            case DATE_FIELD_NAME:
                long randomDateMills = randomLongBetween(0, 2_000_000_000_000L);
                origin = randomBoolean() ? new Origin(randomDateMills) : new Origin(new DateTime(randomDateMills).toString());
                pivot = randomTimeValue(1, 1000, "d", "h", "ms", "s", "m");
                break;
            default: // DATE_NANOS_FIELD_NAME
                randomDateMills = randomLongBetween(0, 2_000_000_000_000L);
                if (randomBoolean()) {
                    origin = new Origin(randomDateMills); // nano_dates long accept milliseconds since epoch
                } else {
                    long randomNanos = randomLongBetween(0, 1_000_000L);
                    Instant randomDateNanos = Instant.ofEpochMilli(randomDateMills).plusNanos(randomNanos);
                    origin = new Origin(randomDateNanos.toString());
                }
                pivot = randomTimeValue(1, 100_000_000, "nanos");
                break;
        }
        return new DistanceFeatureQueryBuilder(field, origin, pivot);
    }

    @Override
    protected void doAssertLuceneQuery(DistanceFeatureQueryBuilder queryBuilder, Query query, QueryShardContext context)
        throws IOException {
        String fieldName = expectedFieldName(queryBuilder.fieldName());
        Object origin = queryBuilder.origin().origin();
        String pivot = queryBuilder.pivot();
        final Query expectedQuery;
        if (fieldName.equals(GEO_POINT_FIELD_NAME)) {
            GeoPoint originGeoPoint = (origin instanceof GeoPoint) ? (GeoPoint) origin : GeoUtils.parseFromString((String) origin);
            double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
            expectedQuery = LatLonPoint.newDistanceFeatureQuery(fieldName, 1.0f, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        } else { // if (fieldName.equals(DATE_FIELD_NAME))
            MapperService mapperService = context.getMapperService();
            DateFieldType fieldType = (DateFieldType) mapperService.fieldType(fieldName);
            long originLong = fieldType.parseToLong(origin, true, null, null, context::nowInMillis);
            TimeValue pivotVal = TimeValue.parseTimeValue(pivot, DistanceFeatureQueryBuilder.class.getSimpleName() + ".pivot");
            long pivotLong;
            if (fieldType.resolution() == DateFieldMapper.Resolution.MILLISECONDS) {
                pivotLong = pivotVal.getMillis();
            } else { // NANOSECONDS
                pivotLong = pivotVal.getNanos();
            }
            expectedQuery = LongField.newDistanceFeatureQuery(fieldName, 1.0f, originLong, pivotLong);
        }
        assertEquals(expectedQuery, query);
    }

    public void testFromJsonDateFieldType() throws IOException {
        // origin as string
        String origin = "2018-01-01T13:10:30Z";
        String pivot = "7d";
        String json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + DATE_FIELD_NAME
            + "\",\n"
            + "            \"origin\": \""
            + origin
            + "\",\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 1.0\n"
            + "    }\n"
            + "}";
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin, parsed.origin().origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 1.0, parsed.boost(), 0.0001);

        // origin as long
        long originLong = 1514812230999L;
        json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + DATE_FIELD_NAME
            + "\",\n"
            + "            \"origin\": "
            + originLong
            + ",\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 1.0\n"
            + "    }\n"
            + "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, originLong, parsed.origin().origin());
    }

    public void testFromJsonDateNanosFieldType() throws IOException {
        // origin as string
        String origin = "2018-01-01T13:10:30.323456789Z";
        String pivot = "100000000nanos";
        String json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + DATE_NANOS_FIELD_NAME
            + "\",\n"
            + "            \"origin\": \""
            + origin
            + "\",\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 1.0\n"
            + "    }\n"
            + "}";
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin, parsed.origin().origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 1.0, parsed.boost(), 0.0001);

        // origin as long
        long originLong = 1514812230999L;
        json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + DATE_NANOS_FIELD_NAME
            + "\",\n"
            + "            \"origin\": "
            + originLong
            + ",\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 1.0\n"
            + "    }\n"
            + "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, originLong, parsed.origin().origin());
    }

    public void testFromJsonGeoFieldType() throws IOException {
        final GeoPoint origin = new GeoPoint(41.12, -71.34);
        final String pivot = "1km";

        // origin as string
        String json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + GEO_POINT_FIELD_NAME
            + "\",\n"
            + "            \"origin\": \""
            + origin.toString()
            + "\",\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 2.0\n"
            + "    }\n"
            + "}";
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin.toString(), parsed.origin().origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);

        // origin as array
        json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + GEO_POINT_FIELD_NAME
            + "\",\n"
            + "            \"origin\": ["
            + origin.lon()
            + ", "
            + origin.lat()
            + "],\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 2.0\n"
            + "    }\n"
            + "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, origin, parsed.origin().origin());

        // origin as object
        json = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + GEO_POINT_FIELD_NAME
            + "\",\n"
            + "            \"origin\": {"
            + "\"lat\":"
            + origin.lat()
            + ", \"lon\":"
            + origin.lon()
            + "},\n"
            + "            \"pivot\" : \""
            + pivot
            + "\",\n"
            + "            \"boost\" : 2.0\n"
            + "    }\n"
            + "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, origin, parsed.origin().origin());
    }

    public void testQueryMatchNoDocsQueryWithUnmappedField() throws IOException {
        Query expectedQuery = Queries.newMatchNoDocsQuery("Can't run [" + DistanceFeatureQueryBuilder.NAME + "] query on unmapped fields!");
        String queryString = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \"random_unmapped_field\",\n"
            + "            \"origin\": \"random_string\",\n"
            + "            \"pivot\" : \"random_string\"\n"
            + "    }\n"
            + "}";
        Query query = parseQuery(queryString).toQuery(createShardContext());
        assertEquals(expectedQuery, query);
    }

    public void testQueryFailsWithWrongFieldType() {
        String query = "{\n"
            + "    \"distance_feature\" : {\n"
            + "            \"field\": \""
            + INT_FIELD_NAME
            + "\",\n"
            + "            \"origin\": 40,\n"
            + "            \"pivot\" : \"random_string\"\n"
            + "    }\n"
            + "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(query).toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("query can only be run on a date, date_nanos or geo_point field type!"));
    }
}
