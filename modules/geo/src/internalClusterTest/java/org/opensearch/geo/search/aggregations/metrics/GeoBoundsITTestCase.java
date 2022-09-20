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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.geo.search.aggregations.metrics;

import org.hamcrest.MatcherAssert;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.util.BigArray;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.opensearch.geo.tests.common.AggregationBuilders.geoBounds;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoBoundsITTestCase extends AbstractGeoAggregatorModulePluginTestCase {
    private static final String aggName = "geoBounds";

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedField_getProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false)))
            .get();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        GeoBounds geobounds = global.getAggregations().get(aggName);
        assertThat(geobounds, notNullValue());
        assertThat(geobounds.getName(), equalTo(aggName));
        assertThat((GeoBounds) ((InternalAggregation) global).getProperty(aggName), sameInstance(geobounds));
        GeoPoint topLeft = geobounds.topLeft();
        GeoPoint bottomRight = geobounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".top"), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".left"), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName + ".bottom"),
            closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE)
        );
        assertThat(
            (double) ((InternalAggregation) global).getProperty(aggName + ".right"),
            closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE)
        );
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
            .addAggregation(geoBounds(aggName).field(MULTI_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(multiTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(multiTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(multiBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(multiBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(singleTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(singleTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(singleBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(singleBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        GeoBounds geoBounds = searchResponse.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft, equalTo(null));
        assertThat(bottomRight, equalTo(null));
    }

    public void testSingleValuedFieldNearDateLine() throws Exception {
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoPoint geoValuesTopLeft = new GeoPoint(38, -179);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, 178);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(geoValuesTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(geoValuesTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(geoValuesBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(geoValuesBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testSingleValuedFieldNearDateLineWrapLongitude() throws Exception {

        GeoPoint geoValuesTopLeft = new GeoPoint(38, 170);
        GeoPoint geoValuesBottomRight = new GeoPoint(-24, -175);
        SearchResponse response = client().prepareSearch(DATELINE_IDX_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(true))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(geoValuesTopLeft.lat(), GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(geoValuesTopLeft.lon(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(geoValuesBottomRight.lat(), GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(geoValuesBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    /**
     * This test forces the {@link GeoBoundsAggregator} to resize the {@link BigArray}s it uses to ensure they are
     * resized correctly
     */
    public void testSingleValuedFieldAsSubAggToHighCardTermsAgg() {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addAggregation(
                terms("terms").field(NUMBER_FIELD_NAME)
                    .subAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat("InternalBucket " + bucket.getKey() + " has wrong number of documents", bucket.getDocCount(), equalTo(1L));
            GeoBounds geoBounds = bucket.getAggregations().get(aggName);
            assertThat(geoBounds, notNullValue());
            assertThat(geoBounds.getName(), equalTo(aggName));
            assertThat(geoBounds.topLeft().getLat(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.topLeft().getLon(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
            assertThat(geoBounds.bottomRight().getLat(), allOf(greaterThanOrEqualTo(-90.0), lessThanOrEqualTo(90.0)));
            assertThat(geoBounds.bottomRight().getLon(), allOf(greaterThanOrEqualTo(-180.0), lessThanOrEqualTo(180.0)));
        }
    }

    public void testSingleValuedFieldWithZeroLon() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_ZERO_NAME)
            .addAggregation(geoBounds(aggName).field(SINGLE_VALUED_FIELD_NAME).wrapLongitude(false))
            .get();

        assertSearchResponse(response);

        GeoBounds geoBounds = response.getAggregations().get(aggName);
        assertThat(geoBounds, notNullValue());
        assertThat(geoBounds.getName(), equalTo(aggName));
        GeoPoint topLeft = geoBounds.topLeft();
        GeoPoint bottomRight = geoBounds.bottomRight();
        assertThat(topLeft.lat(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(topLeft.lon(), closeTo(0.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.lat(), closeTo(1.0, GEOHASH_TOLERANCE));
        assertThat(bottomRight.lon(), closeTo(0.0, GEOHASH_TOLERANCE));
    }

    public void testGeoShapeValuedField() {
        final SearchResponse response = client().prepareSearch(IDX_NAME)
            .addAggregation(geoBounds(aggName).field(GEO_SHAPE_FIELD_NAME).wrapLongitude(false))
            .get();
        assertSearchResponse(response);
        final GeoBounds geoBounds = response.getAggregations().get(aggName);
        MatcherAssert.assertThat(geoBounds, notNullValue());
        MatcherAssert.assertThat(geoBounds.getName(), equalTo(aggName));
        final GeoPoint topLeft = geoBounds.topLeft();
        final GeoPoint bottomRight = geoBounds.bottomRight();
        MatcherAssert.assertThat(topLeft.lat(), closeTo(geoShapeTopLeft.lat(), GEOHASH_TOLERANCE));
        MatcherAssert.assertThat(topLeft.lon(), closeTo(geoShapeTopLeft.lon(), GEOHASH_TOLERANCE));
        MatcherAssert.assertThat(bottomRight.lat(), closeTo(geoShapeBottomRight.lat(), GEOHASH_TOLERANCE));
        MatcherAssert.assertThat(bottomRight.lon(), closeTo(geoShapeBottomRight.lon(), GEOHASH_TOLERANCE));
    }

    public void testEmptyAggregationOnGeoShapes() {
        final SearchResponse searchResponse = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoBounds(aggName).field(GEO_SHAPE_FIELD_NAME).wrapLongitude(false))
            .get();

        MatcherAssert.assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        final GeoBounds geoBounds = searchResponse.getAggregations().get(aggName);
        MatcherAssert.assertThat(geoBounds, notNullValue());
        MatcherAssert.assertThat(geoBounds.getName(), equalTo(aggName));
        final GeoPoint topLeft = geoBounds.topLeft();
        final GeoPoint bottomRight = geoBounds.bottomRight();
        MatcherAssert.assertThat(topLeft, equalTo(null));
        MatcherAssert.assertThat(bottomRight, equalTo(null));
    }
}
