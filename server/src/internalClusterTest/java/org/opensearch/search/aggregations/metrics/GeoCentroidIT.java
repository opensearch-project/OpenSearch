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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.geoCentroid;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Integration Test for GeoCentroid metric aggregator
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoCentroidIT extends AbstractGeoTestCase {
    private static final String aggName = "geoCentroid";

    public GeoCentroidIT(Settings staticSettings) {
        super(staticSettings);
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse response = client().prepareSearch(EMPTY_IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(response.getHits().getTotalHits().value, equalTo(0L));
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(null));
        assertEquals(0, geoCentroid.count());
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(UNMAPPED_IDX_NAME)
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid, equalTo(null));
        assertEquals(0, geoCentroid.count());
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME, UNMAPPED_IDX_NAME)
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid.lat(), closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE));
        assertThat(centroid.lon(), closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE));
        assertEquals(numDocs, geoCentroid.count());
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(response);

        GeoCentroid geoCentroid = response.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid.lat(), closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE));
        assertThat(centroid.lon(), closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE));
        assertEquals(numDocs, geoCentroid.count());
    }

    public void testSingleValueFieldGetProperty() throws Exception {
        SearchResponse response = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME)))
            .get();
        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        GeoCentroid geoCentroid = global.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        assertThat((GeoCentroid) ((InternalAggregation) global).getProperty(aggName), sameInstance(geoCentroid));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid.lat(), closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE));
        assertThat(centroid.lon(), closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE));
        assertThat(
            ((GeoPoint) ((InternalAggregation) global).getProperty(aggName + ".value")).lat(),
            closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE)
        );
        assertThat(
            ((GeoPoint) ((InternalAggregation) global).getProperty(aggName + ".value")).lon(),
            closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE)
        );
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".lat"), closeTo(singleCentroid.lat(), GEOHASH_TOLERANCE));
        assertThat((double) ((InternalAggregation) global).getProperty(aggName + ".lon"), closeTo(singleCentroid.lon(), GEOHASH_TOLERANCE));
        assertEquals(numDocs, (long) ((InternalAggregation) global).getProperty(aggName + ".count"));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch(IDX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(geoCentroid(aggName).field(MULTI_VALUED_FIELD_NAME))
            .get();
        assertSearchResponse(searchResponse);

        GeoCentroid geoCentroid = searchResponse.getAggregations().get(aggName);
        assertThat(geoCentroid, notNullValue());
        assertThat(geoCentroid.getName(), equalTo(aggName));
        GeoPoint centroid = geoCentroid.centroid();
        assertThat(centroid.lat(), closeTo(multiCentroid.lat(), GEOHASH_TOLERANCE));
        assertThat(centroid.lon(), closeTo(multiCentroid.lon(), GEOHASH_TOLERANCE));
        assertEquals(2 * numDocs, geoCentroid.count());
    }
}
