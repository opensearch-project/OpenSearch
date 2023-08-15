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

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGrid;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.GeoCentroid;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.opensearch.search.aggregations.AggregationBuilders.geoCentroid;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoCentroidITTestCase extends AbstractGeoAggregatorModulePluginTestCase {
    private static final String aggName = "geoCentroid";

    public void testSingleValueFieldAsSubAggToGeohashGrid() throws Exception {
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addAggregation(
                AggregationBuilders.geohashGrid("geoGrid")
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .subAggregation(geoCentroid(aggName).field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();
        assertSearchResponse(response);

        GeoGrid grid = response.getAggregations().get("geoGrid");
        assertThat(grid, notNullValue());
        assertThat(grid.getName(), equalTo("geoGrid"));
        List<? extends GeoGrid.Bucket> buckets = grid.getBuckets();
        for (GeoGrid.Bucket cell : buckets) {
            String geohash = cell.getKeyAsString();
            GeoPoint expectedCentroid = expectedCentroidsForGeoHash.get(geohash);
            GeoCentroid centroidAgg = cell.getAggregations().get(aggName);
            assertThat(
                "Geohash " + geohash + " has wrong centroid latitude ",
                expectedCentroid.lat(),
                closeTo(centroidAgg.centroid().lat(), GEOHASH_TOLERANCE)
            );
            assertThat(
                "Geohash " + geohash + " has wrong centroid longitude",
                expectedCentroid.lon(),
                closeTo(centroidAgg.centroid().lon(), GEOHASH_TOLERANCE)
            );
        }
    }
}
