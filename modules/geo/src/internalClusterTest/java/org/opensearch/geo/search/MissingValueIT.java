/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.geo.GeoModulePluginIntegTestCase;
import org.opensearch.geo.search.aggregations.metrics.GeoBounds;
import org.opensearch.geo.tests.common.AggregationBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class MissingValueIT extends GeoModulePluginIntegTestCase {

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").setMapping("date", "type=date", "location", "type=geo_point", "str", "type=keyword").get());
        indexRandom(
            true,
            client().prepareIndex("idx").setId("1").setSource(),
            client().prepareIndex("idx")
                .setId("2")
                .setSource("str", "foo", "long", 3L, "double", 5.5, "date", "2015-05-07", "location", "1,2")
        );
    }

    public void testUnmappedGeoBounds() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(AggregationBuilders.geoBounds("bounds").field("non_existing_field").missing("2,1"))
            .get();
        assertSearchResponse(response);
        GeoBounds bounds = response.getAggregations().get("bounds");
        assertThat(bounds.bottomRight().lat(), closeTo(2.0, 1E-5));
        assertThat(bounds.bottomRight().lon(), closeTo(1.0, 1E-5));
        assertThat(bounds.topLeft().lat(), closeTo(2.0, 1E-5));
        assertThat(bounds.topLeft().lon(), closeTo(1.0, 1E-5));
    }

    public void testGeoBounds() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(AggregationBuilders.geoBounds("bounds").field("location").missing("2,1"))
            .get();
        assertSearchResponse(response);
        GeoBounds bounds = response.getAggregations().get("bounds");
        assertThat(bounds.bottomRight().lat(), closeTo(1.0, 1E-5));
        assertThat(bounds.bottomRight().lon(), closeTo(2.0, 1E-5));
        assertThat(bounds.topLeft().lat(), closeTo(2.0, 1E-5));
        assertThat(bounds.topLeft().lon(), closeTo(1.0, 1E-5));
    }
}
