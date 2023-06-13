/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class SumLongIT extends OpenSearchIntegTestCase {
    void prepareLongTestData() throws InterruptedException {
        prepareCreate("longidx").setMapping("value", "type=long").get();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("longidx").setSource("value", -9223372026854775808L));
        builders.add(client().prepareIndex("longidx").setSource("value", 9223372026854775807L));

        indexRandom(true, builders);
        ensureSearchable();
    }

    void prepareDoubleTestData() throws InterruptedException {
        prepareCreate("doubleidx").setMapping("value", "type=double").get();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("doubleidx").setSource("value", -9223372026854775808D));
        builders.add(client().prepareIndex("doubleidx").setSource("value", 9223372026854775807D));

        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testLongPreciseValues() throws InterruptedException {
        prepareLongTestData();

        SearchResponse searchResponse = client().prepareSearch("longidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").field("value").method("precise"))
            .get();

        assertHitCount(searchResponse, 2);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) (-9223372026854775808L + 9223372026854775807L)));
    }

    public void testLongKahanValues() throws InterruptedException {
        prepareLongTestData();

        SearchResponse searchResponse = client().prepareSearch("longidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").method("kahan").field("value"))
            .get();

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(-9223372026854775808D + 9223372026854775807D));
    }

    public void testLongDefaultValues() throws InterruptedException {
        prepareLongTestData();

        SearchResponse searchResponse = client().prepareSearch("longidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").field("value"))
            .get();

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(-9223372026854775808D + 9223372026854775807D));
    }

    // Precise option does nothing as passed in doubles have lost precision already
    public void testDoublePreciseValues() throws InterruptedException {
        prepareDoubleTestData();

        SearchResponse searchResponse = client().prepareSearch("doubleidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").method("precise").field("value"))
            .get();

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(-9223372026854775808D + 9223372026854775807D));
    }

    public void testDoubleKahanValues() throws InterruptedException {
        prepareDoubleTestData();

        SearchResponse searchResponse = client().prepareSearch("doubleidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").method("kahan").field("value"))
            .get();

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(-9223372026854775808D + 9223372026854775807D));
    }

    public void testDoubleDefaultValues() throws InterruptedException {
        prepareDoubleTestData();

        SearchResponse searchResponse = client().prepareSearch("doubleidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").field("value"))
            .get();

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(-9223372026854775808D + 9223372026854775807D));
    }
}
