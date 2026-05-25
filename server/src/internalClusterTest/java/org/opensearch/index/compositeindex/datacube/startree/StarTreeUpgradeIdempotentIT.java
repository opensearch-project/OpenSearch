/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.action.admin.indices.startree.StarTreeUpgradeAction;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeRequest;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests that calling the star tree upgrade API twice is idempotent —
 * second call succeeds without errors and produces the same results.
 */
public class StarTreeUpgradeIdempotentIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_idempotent";

    public void testUpgradeIdempotent() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        assertAcked(
            client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("category").field("type", "integer").endObject()
                        .startObject("price").field("type", "double").endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "price", 10.0 + i)
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("category"), new NumericDimension("price")),
            List.of(new Metric("price", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );

        // First upgrade
        StarTreeUpgradeResponse response1 = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();
        assertEquals(0, response1.getFailedShards());

        SearchResponse after1 = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("price"))
            .get();
        assertTrue("star tree not active after first upgrade", Boolean.TRUE.equals(after1.isTerminatedEarly()));
        double sum1 = ((Sum) after1.getAggregations().get("total")).getValue();

        // Second upgrade — should be idempotent
        StarTreeUpgradeResponse response2 = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();
        assertEquals(0, response2.getFailedShards());

        SearchResponse after2 = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("price"))
            .get();
        assertTrue("star tree not active after second upgrade", Boolean.TRUE.equals(after2.isTerminatedEarly()));
        double sum2 = ((Sum) after2.getAggregations().get("total")).getValue();

        assertEquals("aggregation changed after second upgrade", sum1, sum2, 0.01);
    }
}
