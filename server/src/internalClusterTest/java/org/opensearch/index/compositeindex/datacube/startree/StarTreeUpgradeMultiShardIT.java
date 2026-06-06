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
 * Tests upgrade on a multi-shard index. Verifies all shards are upgraded successfully
 * and aggregation results are correct across all shards.
 */
public class StarTreeUpgradeMultiShardIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_multi_shard";
    private static final int NUM_SHARDS = 3;

    public void testUpgradeMultipleShards() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
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

        // Ingest enough docs to spread across shards
        for (int i = 0; i < 300; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "price", 10.0 + i)
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture baseline
        SearchResponse before = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("price"))
            .get();
        double baselineSum = ((Sum) before.getAggregations().get("total")).getValue();

        // Upgrade
        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("category"), new NumericDimension("price")),
            List.of(new Metric("price", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );
        StarTreeUpgradeResponse response = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();

        assertEquals("expected all shards successful", NUM_SHARDS, response.getSuccessfulShards());
        assertEquals(0, response.getFailedShards());

        // Verify star tree active and aggregation correct
        SearchResponse after = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("price"))
            .get();
        assertTrue("star tree not active", Boolean.TRUE.equals(after.isTerminatedEarly()));
        double afterSum = ((Sum) after.getAggregations().get("total")).getValue();
        assertEquals("aggregation mismatch across shards", baselineSum, afterSum, 0.01);
    }
}
