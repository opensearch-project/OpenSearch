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
 * Tests that new documents ingested after upgrade are correctly included
 * in star tree aggregations. New segments should use Composite104Codec natively.
 */
public class StarTreeUpgradePostIngestIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_post_ingest";

    public void testIngestAfterUpgrade() throws Exception {
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

        // Initial docs
        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "price", 10.0 + i)
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

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
        assertEquals(0, response.getFailedShards());

        // Ingest more docs AFTER upgrade — these go into new native Composite104Codec segments
        // Note: append_only is enabled after upgrade, so we cannot use custom IDs
        double postIngestSum = 0;
        for (int i = 100; i < 200; i++) {
            double price = 10.0 + i;
            postIngestSum += price;
            client().prepareIndex(INDEX_NAME)
                .setSource("category", i % 5, "price", price)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify total doc count includes new docs
        long totalDocs = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
        assertEquals("expected 200 total docs", 200, totalDocs);

        // Verify star tree active (covers both old upgraded + new native segments)
        SearchResponse after = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("price"))
            .get();
        assertTrue("star tree not active after post-upgrade ingest", Boolean.TRUE.equals(after.isTerminatedEarly()));

        // Verify sum includes all 200 docs
        double expectedTotal = 0;
        for (int i = 0; i < 200; i++) {
            expectedTotal += 10.0 + i;
        }
        double actualTotal = ((Sum) after.getAggregations().get("total")).getValue();
        assertEquals("sum should include all 200 docs", expectedTotal, actualTotal, 0.01);
    }
}
