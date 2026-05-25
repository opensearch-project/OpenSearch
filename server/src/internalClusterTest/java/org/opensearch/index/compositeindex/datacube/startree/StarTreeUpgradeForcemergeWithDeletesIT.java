/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeAction;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeRequest;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests force merge after upgrade with soft-delete segments.
 * The merged segment must correctly filter soft-deleted docs from the star tree
 * via the __soft_deletes capture + LiveDocsFilteredDocValuesProducer skip-only path.
 */
public class StarTreeUpgradeForcemergeWithDeletesIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_forcemerge_deletes";
    private static final int NUM_DOCS = 200;
    private static final int DELETE_COUNT = 30;

    public void testForcemergeAfterUpgradeWithSoftDeletes() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.merge.policy.max_merged_segment", "100gb")
            .build();

        assertAcked(
            client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("category").field("type", "integer").endObject()
                        .startObject("price").field("type", "double").endObject()
                        .startObject("quantity").field("type", "integer").endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Ingest docs
        for (int i = 0; i < NUM_DOCS; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "price", 10.0 + i, "quantity", 1 + (i % 10))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Delete some docs → creates soft deletes
        for (int i = 0; i < DELETE_COUNT; i++) {
            client().prepareDelete(INDEX_NAME, String.valueOf(i)).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify doc count after deletes
        long docCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
        assertEquals(NUM_DOCS - DELETE_COUNT, docCount);

        // Capture baseline aggregation
        SearchResponse before = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_price").field("price"))
            .get();
        double baselineSum = ((Sum) before.getAggregations().get("total_price")).getValue();

        // Upgrade
        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("category"), new NumericDimension("price")),
            List.of(new Metric("price", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );
        StarTreeUpgradeResponse upgradeResponse = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();
        assertEquals(0, upgradeResponse.getFailedShards());

        // Force merge to 1 segment — exercises the merge fallback path
        ForceMergeResponse mergeResponse = client().admin().indices().prepareForceMerge(INDEX_NAME)
            .setMaxNumSegments(1)
            .get();
        assertEquals(0, mergeResponse.getFailedShards());
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Verify star tree active and aggregation correct after merge
        // Use assertBusy because force merge commit may not be visible immediately
        final double expectedSum = baselineSum;
        assertBusy(() -> {
            SearchResponse afterMerge = client().prepareSearch(INDEX_NAME)
                .setSize(0)
                .addAggregation(AggregationBuilders.sum("total_price").field("price"))
                .get();
            assertTrue("star tree not active after force merge", Boolean.TRUE.equals(afterMerge.isTerminatedEarly()));
            double afterSum = ((Sum) afterMerge.getAggregations().get("total_price")).getValue();
            assertEquals("aggregation mismatch after force merge with soft-delete segments", expectedSum, afterSum, 0.01);
        });
    }
}
