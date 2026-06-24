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
 * Regression test for stale PerFieldDocValuesFormat attributes after merge.
 *
 * Scenario: After star tree upgrade, segments retain PerFieldDocValuesFormat attributes
 * (e.g., PerFieldDocValuesFormat.format=Lucene90, PerFieldDocValuesFormat.suffix=0) in their
 * FieldInfos. When these segments are force-merged, the merged segment is written by
 * Composite912Codec with empty suffix (direct naming), but the merged FieldInfos inherits
 * the PerField attributes from source segments. Without the perFieldSuffixedFileExists()
 * guard in Composite912DocValuesFormat.fieldsProducer(), opening the merged segment would
 * attempt to read _N_Lucene90_0.dvm which doesn't exist, causing NoSuchFileException.
 *
 * This test verifies:
 * 1. Multiple segments with PerField attributes can be force-merged without NoSuchFileException
 * 2. The merged segment is readable and queryable after merge
 * 3. New documents ingested after merge (creating segments with native Composite912Codec)
 *    can coexist and be merged again with the first merged segment
 */
public class StarTreeUpgradeStalePerFieldIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_stale_perfield";

    public void testMergeWithStalePerFieldAttributes() throws Exception {
        // Step 1: Create index with multiple shards disabled, merges disabled
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.merge.policy.max_merged_segment", "100gb")
            .build();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(indexSettings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("status")
                        .field("type", "integer")
                        .endObject()
                        .startObject("amount")
                        .field("type", "double")
                        .endObject()
                        .startObject("count")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Step 2: Create 3 segments — each will have PerField attributes from Lucene912Codec
        for (int seg = 0; seg < 3; seg++) {
            for (int i = 0; i < 50; i++) {
                int docId = seg * 50 + i;
                client().prepareIndex(INDEX_NAME)
                    .setId(String.valueOf(docId))
                    .setSource("status", docId % 4, "amount", 10.0 + docId, "count", 1 + (docId % 5))
                    .get();
            }
            client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        }

        // Step 3: Verify we have 3 segments
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull(shard);

        SegmentInfos preUpgradeInfos = SegmentInfos.readLatestCommit(shard.store().directory());
        assertTrue("expected at least 3 segments, got " + preUpgradeInfos.size(), preUpgradeInfos.size() >= 3);

        // Step 4: Capture baseline aggregation
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        SearchResponse baseline = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_amount").field("amount"))
            .get();
        double baselineSum = ((Sum) baseline.getAggregations().get("total_amount")).getValue();
        long baselineCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
        assertEquals("expected 150 docs", 150, baselineCount);

        // Step 5: Upgrade to star tree — segments now have PerField attributes + star tree files
        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("status"), new NumericDimension("count")),
            List.of(new Metric("amount", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.MIN, MetricStat.MAX))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );
        StarTreeUpgradeResponse upgradeResponse = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();
        assertEquals("upgrade failed", 0, upgradeResponse.getFailedShards());

        // Step 6: Force merge — this is where bug 10 would manifest.
        // The merged segment inherits PerField attributes from source segments but writes
        // doc values with empty suffix. Without the fix, reading the merged segment would
        // throw NoSuchFileException for _N_Lucene90_0.dvm.
        ForceMergeResponse mergeResponse = client().admin()
            .indices()
            .prepareForceMerge(INDEX_NAME)
            .setMaxNumSegments(1)
            .setFlush(true)
            .get();
        assertEquals("force merge had failures", 0, mergeResponse.getFailedShards());

        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Step 7: Verify merged segment is readable — if bug 10 exists, this search would fail
        // with NoSuchFileException when Composite912DocValuesFormat.fieldsProducer() tries to
        // open the non-existent suffixed file.
        SearchResponse afterMerge = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_amount").field("amount"))
            .get();
        double mergedSum = ((Sum) afterMerge.getAggregations().get("total_amount")).getValue();
        assertEquals("aggregation mismatch after merge with stale PerField attributes", baselineSum, mergedSum, 0.01);

        // Step 8: Verify single segment with composite codec
        indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        shard = indexService.getShardOrNull(0);
        SegmentInfos postMergeInfos = SegmentInfos.readLatestCommit(shard.store().directory());
        assertEquals("expected 1 segment after force merge", 1, postMergeInfos.size());

        SegmentCommitInfo mergedSegment = postMergeInfos.info(0);
        assertTrue(
            "merged segment should use composite codec, got: " + mergedSegment.info.getCodec().getName(),
            mergedSegment.info.getCodec().getName().startsWith("Composite")
        );

        // Step 9: Ingest new docs after merge — creates a new segment with native Composite912Codec
        for (int i = 150; i < 200; i++) {
            client().prepareIndex(INDEX_NAME).setSource("status", i % 4, "amount", 10.0 + i, "count", 1 + (i % 5)).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Step 10: Force merge again — merges the first merged segment (with inherited PerField
        // attributes) with the new native segment. This tests the second-generation merge case.
        ForceMergeResponse secondMerge = client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertEquals("second force merge had failures", 0, secondMerge.getFailedShards());

        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Step 11: Verify second merge is also readable
        SearchResponse afterSecondMerge = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_amount").field("amount"))
            .get();
        long finalCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
        assertEquals("expected 200 docs after second merge", 200, finalCount);

        // Verify star tree is active on the final merged segment
        assertTrue("star tree not active after second merge", Boolean.TRUE.equals(afterSecondMerge.isTerminatedEarly()));
    }
}
