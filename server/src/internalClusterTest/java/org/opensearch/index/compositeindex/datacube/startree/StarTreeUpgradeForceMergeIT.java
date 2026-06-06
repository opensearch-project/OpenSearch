/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.index.IndexFileNames;
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
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests force merge after star tree upgrade with mixed segments:
 * - Segment 1: clean (no deletes) → codec switched to Composite912Codec
 * - Segment 2: has soft deletes (docValuesGen != -1) → codec NOT switched, uses direct reader
 * After force merge:
 * 1. Single segment with native composite codec
 * 2. Star tree files present
 * 3. Aggregation correct (deleted docs excluded)
 * 4. Force merge API succeeds without errors
 */
public class StarTreeUpgradeForceMergeIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_force_merge_mixed";

    public void testForceMergeWithMixedSegments() throws Exception {
        // Step 1: Create index — disable merges to control segment topology
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
                        .startObject("category")
                        .field("type", "integer")
                        .endObject()
                        .startObject("price")
                        .field("type", "double")
                        .endObject()
                        .startObject("quantity")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );

        // Step 2: Create Segment 1 — clean (no deletes)
        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "price", 10.0 + i, "quantity", 1 + (i % 10))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Step 3: Create Segment 2 — will have soft deletes after we delete from it
        for (int i = 100; i < 200; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "price", 10.0 + i, "quantity", 1 + (i % 10))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Step 4: Delete some docs from Segment 2 → creates soft deletes → docValuesGen != -1
        for (int i = 100; i < 120; i++) {
            client().prepareDelete(INDEX_NAME, String.valueOf(i)).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Step 5: Verify we have mixed segments (some with deletes, some without)
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull(shard);

        SegmentInfos preUpgradeInfos = SegmentInfos.readLatestCommit(shard.store().directory());
        assertTrue("expected at least 2 segments", preUpgradeInfos.size() >= 2);

        // Step 6: Capture baseline aggregation (after deletes, before upgrade)
        SearchResponse before = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_price").field("price"))
            .get();
        double baselineSum = ((Sum) before.getAggregations().get("total_price")).getValue();
        long baselineDocCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
        // 200 docs - 20 deleted = 180 live docs
        assertEquals("expected 180 live docs", 180, baselineDocCount);

        // Step 7: Upgrade to star tree
        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("category"), new NumericDimension("quantity")),
            List.of(new Metric("price", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.MIN, MetricStat.MAX))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );
        StarTreeUpgradeResponse upgradeResponse = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();
        assertEquals("upgrade failed", 0, upgradeResponse.getFailedShards());

        // Step 8: Verify star tree active after upgrade (before merge)
        SearchResponse afterUpgrade = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_price").field("price"))
            .get();
        assertTrue("star tree not active after upgrade", Boolean.TRUE.equals(afterUpgrade.isTerminatedEarly()));
        assertEquals(
            "sum mismatch after upgrade (deleted docs included?)",
            baselineSum,
            ((Sum) afterUpgrade.getAggregations().get("total_price")).getValue(),
            0.01
        );

        // Step 9: Force merge to 1 segment
        ForceMergeResponse mergeResponse = client().admin()
            .indices()
            .prepareForceMerge(INDEX_NAME)
            .setMaxNumSegments(1)
            .setFlush(true)
            .get();
        assertEquals("force merge had failures", 0, mergeResponse.getFailedShards());

        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Step 10: Verify single segment with composite codec
        indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        shard = indexService.getShardOrNull(0);
        assertNotNull(shard);

        SegmentInfos postMergeInfos = SegmentInfos.readLatestCommit(shard.store().directory());
        assertEquals("expected 1 segment after force merge", 1, postMergeInfos.size());

        SegmentCommitInfo mergedSegment = postMergeInfos.info(0);
        String codecName = mergedSegment.info.getCodec().getName();
        assertTrue("merged segment should use a composite codec, got: " + codecName, codecName.startsWith("Composite"));

        // Step 11: Verify star tree files exist in merged segment
        String segName = mergedSegment.info.name;
        Set<String> files = new HashSet<>(mergedSegment.files());
        String cidFile = IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.DATA_EXTENSION);
        String cimFile = IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.META_EXTENSION);
        String cidvdFile = IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION);
        String cidvmFile = IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION);

        assertTrue("missing .cid file in merged segment", files.contains(cidFile));
        assertTrue("missing .cim file in merged segment", files.contains(cimFile));
        assertTrue("missing .cidvd file in merged segment", files.contains(cidvdFile));
        assertTrue("missing .cidvm file in merged segment", files.contains(cidvmFile));

        // Step 12: Verify aggregation correct after merge — deleted docs still excluded
        SearchResponse afterMerge = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total_price").field("price"))
            .get();
        assertTrue("star tree not active after merge", Boolean.TRUE.equals(afterMerge.isTerminatedEarly()));
        double mergedSum = ((Sum) afterMerge.getAggregations().get("total_price")).getValue();
        assertEquals("sum mismatch after force merge (deleted docs included?)", baselineSum, mergedSum, 0.01);

        // Step 13: Verify doc count preserved (deleted docs purged after merge)
        long postMergeDocCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
        assertEquals("doc count changed after merge", baselineDocCount, postMergeDocCount);
    }
}
