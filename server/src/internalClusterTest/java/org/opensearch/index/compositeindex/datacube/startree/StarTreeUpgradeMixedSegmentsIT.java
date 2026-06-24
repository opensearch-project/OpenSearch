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
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeAction;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeRequest;
import org.opensearch.action.admin.indices.startree.StarTreeUpgradeResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests upgrade on an index with mixed segments: some with docValuesGen == -1 (clean)
 * and some with docValuesGen != -1 (soft deletes). Verifies both codec paths are exercised.
 */
public class StarTreeUpgradeMixedSegmentsIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_mixed_segments";

    public void testUpgradeMixedSegments() throws Exception {
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
                        .startObject("status")
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

        // Segment 1: clean (no deletes)
        for (int i = 0; i < 50; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "status", i % 3, "price", 10.0 + i, "quantity", 1 + (i % 10))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Segment 2: will have soft deletes
        for (int i = 50; i < 100; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("category", i % 5, "status", i % 3, "price", 10.0 + i, "quantity", 1 + (i % 10))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Delete from segment 2 → creates soft deletes → docValuesGen != -1
        for (int i = 50; i < 65; i++) {
            client().prepareDelete(INDEX_NAME, String.valueOf(i)).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture baseline
        SearchResponse before = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(
                AggregationBuilders.terms("by_category")
                    .field("category")
                    .size(10)
                    .subAggregation(AggregationBuilders.sum("total_price").field("price"))
            )
            .get();
        Terms beforeTerms = before.getAggregations().get("by_category");

        // Upgrade
        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("category"), new NumericDimension("status")),
            List.of(new Metric("price", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );
        StarTreeUpgradeResponse response = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();
        assertEquals(0, response.getFailedShards());

        // Verify both codec paths exercised
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull(shard);

        SegmentInfos infos = SegmentInfos.readLatestCommit(shard.store().directory());
        boolean foundComposite912 = false;
        boolean foundNonComposite = false;
        for (SegmentCommitInfo ci : infos) {
            if (Composite912Codec.COMPOSITE_INDEX_CODEC_NAME.equals(ci.info.getCodec().getName())) {
                foundComposite912 = true;
            } else {
                foundNonComposite = true;
            }
        }
        assertTrue("expected at least one Composite912Codec segment", foundComposite912);
        assertTrue("expected at least one non-Composite912 segment (soft-delete path)", foundNonComposite);
        assertFalse("direct reader cache should not be empty", shard.getStarTreeDirectReaderCache().isEmpty());

        // Verify aggregation correctness
        SearchResponse after = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(
                AggregationBuilders.terms("by_category")
                    .field("category")
                    .size(10)
                    .subAggregation(AggregationBuilders.sum("total_price").field("price"))
            )
            .get();
        assertTrue("star tree not active", Boolean.TRUE.equals(after.isTerminatedEarly()));

        Terms afterTerms = after.getAggregations().get("by_category");
        assertEquals(beforeTerms.getBuckets().size(), afterTerms.getBuckets().size());
        for (Terms.Bucket beforeBucket : beforeTerms.getBuckets()) {
            Terms.Bucket afterBucket = afterTerms.getBucketByKey(beforeBucket.getKeyAsString());
            assertNotNull("missing bucket " + beforeBucket.getKey(), afterBucket);
            assertEquals(beforeBucket.getDocCount(), afterBucket.getDocCount());
            double beforePrice = ((Sum) beforeBucket.getAggregations().get("total_price")).getValue();
            double afterPrice = ((Sum) afterBucket.getAggregations().get("total_price")).getValue();
            assertEquals(beforePrice, afterPrice, 0.01);
        }
    }
}
