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
 * Integration test for star tree upgrade on segments with docValuesGen != -1.
 * Forces the topology deterministically, runs the upgrade, and verifies correctness.
 */
public class StarTreeUpgradeWithDocValuesGenIT extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_NAME = "test_dvgen";

    public void testUpgradeWithDocValuesGenNotMinusOne() throws Exception {
        // Step 1: Create index with numeric fields
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(indexSettings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("day_of_week")
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

        // Step 2: Index 100 documents and flush (creates committed segments)
        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("day_of_week", i % 7, "price", 10.0 + i, "quantity", 1 + (i % 10))
                .get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Step 3: Delete docs from committed segments → forces docValuesGen != -1
        client().prepareDelete(INDEX_NAME, "0").get();
        client().prepareDelete(INDEX_NAME, "1").get();
        client().prepareDelete(INDEX_NAME, "2").get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Step 4: Verify docValuesGen != -1 on at least one segment
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull("shard not found", shard);

        SegmentInfos infos = SegmentInfos.readLatestCommit(shard.store().directory());
        boolean foundDvGen = false;
        for (SegmentCommitInfo ci : infos) {
            logger.info(
                "segment={} docValuesGen={} fieldInfosGen={} softDelCount={}",
                ci.info.name,
                ci.getDocValuesGen(),
                ci.getFieldInfosGen(),
                ci.getSoftDelCount()
            );
            if (ci.getDocValuesGen() != -1) {
                foundDvGen = true;
            }
        }
        assertTrue("Test setup failed: no segment with docValuesGen != -1", foundDvGen);

        // Step 5: Capture baseline aggregation
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        SearchResponse before = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(
                AggregationBuilders.terms("by_day")
                    .field("day_of_week")
                    .size(10)
                    .subAggregation(AggregationBuilders.sum("total_price").field("price"))
            )
            .get();

        Terms beforeTerms = before.getAggregations().get("by_day");
        assertNotNull("baseline aggregation returned null", beforeTerms);
        assertTrue("baseline has no buckets", beforeTerms.getBuckets().size() > 0);
        logger.info("BEFORE: {} buckets", beforeTerms.getBuckets().size());
        for (Terms.Bucket b : beforeTerms.getBuckets()) {
            logger.info(
                "  day={}: count={}, price={}",
                b.getKey(),
                b.getDocCount(),
                ((Sum) b.getAggregations().get("total_price")).getValue()
            );
        }

        // Step 6: Run star tree upgrade
        StarTreeField starTreeField = new StarTreeField(
            "test_star_tree",
            Arrays.asList(new NumericDimension("day_of_week"), new NumericDimension("quantity")),
            List.of(new Metric("price", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT))),
            new StarTreeFieldConfiguration(10000, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP)
        );

        StarTreeUpgradeResponse upgradeResponse = client().execute(
            StarTreeUpgradeAction.INSTANCE,
            new StarTreeUpgradeRequest(new String[] { INDEX_NAME }, starTreeField)
        ).actionGet();

        logger.info(
            "Upgrade: total={}, successful={}, failed={}",
            upgradeResponse.getTotalShards(),
            upgradeResponse.getSuccessfulShards(),
            upgradeResponse.getFailedShards()
        );
        assertEquals("upgrade had failures", 0, upgradeResponse.getFailedShards());
        assertEquals("upgrade not successful", 1, upgradeResponse.getSuccessfulShards());

        // Step 7: Verify direct reader cache is populated
        // Re-fetch shard reference (engine was restarted)
        indexService = indicesService.indexService(resolveIndex(INDEX_NAME));
        shard = indexService.getShardOrNull(0);
        assertNotNull("shard not found after upgrade", shard);

        logger.info("Direct reader cache size: {}", shard.getStarTreeDirectReaderCache().size());
        // At least one segment should be in the direct reader cache (the one with docValuesGen != -1)
        assertFalse("direct reader cache is empty — docValuesGen != -1 path broken", shard.getStarTreeDirectReaderCache().isEmpty());

        // Step 8: Verify post-upgrade aggregation matches baseline
        SearchResponse after = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(
                AggregationBuilders.terms("by_day")
                    .field("day_of_week")
                    .size(10)
                    .subAggregation(AggregationBuilders.sum("total_price").field("price"))
            )
            .get();

        // Verify star tree was used
        assertTrue("star tree not active after upgrade (terminated_early not true)", Boolean.TRUE.equals(after.isTerminatedEarly()));

        Terms afterTerms = after.getAggregations().get("by_day");
        assertNotNull("post-upgrade aggregation returned null", afterTerms);

        // Verify bucket counts match
        assertEquals("bucket count mismatch", beforeTerms.getBuckets().size(), afterTerms.getBuckets().size());

        for (Terms.Bucket beforeBucket : beforeTerms.getBuckets()) {
            Terms.Bucket afterBucket = afterTerms.getBucketByKey(beforeBucket.getKeyAsString());
            assertNotNull("missing bucket " + beforeBucket.getKey() + " after upgrade", afterBucket);
            assertEquals("doc count mismatch for bucket " + beforeBucket.getKey(), beforeBucket.getDocCount(), afterBucket.getDocCount());

            double beforeRevenue = ((Sum) beforeBucket.getAggregations().get("total_price")).getValue();
            double afterRevenue = ((Sum) afterBucket.getAggregations().get("total_price")).getValue();
            assertEquals("revenue mismatch for bucket " + beforeBucket.getKey(), beforeRevenue, afterRevenue, 0.01);
        }

        logger.info("ALL ASSERTIONS PASSED: docValuesGen != -1 upgrade works correctly");
    }
}
