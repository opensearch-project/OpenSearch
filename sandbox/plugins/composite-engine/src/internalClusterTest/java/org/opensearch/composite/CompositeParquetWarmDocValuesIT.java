/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Verifies a numeric aggregation over Parquet-resident doc values on the warm tier, where the backing
 * Parquet file exists only in the remote object store: read-only-engine stamping, the stamped store
 * pointer, and native reads of footer, page index and pages, in one path.
 *
 * <p>The same aggregation is asserted hot first, so a failure there indicates broken setup rather than
 * a broken warm path. The local Parquet file is deleted before the warm assertion so the object store
 * is the only possible source of the bytes.
 */
public class CompositeParquetWarmDocValuesIT extends DataFormatAwareReadonlyEngineBaseIT {

    private static final int DOCS = 50;                              // values 0..49
    private static final double EXPECTED_SUM = DOCS * (DOCS - 1) / 2; // 1225

    public void testNumericAggregationOverParquetDocValuesOnWarmTier() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);

        // An explicit mapping, unlike the base helper's dynamic one: the aggregation must read through
        // ParquetNumericDocValues, which only happens for a field the Lucene secondary does not hold.
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put(dfaIndexSettings(0)).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .setMapping("field_text", "type=keyword", "field_number", "type=long")
            .get();
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < DOCS; i++) {
            RestStatus status = client().prepareIndex(INDEX_NAME)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
                .setSource("field_text", "value_" + i, "field_number", (long) i)
                .get()
                .status();
            assertEquals(RestStatus.CREATED, status);
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertNumericAggregation("hot");

        // Tier to warm: close, mark warm, reopen. The reopened primary runs the read-only engine and
        // its Parquet files are served from the remote object store.
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true))
            .get();
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        assertWarmShardHasNativeParquetStore();
        // Warm flip leaves the file the hot shard wrote behind on disk, so an aggregation at this point
        // would pass whether or not the remote path works. Deleting it makes the object store the only
        // possible source of the bytes below - the same trick the native tiered-storage tests use.
        deleteLocalParquetFiles();

        assertNumericAggregation("warm");
    }

    /** Asserts the warm shard is wired to a native Parquet object store at all. */
    private void assertWarmShardHasNativeParquetStore() {
        NativeStoreHandle parquetStore = getIndexShard(primaryNodeName()).store()
            .getDataformatAwareStoreHandles()
            .entrySet()
            .stream()
            .filter(e -> "parquet".equals(e.getKey().name()))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
        assertNotNull("warm shard must expose a native store handle for the parquet format", parquetStore);
        assertTrue("warm shard's parquet store handle must be live", parquetStore.isLive());
    }

    /** Removes every Parquet file under the warm primary's data path, so only the object store has them. */
    private void deleteLocalParquetFiles() throws IOException {
        Path dataPath = getIndexShard(primaryNodeName()).shardPath().getDataPath();
        List<Path> local;
        try (Stream<Path> tree = Files.walk(dataPath)) {
            local = tree.filter(p -> p.getFileName().toString().endsWith(".parquet")).collect(Collectors.toList());
        }
        assertFalse("expected the hot shard to have left a local Parquet file to delete", local.isEmpty());
        for (Path file : local) {
            Files.delete(file);
        }
    }

    /** Asserts the sum/count over the Parquet-resident numeric, labelling the failure with the tier. */
    private void assertNumericAggregation(String tier) {
        SearchResponse response = client().prepareSearch(INDEX_NAME)
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("field_number"))
            .addAggregation(AggregationBuilders.count("cnt").field("field_number"))
            .get();

        assertEquals(
            tier + ": no shard failures expected, got " + Arrays.toString(response.getShardFailures()),
            0,
            response.getFailedShards()
        );

        Sum total = response.getAggregations().get("total");
        ValueCount count = response.getAggregations().get("cnt");
        assertEquals(tier + ": doc count from Parquet doc values", DOCS, count.getValue());
        assertEquals(tier + ": sum from Parquet doc values", EXPECTED_SUM, total.getValue(), 0.0);
    }
}
