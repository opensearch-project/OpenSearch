/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * Integration test that verifies temporary Lucene writer directories (lucene_gen_*)
 * are cleaned up after refresh incorporates their segments into the shared writer.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeLuceneTempCleanupIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-lucene-temp-cleanup";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    public void testLuceneTempDirectoriesCleanedAfterRefresh() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();

        createIndex(INDEX_NAME, indexSettings);
        client().admin()
            .indices()
            .preparePutMapping(INDEX_NAME)
            .setSource("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        // Ingest 3 batches with a refresh after each to create multiple writer generations
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 100; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource("field_keyword", "value_" + batch + "_" + i, "field_number", i)
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }

        // Get the shard's lucene base directory
        IndexShard shard = getPrimaryShard();
        Path luceneBaseDir = shard.shardPath().getDataPath().resolve("lucene");

        // After refresh, the lucene_gen_* temp directories should NOT exist
        if (Files.exists(luceneBaseDir)) {
            try (Stream<Path> dirs = Files.list(luceneBaseDir)) {
                long staleGenDirs = dirs.filter(p -> p.getFileName().toString().startsWith("lucene_gen_")).count();
                assertEquals("Lucene temp directories (lucene_gen_*) should be cleaned up after refresh", 0L, staleGenDirs);
            }
        }

        // Verify data is intact in the index/ directory
        Path indexDir = shard.shardPath().resolveIndex();
        assertTrue("Index directory should exist", Files.exists(indexDir));
        try (Stream<Path> files = Files.list(indexDir)) {
            long fileCount = files.count();
            assertTrue("Index directory should contain segment files", fileCount > 0);
        }
    }

    public void testLuceneTempDirectoriesCleanedAfterForceMerge() throws Exception {
        String forceMergeIndex = "test-lucene-temp-cleanup-forcemerge";

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();

        createIndex(forceMergeIndex, indexSettings);
        client().admin()
            .indices()
            .preparePutMapping(forceMergeIndex)
            .setSource("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        ensureGreen(forceMergeIndex);

        // Ingest 5 batches with refreshes to create multiple segments
        for (int batch = 0; batch < 5; batch++) {
            for (int i = 0; i < 100; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(forceMergeIndex)
                    .setSource("field_keyword", "value_" + batch + "_" + i, "field_number", i)
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(forceMergeIndex).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }

        // Force merge to 1 segment
        ForceMergeResponse forceMergeResponse = client().admin().indices().prepareForceMerge(forceMergeIndex).setMaxNumSegments(1).get();
        assertEquals(RestStatus.OK, forceMergeResponse.getStatus());

        // Get the shard's lucene base directory
        String nodeId = getClusterState().routingTable().index(forceMergeIndex).shard(0).primaryShard().currentNodeId();
        String nodeName = getClusterState().nodes().get(nodeId).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(forceMergeIndex));
        IndexShard shard = indexService.getShard(0);
        Path luceneBaseDir = shard.shardPath().getDataPath().resolve("lucene");

        // After force merge, the lucene_gen_* temp directories should NOT exist
        if (Files.exists(luceneBaseDir)) {
            try (Stream<Path> dirs = Files.list(luceneBaseDir)) {
                long staleGenDirs = dirs.filter(p -> p.getFileName().toString().startsWith("lucene_gen_")).count();
                assertEquals("Lucene temp directories (lucene_gen_*) should be cleaned up after force merge", 0L, staleGenDirs);
            }
        }
    }

    private IndexShard getPrimaryShard() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeName = getClusterState().nodes().get(nodeId).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }
}
