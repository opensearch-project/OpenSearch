/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Base class for composite engine integration tests.
 *
 * <p>Provides common infrastructure for tests that need a composite index with
 * parquet primary + lucene secondary data formats. Subclasses inherit plugin
 * wiring, index creation helpers, and utility methods to access engine internals.
 */
@ThreadLeakFilters(filters = AbstractCompositeEngineIT.ParquetNativeThreadFilter.class)
public abstract class AbstractCompositeEngineIT extends OpenSearchIntegTestCase {

    /**
     * Suppresses generic "Thread-N" worker threads spawned by parquet's native (Rust) merge path.
     * These JNI threads have empty Java stack traces; the JVM's randomized testing framework
     * cannot identify their origin so they appear as leaks at suite teardown.
     */
    public static class ParquetNativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC_THREAD_NAME = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC_THREAD_NAME.matcher(t.getName()).matches();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            // Netty4ModulePlugin provides the real HTTP server transport. Subclasses that override
            // addMockHttpTransport() to return false rely on this plugin for the REST endpoints.
            Netty4ModulePlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            // Use Netty4 as the HTTP transport so getRestClient() ITs can hit /_plugins/* endpoints.
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            // Pin processors to a fixed value. InternalTestCluster otherwise randomizes node.processors per-test
            // which conflicts with Netty's process-wide NettyRuntime.availableProcessors static across test methods.
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 1)
            .build();
    }

    protected void createCompositeIndex(String indexName) {
        createCompositeIndex(indexName, true);
    }

    protected void createCompositeIndex(String indexName, boolean withLuceneSecondary) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet");

        if (withLuceneSecondary) {
            settingsBuilder.putList("index.composite.secondary_data_formats", "lucene");
        } else {
            settingsBuilder.putList("index.composite.secondary_data_formats");
        }

        String keywordMapping = "type=keyword";
        if (false == withLuceneSecondary) {
            keywordMapping += ",index=false";
        }

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settingsBuilder)
            .setMapping("name", keywordMapping, "value", "type=integer")
            .get();
        ensureGreen(indexName);
    }

    protected List<String> indexDocs(String indexName, int count, int startId) {
        List<String> ids = new ArrayList<>();
        for (int i = startId; i < startId + count; i++) {
            IndexResponse indexResponse = client().prepareIndex().setIndex(indexName).setSource("name", "doc_" + i, "value", i).get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
            ids.add(indexResponse.getId());
        }
        return ids;
    }

    protected void refreshIndex(String indexName) {
        client().admin().indices().prepareRefresh(indexName).get();
    }

    protected FlushResponse flushIndex(String indexName) {
        return client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
    }

    protected IndexShard getPrimaryShard(String indexName) {
        String nodeId = getClusterState().routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        String nodeName = getClusterState().nodes().get(nodeId).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        return indexService.getShard(0);
    }

    protected DataFormatAwareEngine getEngine(String indexName) {
        return (DataFormatAwareEngine) IndexShardTestCase.getIndexer(getPrimaryShard(indexName));
    }

    protected CatalogSnapshot acquireAndGetSnapshot(String indexName) throws IOException {
        DataFormatAwareEngine engine = getEngine(indexName);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get();
        }
    }

    protected DataformatAwareCatalogSnapshot getCommittedSnapshot(String indexName) throws IOException {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setDocs(true).get();
        ShardStats shardStats = statsResponse.getIndex(indexName).getShards()[0];
        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull("Commit stats must exist", commitStats);
        String serialized = commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY);
        assertNotNull("Committed snapshot must be present in commit data", serialized);
        return DataformatAwareCatalogSnapshot.deserializeFromString(serialized, Function.identity());
    }

    protected long getTotalRowCount(CatalogSnapshot snapshot) {
        return snapshot.getSegments()
            .stream()
            .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
            .mapToLong(org.opensearch.index.engine.exec.WriterFileSet::numRows)
            .sum();
    }
}
