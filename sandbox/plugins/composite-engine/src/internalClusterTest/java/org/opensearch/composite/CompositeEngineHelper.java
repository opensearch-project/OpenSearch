/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.be.lucene.LuceneReader;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Shared utility methods for composite engine integration tests.
 * Stateless — all methods take explicit parameters so any IT can use them.
 */
public final class CompositeEngineHelper {

    private CompositeEngineHelper() {}

    // -- Index creation --

    public static void createCompositeIndex(
        OpenSearchIntegTestCase testCase,
        String indexName,
        String primaryFormat,
        String... secondaryFormats
    ) {
        Settings.Builder sb = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", primaryFormat);
        sb.putList("index.composite.secondary_data_formats", secondaryFormats);
        testCase.createIndex(indexName, sb.build());
        testCase.ensureGreen(indexName);
    }

    public static void createCompositeIndexWithMapping(
        OpenSearchIntegTestCase testCase,
        String indexName,
        String primaryFormat,
        Settings extraSettings,
        String... secondaryFormats
    ) {
        Settings.Builder sb = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", primaryFormat)
            .put(extraSettings);
        sb.putList("index.composite.secondary_data_formats", secondaryFormats);
        OpenSearchIntegTestCase.client()
            .admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(sb)
            .setMapping("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        testCase.ensureGreen(indexName);
    }

    // -- Engine access --

    public static IndexShard getPrimaryShard(ClusterService clusterService, InternalTestCluster cluster, String indexName) {
        ClusterState state = clusterService.state();
        String nodeId = state.routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        String nodeName = state.nodes().get(nodeId).getName();
        IndexService svc = cluster.getInstance(IndicesService.class, nodeName)
            .indexServiceSafe(state.metadata().index(indexName).getIndex());
        return svc.getShard(0);
    }

    public static DataFormatAwareEngine getEngine(ClusterService clusterService, InternalTestCluster cluster, String indexName) {
        return (DataFormatAwareEngine) IndexShardTestCase.getIndexer(getPrimaryShard(clusterService, cluster, indexName));
    }

    // -- Row counts --

    /**
     * Returns the total live-doc count for a format by querying its actual reader.
     *
     * <p>For Lucene we use {@link DirectoryReader#numDocs()} which excludes soft-deleted
     * docs (e.g., docs removed by {@code rollbackLastDoc}). For Parquet — which has no
     * delete primitive in this codebase yet — we fall back to summing
     * {@link WriterFileSet#numRows()} from the catalog snapshot, since the file footer's
     * numRows IS the authoritative source for a Parquet file.
     */
    public static long getRowCount(ClusterService clusterService, InternalTestCluster cluster, String indexName, String formatName)
        throws IOException {
        DataFormatAwareEngine engine = getEngine(clusterService, cluster, indexName);
        if ("lucene".equals(formatName)) {
            String nodeId = clusterService.state().routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
            String nodeName = clusterService.state().nodes().get(nodeId).getName();
            DataFormat luceneFormat = new DataFormatRegistry(cluster.getInstance(PluginsService.class, nodeName)).format(formatName);
            try (GatedCloseable<IndexReaderProvider.Reader> ref = engine.acquireReader()) {
                Object raw = ref.get().reader(luceneFormat);
                if (raw == null) {
                    return 0L;
                }
                if (raw instanceof LuceneReader == false) {
                    throw new IllegalStateException(
                        "Expected LuceneReader for format [" + formatName + "] but got " + raw.getClass().getName()
                    );
                }
                return ((LuceneReader) raw).directoryReader().numDocs();
            }
        }
        // Parquet path: file-footer numRows is the source of truth (no deletes for parquet today).
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSearchableFiles(formatName).stream().mapToLong(WriterFileSet::numRows).sum();
        }
    }

    /**
     * Engine-only overload — used when the caller already has the engine and only needs the
     * Parquet/file-metadata path. The Lucene branch routes through
     * {@link #getRowCount(ClusterService, InternalTestCluster, String, String)} which can
     * resolve {@link DataFormat} via {@link DataFormatRegistry}.
     */
    public static long getRowCount(DataFormatAwareEngine engine, String formatName) throws IOException {
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSearchableFiles(formatName).stream().mapToLong(WriterFileSet::numRows).sum();
        }
    }

    // -- Indexing --

    public static void indexDocs(OpenSearchIntegTestCase testCase, String indexName, int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(
                RestStatus.CREATED,
                OpenSearchIntegTestCase.client()
                    .prepareIndex(indexName)
                    .setSource(
                        "field_keyword",
                        OpenSearchIntegTestCase.randomAlphaOfLength(10),
                        "field_number",
                        OpenSearchIntegTestCase.randomIntBetween(0, 1000)
                    )
                    .get()
                    .status()
            );
        }
    }

    // -- Flush --

    public static void flush(OpenSearchIntegTestCase testCase, String indexName) {
        OpenSearchIntegTestCase.client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
    }

    // -- Assertions --

    public static void assertPerSegmentRowCountsMatch(DataFormatAwareEngine engine, String format1, String format2) throws IOException {
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            for (Segment seg : ref.get().getSegments()) {
                WriterFileSet wfs1 = seg.dfGroupedSearchableFiles().get(format1);
                WriterFileSet wfs2 = seg.dfGroupedSearchableFiles().get(format2);
                assertNotNull("segment gen=" + seg.generation() + " missing " + format1, wfs1);
                assertNotNull("segment gen=" + seg.generation() + " missing " + format2, wfs2);
                assertEquals("segment gen=" + seg.generation() + " row counts must match", wfs1.numRows(), wfs2.numRows());
            }
        }
    }
}
