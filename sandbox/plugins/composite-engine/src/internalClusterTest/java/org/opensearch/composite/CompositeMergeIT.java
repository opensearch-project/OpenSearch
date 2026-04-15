/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Integration tests for composite merge operations across single and multiple data format engines.
 *
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeMergeIT" \
 *   -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-merge";

    // ── Stub DataFormat for "parquet" ──

    private static final DataFormat STUB_PARQUET_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return "parquet";
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    // ── Stub DocumentInput ──

    private static class StubDocumentInput implements DocumentInput<Object> {
        @Override
        public void addField(MappedFieldType fieldType, Object value) {}

        @Override
        public void setRowId(String rowIdFieldName, long rowId) {}

        @Override
        public Object getFinalInput() {
            return null;
        }

        @Override
        public void close() {}
    }

    // ── Stub Writer ──

    private static class StubWriter implements Writer<StubDocumentInput> {
        private final long generation;
        private int docCount = 0;

        StubWriter(long generation) {
            this.generation = generation;
        }

        @Override
        public WriteResult addDoc(StubDocumentInput documentInput) {
            docCount++;
            return new WriteResult.Success(1L, 1L, docCount);
        }

        @Override
        public FileInfos flush() {
            if (docCount == 0) {
                return FileInfos.empty();
            }
            WriterFileSet wfs = new WriterFileSet("/tmp/stub", generation, Set.of("stub_" + generation + ".parquet"), docCount);
            return new FileInfos(Map.of(STUB_PARQUET_FORMAT, wfs));
        }

        @Override
        public void sync() {}

        @Override
        public long generation() {
            return generation;
        }

        @Override
        public void lock() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public void close() throws IOException {}
    }

    // ── Stub IndexingExecutionEngine ──

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static class StubParquetEngine implements IndexingExecutionEngine<DataFormat, StubDocumentInput> {

        @Override
        public Writer<StubDocumentInput> createWriter(long writerGeneration) {
            return new StubWriter(writerGeneration);
        }

        @Override
        public Merger getMerger() {
            return mergeInput -> {
                long totalRows = mergeInput.writerFiles().stream().mapToLong(WriterFileSet::numRows).sum();
                WriterFileSet merged = new WriterFileSet("/tmp/stub", mergeInput.newWriterGeneration(),
                    Set.of("merged_" + mergeInput.newWriterGeneration() + ".parquet"), totalRows);
                return new MergeResult(Map.of(STUB_PARQUET_FORMAT, merged));
            };
        }

        @Override
        public RefreshResult refresh(RefreshInput refreshInput) {
            if (refreshInput == null) return new RefreshResult(List.of());
            List<Segment> segments = new ArrayList<>();
            segments.addAll(refreshInput.existingSegments());
            segments.addAll(refreshInput.writerFiles());
            return new RefreshResult(List.copyOf(segments));
        }

        @Override
        public long getNextWriterGeneration() {
            return 0;
        }

        @Override
        public DataFormat getDataFormat() {
            return STUB_PARQUET_FORMAT;
        }

        @Override
        public long getNativeBytesUsed() {
            return 0;
        }

        @Override
        public void deleteFiles(Map<String, Collection<String>> filesToDelete) {}

        @Override
        public StubDocumentInput newDocumentInput() {
            return new StubDocumentInput();
        }

        @Override
        public IndexStoreProvider getProvider() {
            return null;
        }

        @Override
        public void close() {}
    }

    // ── Stub DataFormatPlugin ──

    public static class MockParquetDataFormatPlugin extends Plugin implements DataFormatPlugin {
        @Override
        public DataFormat getDataFormat() {
            return STUB_PARQUET_FORMAT;
        }

        @Override
        public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings, FormatChecksumStrategy checksumStrategy) {
            return new StubParquetEngine();
        }

        @Override
        public Map<String, DataFormatDescriptor> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
            return Map.of("parquet", new DataFormatDescriptor("parquet", new PrecomputedChecksumStrategy()));
        }
    }

    // ── Test setup ──

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist if test failed before creation
        }
        super.tearDown();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    // ── Tests ──

    /**
     * Verifies that background merges are triggered automatically after refresh
     * when enough segments accumulate to exceed the TieredMergePolicy threshold.
     * <p>
     * Flow: index docs across many refresh cycles → each refresh calls
     * triggerPossibleMerges() → MergeScheduler picks up merge candidates
     * asynchronously → segment count decreases.
     */
    public void testBackgroundMergeSingleEngine() throws Exception {
        createIndex(INDEX_NAME, singleEngineSettings());
        ensureGreen(INDEX_NAME);

        // Create enough segments to exceed TieredMergePolicy's default threshold (~10)
        int totalSegmentsCreated = indexDocsAcrossMultipleRefreshes(15, 5);

        // Wait for async background merges to complete
        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + totalSegmentsCreated
                    + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < totalSegmentsCreated
            );
        });

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
    }

    // ── Helpers ──

    private Settings singleEngineSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }


    private int indexDocsAcrossMultipleRefreshes(int refreshCycles, int docsPerCycle) {
        int totalDocs = 0;
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource("field_text", randomAlphaOfLength(10), "field_number", randomIntBetween(1, 1000))
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
                totalDocs++;
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
        return totalDocs;
    }

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setStore(true).get();
        ShardStats shardStats = statsResponse.getIndex(INDEX_NAME).getShards()[0];
        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        return DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
    }

    private MergeStats getMergeStats() {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setMerge(true).get();
        return statsResponse.getIndex(INDEX_NAME).getShards()[0].getStats().getMerge();
    }
}
