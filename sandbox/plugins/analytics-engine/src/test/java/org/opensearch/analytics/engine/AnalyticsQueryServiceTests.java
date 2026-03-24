/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.RelNode;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.AnalyticsQueryService;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end tests for {@link AnalyticsQueryService} with mock
 * {@link SearchExecEngine} and {@link AnalyticsSearchBackendPlugin}.
 * Validates the write → refresh → catalog snapshot → acquire reader → execute path.
 */
public class AnalyticsQueryServiceTests extends OpenSearchTestCase {

    /**
     * Full lifecycle: build catalog snapshot from writer file sets, wire up
     * DataFormatAwareEngine, and execute via AnalyticsQueryService with a
     * mock backend that returns rows from the reader.
     */
    public void testEndToEndExecuteViaAnalyticsQueryService() throws IOException {
        MockDataFormat format = new MockDataFormat();
        Path dir = createTempDir();

        // Simulate two writer generations producing file sets
        WriterFileSet fs1 = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("data_gen1.parquet").addNumRows(2).build();
        WriterFileSet fs2 = WriterFileSet.builder().directory(dir).writerGeneration(2L).addFile("data_gen2.parquet").addNumRows(1).build();

        // Build segments and catalog snapshot
        Segment seg1 = Segment.builder(0L).addSearchableFiles(format, fs1).build();
        Segment seg2 = Segment.builder(1L).addSearchableFiles(format, fs2).build();
        MockCatalogSnapshot snapshot = new MockCatalogSnapshot(1L, List.of(seg1, seg2), format);

        // Wire reader manager and DataFormatAwareEngine
        MockReaderManager readerManager = new MockReaderManager(format.name());
        readerManager.afterRefresh(true, snapshot);

        DataFormatAwareEngine engine = new DataFormatAwareEngine(Map.of(format, readerManager));
        engine.setLatestSnapshot(snapshot);

        // Mock IndexShard to return our engine
        IndexShard shard = mock(IndexShard.class);
        when(shard.getCompositeEngine()).thenReturn(engine);

        // Create mock backend plugin that returns rows based on reader content
        MockBackendPlugin backendPlugin = new MockBackendPlugin(format);
        AnalyticsQueryService service = new AnalyticsQueryService(Map.of("mock-backend", backendPlugin));

        // Build a resolved plan targeting our mock backend
        RelNode mockRoot = mock(RelNode.class);
        when(mockRoot.getTable()).thenReturn(null);
        ResolvedPlan plan = new ResolvedPlan(mockRoot, "mock-backend", Map.of());

        Iterable<Object[]> results = service.execute(plan, shard, mock(SearchShardTask.class));
        List<Object[]> rows = new ArrayList<>();
        results.forEach(rows::add);

        // Mock engine returns 3 total rows (2 from gen1 + 1 from gen2)
        assertEquals(3, rows.size());
        assertEquals(0, service.getActiveContextCount());
    }

    /**
     * Verifies context tracking: contexts are registered during execution
     * and cleaned up after completion.
     */
    public void testContextTrackingLifecycle() {
        AnalyticsQueryService service = new AnalyticsQueryService(Map.of());
        ExecutionContext ctx = new ExecutionContext(null, "test-table", null, null);

        long id = service.putContext(ctx);
        assertEquals(1, service.getActiveContextCount());
        assertSame(ctx, service.getContext(id));

        ExecutionContext removed = service.removeContext(id);
        assertSame(ctx, removed);
        assertEquals(0, service.getActiveContextCount());
        assertNull(service.getContext(id));
    }

    /**
     * Verifies that execute throws when no backend plugin is registered
     * for the plan's primary backend.
     */
    public void testExecuteThrowsForUnknownBackend() {
        MockDataFormat format = new MockDataFormat();
        Path dir = createTempDir();
        WriterFileSet fs = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("f.parquet").addNumRows(1).build();
        Segment seg = Segment.builder(0L).addSearchableFiles(format, fs).build();
        MockCatalogSnapshot snapshot = new MockCatalogSnapshot(1L, List.of(seg), format);

        MockReaderManager rm = new MockReaderManager(format.name());
        rm.afterRefresh(true, snapshot);
        DataFormatAwareEngine engine = new DataFormatAwareEngine(Map.of(format, rm));
        engine.setLatestSnapshot(snapshot);

        IndexShard shard = mock(IndexShard.class);
        when(shard.getCompositeEngine()).thenReturn(engine);

        AnalyticsQueryService service = new AnalyticsQueryService(Map.of());
        ResolvedPlan plan = new ResolvedPlan(null, "nonexistent", Map.of());

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> service.execute(plan, shard, null));
        assertTrue(ex.getMessage().contains("No plugin registered for backend"));
    }

    /**
     * Verifies that execute throws when shard has no composite engine.
     */
    public void testExecuteThrowsWhenNoCompositeEngine() {
        IndexShard shard = mock(IndexShard.class);
        when(shard.getCompositeEngine()).thenReturn(null);
        when(shard.shardId()).thenReturn(new org.opensearch.core.index.shard.ShardId("idx", "uuid", 0));

        AnalyticsQueryService service = new AnalyticsQueryService(Map.of("be", mock(AnalyticsSearchBackendPlugin.class)));
        ResolvedPlan plan = new ResolvedPlan(null, "be", Map.of());

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> service.execute(plan, shard, null));
        assertTrue(ex.getMessage().contains("No CompositeEngine on shard"));
    }

    // --- Mock implementations ---

    static class MockDataFormat implements DataFormat {
        @Override
        public String name() {
            return "mock-columnar";
        }

        @Override
        public long priority() {
            return 100L;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of(
                new FieldTypeCapabilities(
                    "integer",
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                )
            );
        }
    }

    static class MockReaderManager implements EngineReaderManager<Object> {
        private final String formatName;
        private final Map<CatalogSnapshot, Object> readers = new HashMap<>();

        MockReaderManager(String formatName) {
            this.formatName = formatName;
        }

        @Override
        public Object getReader(CatalogSnapshot snapshot) {
            return readers.get(snapshot);
        }

        @Override
        public void beforeRefresh() {}

        @Override
        public void afterRefresh(boolean didRefresh, CatalogSnapshot snapshot) {
            if (didRefresh == false || readers.containsKey(snapshot)) return;
            Collection<WriterFileSet> files = snapshot.getSearchableFiles(formatName);
            long totalRows = 0;
            for (WriterFileSet wfs : files) {
                totalRows += wfs.numRows();
            }
            readers.put(snapshot, totalRows);
        }

        @Override
        public void onDeleted(CatalogSnapshot snapshot) {
            readers.remove(snapshot);
        }

        @Override
        public void onFilesDeleted(Collection<String> files) {}

        @Override
        public void onFilesAdded(Collection<String> files) {}
    }

    static class MockCatalogSnapshot extends CatalogSnapshot {
        private final List<Segment> segments;
        private final MockDataFormat format;

        MockCatalogSnapshot(long generation, List<Segment> segments, MockDataFormat format) {
            super("mock-snapshot", generation, 1L);
            this.segments = segments;
            this.format = format;
        }

        @Override
        public Map<String, String> getUserData() {
            return Map.of();
        }

        @Override
        public long getId() {
            return generation;
        }

        @Override
        public List<Segment> getSegments() {
            return segments;
        }

        @Override
        public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
            List<WriterFileSet> result = new ArrayList<>();
            for (Segment seg : segments) {
                WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(dataFormat);
                if (wfs != null) result.add(wfs);
            }
            return result;
        }

        @Override
        public Set<String> getDataFormats() {
            return Set.of(format.name());
        }

        @Override
        public long getLastWriterGeneration() {
            return generation;
        }

        @Override
        public String serializeToString() {
            return "mock-snapshot-" + generation;
        }

        @Override
        public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> map) {}

        @Override
        public void setUserData(Map<String, String> userData, boolean b) {}

        @Override
        public Object getReader(DataFormat dataFormat) {
            return null;
        }

        @Override
        protected void closeInternal() {}
    }

    /**
     * Mock SearchExecEngine that produces rows based on a fixed count
     * provided at construction time.
     */
    static class MockSearchExecEngine implements SearchExecEngine {
        private final long totalRows;

        MockSearchExecEngine(long totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public void prepare(ExecutionContext context) {}

        @Override
        public EngineResultStream execute(ExecutionContext context) {
            return new MockResultStream(totalRows);
        }

        @Override
        public void close() {}
    }

    static class MockResultStream implements EngineResultStream {
        private final long rowCount;

        MockResultStream(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public EngineResultBatchIterator iterator() {
            return new MockBatchIterator(rowCount);
        }

        @Override
        public void close() {}
    }

    static class MockBatchIterator implements EngineResultBatchIterator {
        private final long rowCount;
        private boolean consumed;

        MockBatchIterator(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public boolean hasNext() {
            return consumed == false;
        }

        @Override
        public EngineResultBatch next() {
            consumed = true;
            return new MockResultBatch((int) rowCount);
        }
    }

    static class MockResultBatch implements EngineResultBatch {
        private final int rowCount;

        MockResultBatch(int rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public List<String> getFieldNames() {
            return List.of("value");
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        @Override
        public Object getFieldValue(String fieldName, int rowIndex) {
            return "row_" + rowIndex;
        }
    }

    static class MockBackendPlugin implements AnalyticsSearchBackendPlugin {
        private final DataFormat format;

        MockBackendPlugin(DataFormat format) {
            this.format = format;
        }

        @Override
        public String name() {
            return "mock-backend";
        }

        @Override
        public SearchExecEngine searcher(ExecutionContext ctx) {
            // Reader manager stores totalRows (Long) as the reader object
            Object reader = ctx.getReader().getReader(format);
            long rows = reader instanceof Long ? (Long) reader : 0L;
            return new MockSearchExecEngine(rows);
        }

        @Override
        public List<DataFormat> getSupportedFormats() {
            return List.of(format);
        }
    }
}
