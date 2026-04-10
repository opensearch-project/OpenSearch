/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DefaultPlanExecutor}.
 */
public class DefaultPlanExecutorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * extractTableName returns the table name from a TableScan node.
     */
    public void testExtractTableNameFromTableScan() {
        RelOptTable table = mockTable("schema", "my_index");
        TableScan scan = new StubTableScan(cluster, cluster.traitSet(), table);
        assertEquals("my_index", DefaultPlanExecutor.extractTableName(scan));
    }

    /**
     * extractTableName throws when no TableScan is found.
     */
    public void testExtractTableNameThrowsForNonTableScan() {
        RelNode stub = new StubRelNode(cluster, cluster.traitSet(), buildRowType(1));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> DefaultPlanExecutor.extractTableName(stub));
        assertTrue(ex.getMessage().contains("No TableScan found"));
    }

    /**
     * End-to-end: write file sets → catalog snapshot → DataFormatAwareEngine →
     * DefaultPlanExecutor.execute() with mock backend returns rows via EngineResultStream.
     */
    public void testEndToEndExecuteWithMockBackend() throws IOException {
        MockDataFormat format = new MockDataFormat();
        Path dir = createTempDir();

        WriterFileSet fs1 = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("gen1.parquet").addNumRows(2).build();
        WriterFileSet fs2 = WriterFileSet.builder().directory(dir).writerGeneration(2L).addFile("gen2.parquet").addNumRows(1).build();

        Segment seg1 = Segment.builder(0L).addSearchableFiles(format, fs1).build();
        Segment seg2 = Segment.builder(1L).addSearchableFiles(format, fs2).build();

        CatalogSnapshotManager snapshotManager = new CatalogSnapshotManager(1L, 1L, 0L, List.of(seg1, seg2), 2L, Map.of());

        MockReaderManager readerManager = new MockReaderManager(format.name());
        try (GatedCloseable<CatalogSnapshot> ref = snapshotManager.acquireSnapshot()) {
            readerManager.afterRefresh(true, ref.get());
        }

        DataFormatAwareEngine engine = new DataFormatAwareEngine(Map.of(format, readerManager), snapshotManager);

        // Mock shard + cluster wiring
        IndexShard shard = mock(IndexShard.class);
        when(shard.getCompositeEngine()).thenReturn(engine);

        Index index = new Index("my_index", "uuid");
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(index);

        Metadata metadata = mock(Metadata.class);
        when(metadata.index("my_index")).thenReturn(indexMetadata);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        IndexService indexService = mock(IndexService.class);
        when(indexService.shardIds()).thenReturn(Set.of(0));
        when(indexService.getShardOrNull(0)).thenReturn(shard);

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexService(index)).thenReturn(indexService);

        MockBackendPlugin backendPlugin = new MockBackendPlugin(format);
        DefaultPlanExecutor executor = new DefaultPlanExecutor(List.of(backendPlugin), indicesService, clusterService);

        RelOptTable table = mockTable("my_index");
        TableScan scan = new StubTableScan(cluster, cluster.traitSet(), table);

        Iterable<Object[]> results = executor.execute(scan, new Object());
        List<Object[]> rows = new ArrayList<>();
        results.forEach(rows::add);

        assertEquals(3, rows.size());
    }

    private RelOptTable mockTable(String... qualifiedName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(qualifiedName));
        when(table.getRowType()).thenReturn(buildRowType(1));
        return table;
    }

    private RelDataType buildRowType(int fieldCount) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add("field_" + i, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }

    private static class StubRelNode extends AbstractRelNode {
        StubRelNode(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
            super(cluster, traitSet);
            this.rowType = rowType;
        }
    }

    private static class StubTableScan extends TableScan {
        StubTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }

    static class MockDataFormat extends DataFormat {
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
            long totalRows = 0;
            for (WriterFileSet wfs : snapshot.getSearchableFiles(formatName)) {
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
        private final DataFormat format;

        MockCatalogSnapshot(long generation, List<Segment> segments, DataFormat format) {
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
        public void setUserData(Map<String, String> userData) {}

        @Override
        public MockCatalogSnapshot clone() {
            return new MockCatalogSnapshot(generation, segments, format);
        }

        @Override
        protected void closeInternal() {}
    }

    static class MockSearchExecEngine implements SearchExecEngine<ExecutionContext, EngineResultStream> {
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
        public Iterator<EngineResultBatch> iterator() {
            return new MockBatchIterator(rowCount);
        }

        @Override
        public void close() {}
    }

    static class MockBatchIterator implements Iterator<EngineResultBatch> {
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
        public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
            Object reader = ctx.getReader().reader(format);
            long rows = reader instanceof Long ? (Long) reader : 0L;
            return new MockSearchExecEngine(rows);
        }
    }
}
