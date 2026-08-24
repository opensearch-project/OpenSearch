/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.be.lucene.DocValuesBatchSourceFactory;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.substrait.extension.DefaultExtensionCatalog;

/** End-to-end Lucene doc-values to DataFusion pull-source execution test. */
public class DatafusionArrowBatchSourceExecutorTests extends OpenSearchTestCase {

    public void testCompilesNamedArrowSourcePlan() throws Exception {
        DataFusionPlugin plugin = org.mockito.Mockito.mock(DataFusionPlugin.class);
        org.mockito.Mockito.when(plugin.getSubstraitExtensions()).thenReturn(DefaultExtensionCatalog.DEFAULT_COLLECTION);
        DatafusionArrowBatchSourceExecutor executor = new DatafusionArrowBatchSourceExecutor(DataFusionService.builder().build(), plugin);

        byte[] planBytes = executor.compile(inputScan("input-0"), false);

        io.substrait.proto.Plan plan = io.substrait.proto.Plan.parseFrom(planBytes);
        assertEquals("input-0", plan.getRelations(0).getRoot().getInput().getRead().getNamedTable().getNames(0));
    }

    public void testSetupFailureClosesTransferredFactory() {
        AtomicInteger closes = new AtomicInteger();
        ArrowBatchSourceFactory factory = new ArrowBatchSourceFactory() {
            @Override
            public ArrowBatchSource open(int[] projection) {
                throw new AssertionError("source must not open when the service is stopped");
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
        ArrowBatchSourcePlan plan = new ArrowBatchSourcePlan("input-0", new byte[] { 1 }, List.of(new InputColumn("x", ColumnKind.LONG)));
        DataFusionService stoppedService = DataFusionService.builder().build();
        DatafusionArrowBatchSourceExecutor executor = new DatafusionArrowBatchSourceExecutor(stoppedService);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            expectThrows(IllegalStateException.class, () -> executor.execute(allocator, plan, factory, null, null));
        }
        assertEquals(1, closes.get());
    }

    public void testCancelledBeforeSetupClosesTransferredFactory() {
        AtomicInteger closes = new AtomicInteger();
        ArrowBatchSourceFactory factory = new ArrowBatchSourceFactory() {
            @Override
            public ArrowBatchSource open(int[] projection) {
                throw new AssertionError("source must not open after cancellation");
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
        CancellableTask task = new CancellableTask(98_002L, "test", "arrow-source", "cancelled", TaskId.EMPTY_TASK_ID, Map.of()) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return false;
            }
        };
        task.cancel("test cancellation");
        ArrowBatchSourcePlan plan = new ArrowBatchSourcePlan("input-0", new byte[] { 1 }, List.of(new InputColumn("x", ColumnKind.LONG)));
        DatafusionArrowBatchSourceExecutor executor = new DatafusionArrowBatchSourceExecutor(DataFusionService.builder().build());

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            expectThrows(TaskCancelledException.class, () -> executor.execute(allocator, plan, factory, task, null));
        }
        assertEquals(1, closes.get());
    }

    public void testEarlyResultStreamCloseReleasesLuceneFactory() throws Exception {
        Path spillDirectory = createTempDir("arrow-source-early-close-spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64L * 1024L * 1024L)
            .spillMemoryLimit(32L * 1024L * 1024L)
            .spillDirectory(spillDirectory.toString())
            .cpuThreads(2)
            .build();
        service.start();
        DataFusionPlugin plugin = org.mockito.Mockito.mock(DataFusionPlugin.class);
        org.mockito.Mockito.when(plugin.getSubstraitExtensions()).thenReturn(DefaultExtensionCatalog.DEFAULT_COLLECTION);
        DatafusionArrowBatchSourceExecutor executor = new DatafusionArrowBatchSourceExecutor(service, plugin);

        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            ByteBuffersDirectory directory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))
        ) {
            addDocument(writer, 11L, "a");
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("x", ColumnKind.LONG)),
                    allocator,
                    null
                );
                ArrowBatchSourcePlan plan = new ArrowBatchSourcePlan(
                    "input-0",
                    executor.compile(inputScan("input-0"), false),
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                EngineResultStream stream = executor.execute(allocator, plan, factory, null, null);
                assertTrue("factory and native source hold reader references", reader.getRefCount() > initialRefCount);

                stream.close();
                stream.close();

                assertBusy(() -> assertEquals(initialRefCount, reader.getRefCount()));
                expectThrows(IllegalStateException.class, () -> factory.open(new int[] { 0 }));
            }
        } finally {
            service.close();
        }
    }

    public void testExecutesLuceneDocValuesThroughDataFusion() throws Exception {
        Path spillDirectory = createTempDir("arrow-source-spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64L * 1024L * 1024L)
            .spillMemoryLimit(32L * 1024L * 1024L)
            .spillDirectory(spillDirectory.toString())
            .cpuThreads(2)
            .build();
        service.start();
        DataFusionPlugin plugin = org.mockito.Mockito.mock(DataFusionPlugin.class);
        org.mockito.Mockito.when(plugin.getSubstraitExtensions()).thenReturn(DefaultExtensionCatalog.DEFAULT_COLLECTION);
        DatafusionArrowBatchSourceExecutor executor = new DatafusionArrowBatchSourceExecutor(service, plugin);

        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            ByteBuffersDirectory directory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))
        ) {
            addDocument(writer, 11L, "a");
            addDocument(writer, 22L, "b");
            addDocument(writer, null, "missing");
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("x", ColumnKind.LONG)),
                    allocator,
                    null
                );
                ArrowBatchSourcePlan plan = new ArrowBatchSourcePlan(
                    "input-0",
                    executor.compile(filteredInput("input-0", 15L), false),
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                Task task = new Task(98_001L, "test", "arrow-source", "arrow-source", TaskId.EMPTY_TASK_ID, Collections.emptyMap());

                try (EngineResultStream stream = executor.execute(allocator, plan, factory, task, null)) {
                    Iterator<EngineResultBatch> batches = stream.iterator();
                    assertTrue(batches.hasNext());
                    EngineResultBatch batch = batches.next();
                    try (VectorSchemaRoot root = batch.getArrowRoot()) {
                        assertEquals(1, root.getRowCount());
                        BigIntVector values = (BigIntVector) root.getVector("x");
                        assertEquals(22L, values.get(0));
                    }
                    assertFalse(batches.hasNext());
                }

                expectThrows(IllegalStateException.class, () -> factory.open(new int[] { 0 }));
                assertEquals(initialRefCount, reader.getRefCount());

                DocValuesBatchSourceFactory countFactory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("x", ColumnKind.LONG)),
                    allocator,
                    null
                );
                ArrowBatchSourcePlan countPlan = new ArrowBatchSourcePlan(
                    "input-0",
                    countSubstrait("input-0", true),
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                try (EngineResultStream stream = executor.execute(allocator, countPlan, countFactory, task, null)) {
                    Iterator<EngineResultBatch> batches = stream.iterator();
                    assertTrue(batches.hasNext());
                    try (VectorSchemaRoot root = batches.next().getArrowRoot()) {
                        assertEquals(1, root.getRowCount());
                        assertEquals(1, root.getFieldVectors().size());
                        assertEquals(3L, ((BigIntVector) root.getVector(0)).get(0));
                    }
                    assertFalse(batches.hasNext());
                }
                expectThrows(IllegalStateException.class, () -> countFactory.open(new int[0]));
                assertEquals(initialRefCount, reader.getRefCount());

                DocValuesBatchSourceFactory fieldCountFactory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("x", ColumnKind.LONG)),
                    allocator,
                    null
                );
                ArrowBatchSourcePlan fieldCountPlan = new ArrowBatchSourcePlan(
                    "input-0",
                    countSubstrait("input-0", false),
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                try (EngineResultStream stream = executor.execute(allocator, fieldCountPlan, fieldCountFactory, task, null)) {
                    Iterator<EngineResultBatch> batches = stream.iterator();
                    assertTrue(batches.hasNext());
                    try (VectorSchemaRoot root = batches.next().getArrowRoot()) {
                        assertEquals(2L, ((BigIntVector) root.getVector(0)).get(0));
                    }
                    assertFalse(batches.hasNext());
                }
                expectThrows(IllegalStateException.class, () -> fieldCountFactory.open(new int[] { 0 }));
                assertEquals(initialRefCount, reader.getRefCount());

                DocValuesBatchSourceFactory keywordFactory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("keyword", ColumnKind.KEYWORD)),
                    allocator,
                    null
                );
                ArrowBatchSourcePlan keywordPlan = new ArrowBatchSourcePlan(
                    "input-0",
                    executor.compile(inputScan("input-0", "keyword", SqlTypeName.VARCHAR), false),
                    List.of(new InputColumn("keyword", ColumnKind.KEYWORD))
                );
                try (EngineResultStream stream = executor.execute(allocator, keywordPlan, keywordFactory, task, null)) {
                    Iterator<EngineResultBatch> batches = stream.iterator();
                    assertTrue(batches.hasNext());
                    try (VectorSchemaRoot root = batches.next().getArrowRoot()) {
                        assertEquals(3, root.getRowCount());
                        assertEquals("a", root.getVector("keyword").getObject(0).toString());
                        assertEquals("b", root.getVector("keyword").getObject(1).toString());
                        assertEquals("missing", root.getVector("keyword").getObject(2).toString());
                    }
                    assertFalse(batches.hasNext());
                }
                expectThrows(IllegalStateException.class, () -> keywordFactory.open(new int[] { 0 }));
                assertEquals(initialRefCount, reader.getRefCount());
            }
        } finally {
            service.close();
        }
    }

    private static void addDocument(IndexWriter writer, Long value, String keyword) throws Exception {
        Document document = new Document();
        if (value != null) {
            document.add(new NumericDocValuesField("x", value));
        }
        document.add(new SortedDocValuesField("keyword", new BytesRef(keyword)));
        writer.addDocument(document);
    }

    private static byte[] countSubstrait(String inputId, boolean countStar) {
        RelNode scan = inputScan(inputId);
        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            countStar ? List.of() : List.of(0),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count"
        );
        return convert(LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(count)));
    }

    private static RelNode filteredInput(String inputId, long lowerBound) {
        RelNode scan = inputScan(inputId);
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        return org.apache.calcite.rel.logical.LogicalFilter.create(
            scan,
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(scan, 0),
                rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(lowerBound))
            )
        );
    }

    private static RelNode inputScan(String inputId) {
        return inputScan(inputId, "x", SqlTypeName.BIGINT);
    }

    private static RelNode inputScan(String inputId, String fieldName, SqlTypeName type) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelDataType fieldType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(type), true);
        RelDataType rowType = typeFactory.builder().add(fieldName, fieldType).build();
        int childStageId = Integer.parseInt(inputId.substring("input-".length()));
        return new OpenSearchStageInputScan(cluster, cluster.traitSet(), childStageId, rowType, List.of(), List.of());
    }

    private static byte[] convert(RelNode node) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(DatafusionArrowBatchSourceExecutorTests.class.getClassLoader());
            return new DataFusionFragmentConvertor(DefaultExtensionCatalog.DEFAULT_COLLECTION).convertFragment(node);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
