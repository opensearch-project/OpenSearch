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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.be.lucene.DocValuesBatchSourceFactory;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.substrait.extension.DefaultExtensionCatalog;

/** End-to-end Lucene doc-values to DataFusion pull-source execution test. */
public class DatafusionArrowBatchSourceExecutorTests extends OpenSearchTestCase {

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

    public void testExecutesLuceneDocValuesThroughDataFusion() throws Exception {
        Path spillDirectory = createTempDir("arrow-source-spill");
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64L * 1024L * 1024L)
            .spillMemoryLimit(32L * 1024L * 1024L)
            .spillDirectory(spillDirectory.toString())
            .cpuThreads(2)
            .build();
        service.start();

        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            ByteBuffersDirectory directory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))
        ) {
            addDocument(writer, 11L);
            addDocument(writer, 22L);
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
                    passthroughSubstrait("input-0"),
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                Task task = new Task(98_001L, "test", "arrow-source", "arrow-source", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
                DatafusionArrowBatchSourceExecutor executor = new DatafusionArrowBatchSourceExecutor(service);

                try (EngineResultStream stream = executor.execute(allocator, plan, factory, task, null)) {
                    Iterator<EngineResultBatch> batches = stream.iterator();
                    assertTrue(batches.hasNext());
                    EngineResultBatch batch = batches.next();
                    try (VectorSchemaRoot root = batch.getArrowRoot()) {
                        assertEquals(2, root.getRowCount());
                        BigIntVector values = (BigIntVector) root.getVector("x");
                        assertEquals(11L, values.get(0));
                        assertEquals(22L, values.get(1));
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
                    countSubstrait("input-0"),
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                try (EngineResultStream stream = executor.execute(allocator, countPlan, countFactory, task, null)) {
                    Iterator<EngineResultBatch> batches = stream.iterator();
                    assertTrue(batches.hasNext());
                    try (VectorSchemaRoot root = batches.next().getArrowRoot()) {
                        assertEquals(1, root.getRowCount());
                        assertEquals(1, root.getFieldVectors().size());
                        assertEquals(2L, ((BigIntVector) root.getVector(0)).get(0));
                    }
                    assertFalse(batches.hasNext());
                }
                expectThrows(IllegalStateException.class, () -> countFactory.open(new int[0]));
                assertEquals(initialRefCount, reader.getRefCount());
            }
        } finally {
            service.close();
        }
    }

    private static void addDocument(IndexWriter writer, long value) throws Exception {
        Document document = new Document();
        document.add(new NumericDocValuesField("x", value));
        writer.addDocument(document);
    }

    private static byte[] passthroughSubstrait(String inputId) {
        return convert(inputScan(inputId));
    }

    private static byte[] countSubstrait(String inputId) {
        RelNode scan = inputScan(inputId);
        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count"
        );
        return convert(LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(count)));
    }

    private static RelNode inputScan(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigint).build();
        return new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);
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
