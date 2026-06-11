/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Mirror of {@link DatafusionReduceSinkTests} for the memtable variant. Same Substrait plan, same
 * batches, same downstream assertion — exercises the buffered-batch handoff path instead of the
 * streaming sender path.
 */
public class DatafusionMemtableReduceSinkTests extends OpenSearchTestCase {

    public void testInputIdConstantMatchesDesign() {
        assertEquals("Single-input reduce uses the synthetic id 'input-0'", "input-0", DatafusionMemtableReduceSink.INPUT_ID);
    }

    public void testFeedDrainsSumToDownstream() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] substrait = buildSumSubstraitBytes(DatafusionMemtableReduceSink.INPUT_ID);

            CapturingSink downstream = new CapturingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-1",
                0,
                0L,
                substrait,
                alloc,
                List.of(new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes(DatafusionMemtableReduceSink.INPUT_ID))),
                downstream
            );

            DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
            try {
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 1L, 2L, 3L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 4L, 5L, 6L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 7L, 8L, 9L }));
                // Memtable runs the reduce work in reduce() now; close() is cleanup-only.
                PlainActionFuture<Void> reduceDone = PlainActionFuture.newFuture();
                sink.reduce(reduceDone);
                reduceDone.actionGet(10, TimeUnit.SECONDS);
            } finally {
                sink.close();
            }

            assertFalse("downstream must NOT be closed by the reduce sink", downstream.closed);
            assertTrue("downstream should receive at least one row, got " + downstream.totalRows, downstream.totalRows >= 1);
            assertEquals("SUM(1..9) should be 45", 45L, downstream.total);
        } finally {
            runtimeHandle.close();
        }
    }

    /**
     * Two-input coordinator join, the minimal shape of TPC-H q15's deadlock
     * ({@code supplier ⋈ revenue0}): a buffered multi-input reduce running
     * {@code Join(input-0, input-2) ON input-0.x = input-2.x}. Feeds both inputs via
     * {@link DatafusionMemtableReduceSink#sinkForChild(int)}, closes each, runs {@code reduce},
     * and asserts the join produced the matching rows. Exercises the multi-input MemTable path
     * end-to-end against a live native runtime — the streaming sink deadlocks on this shape
     * (build-side back-pressure), which is why join-reduces route to the buffered sink.
     */
    public void testTwoInputJoinDrainsToDownstream() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
            byte[] joinSubstrait = buildJoinSubstraitBytes("input-0", "input-2");

            CountingSink downstream = new CountingSink();
            ExchangeSinkContext ctx = new ExchangeSinkContext(
                "q-join",
                0,
                0L,
                joinSubstrait,
                alloc,
                List.of(
                    new ExchangeSinkContext.ChildInput(0, buildPassthroughSubstraitBytes("input-0")),
                    new ExchangeSinkContext.ChildInput(2, buildPassthroughSubstraitBytes("input-2"))
                ),
                downstream
            );

            DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
            try {
                // Build side (input-0): {1,2,3}. Probe side (input-2): {2,3,4}. Inner join on x → {2,3}.
                ExchangeSink left = sink.sinkForChild(0);
                ExchangeSink right = sink.sinkForChild(2);
                left.feed(makeBatch(alloc, inputSchema, new long[] { 1L, 2L, 3L }));
                right.feed(makeBatch(alloc, inputSchema, new long[] { 2L, 3L, 4L }));
                left.close();
                right.close();
                PlainActionFuture<Void> reduceDone = PlainActionFuture.newFuture();
                sink.reduce(reduceDone);
                reduceDone.actionGet(10, TimeUnit.SECONDS);
            } finally {
                sink.close();
            }

            assertFalse("downstream must NOT be closed by the reduce sink", downstream.closed);
            assertEquals("inner join {1,2,3} ⋈ {2,3,4} on x should yield 2 rows", 2, downstream.totalRows);
        } finally {
            runtimeHandle.close();
        }
    }

    private static byte[] buildSumSubstraitBytes(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        AggregateCall sumCall = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(0), -1, bigintNullable, "total");
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(agg);
    }

    /**
     * Bare {@code SELECT * FROM "input-0"} substrait — used as the producer-side plan in
     * {@link ExchangeSinkContext.ChildInput#producerPlanBytes()}. Its lowered output schema
     * is the leaf row type ({@code x: BIGINT}), which the sink registers as the input
     * partition's declared schema.
     */
    private static byte[] buildPassthroughSubstraitBytes(String inputId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), inputId, rowType);

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(scan);
    }

    /**
     * Builds {@code Join(StageInputScan(leftId), StageInputScan(rightId)) ON left.x = right.x}
     * substrait — the minimal two-input coordinator-join shape.
     */
    private static byte[] buildJoinSubstraitBytes(String leftId, String rightId) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();

        RelNode left = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), leftId, rowType);
        RelNode right = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), rightId, rowType);

        // Equi-join condition: left.x ($0) == right.x ($1 in the joined row).
        org.apache.calcite.rex.RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(bigintNullable, 0),
            rexBuilder.makeInputRef(bigintNullable, 1)
        );
        RelNode join = org.apache.calcite.rel.logical.LogicalJoin.create(
            left,
            right,
            List.of(),
            cond,
            java.util.Set.of(),
            org.apache.calcite.rel.core.JoinRelType.INNER
        );

        return new DataFusionFragmentConvertor(loadExtensions()).convertFragment(join);
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatafusionMemtableReduceSinkTests.class.getClassLoader());
            return DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private static VectorSchemaRoot makeBatch(BufferAllocator alloc, Schema schema, long[] values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();
        BigIntVector col = (BigIntVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            col.setSafe(i, values[i]);
        }
        col.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    /** Counts rows across fed batches (join output has multiple columns; we only assert cardinality). */
    private static final class CountingSink implements ExchangeSink {
        int totalRows;
        boolean closed;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                totalRows += batch.getRowCount();
            } finally {
                batch.close();
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }

    private static final class CapturingSink implements ExchangeSink {
        long total;
        int totalRows;
        boolean closed;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                BigIntVector col = (BigIntVector) batch.getVector(0);
                int rows = batch.getRowCount();
                totalRows += rows;
                for (int i = 0; i < rows; i++) {
                    total += col.getDataBuffer().getLong((long) i * BigIntVector.TYPE_WIDTH);
                }
            } finally {
                batch.close();
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }
}
