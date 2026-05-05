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
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

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
            ExchangeSinkContext ctx = new ExchangeSinkContext("q-1", 0, substrait, alloc, inputSchema, downstream);

            DatafusionMemtableReduceSink sink = new DatafusionMemtableReduceSink(ctx, runtimeHandle);
            try {
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 1L, 2L, 3L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 4L, 5L, 6L }));
                sink.feed(makeBatch(alloc, inputSchema, new long[] { 7L, 8L, 9L }));
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

        return new DataFusionFragmentConvertor(loadExtensions()).convertFinalAggFragment(agg);
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
