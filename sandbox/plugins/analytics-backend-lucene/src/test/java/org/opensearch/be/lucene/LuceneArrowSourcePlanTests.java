/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ArrowBatchSourceExecutor;
import org.opensearch.analytics.spi.ArrowBatchSourceExecutorHolder;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class LuceneArrowSourcePlanTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
    }

    public void testAggregateShapeUsesOnlyReferencedDocValuesColumns() {
        RelDataType bigint = nullable(SqlTypeName.BIGINT);
        RelDataType rowType = typeFactory.builder().add("key", bigint).add("metric", bigint).add("unused", bigint).build();
        RelNode scan = scan(
            rowType,
            List.of(storage("key", FieldType.LONG), storage("metric", FieldType.LONG), storage("unused", FieldType.LONG))
        );
        AggregateCall sum = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(1), -1, scan, bigint, "sum_metric");
        RelNode aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sum));

        LuceneFragmentConvertor.ArrowSourceShape shape = LuceneFragmentConvertor.extractArrowSourceShape(aggregate);

        assertNotNull(shape);
        assertEquals(List.of("key", "metric"), shape.inputColumns().stream().map(ArrowBatchSourceFactory.InputColumn::name).toList());
        assertEquals(List.of("key", "sum_metric"), shape.outputNames());
        assertTrue(shape.rebasedFragment().getInput(0) instanceof OpenSearchStageInputScan);
        assertEquals(List.of("key", "metric"), shape.rebasedFragment().getInput(0).getRowType().getFieldNames());
    }

    public void testCountFieldUsesArrowSourceForNullSemantics() {
        RelDataType bigint = nullable(SqlTypeName.BIGINT);
        RelNode scan = scan(typeFactory.builder().add("metric", bigint).build(), List.of(storage("metric", FieldType.LONG)));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(0),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count_metric"
        );
        RelNode aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(count));

        assertFalse(LuceneFragmentConvertor.isCountFastPath(aggregate));
        assertNotNull(LuceneFragmentConvertor.extractArrowSourceShape(aggregate));
    }

    public void testCountStarRetainsMetadataFastPath() {
        RelDataType bigint = nullable(SqlTypeName.BIGINT);
        RelNode scan = scan(typeFactory.builder().add("metric", bigint).build(), List.of(storage("metric", FieldType.LONG)));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count_star"
        );
        RelNode aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(count));

        assertTrue(LuceneFragmentConvertor.isCountFastPath(aggregate));
        assertNull(LuceneFragmentConvertor.extractArrowSourceShape(aggregate));
    }

    public void testRowProjectionRebasesKeywordInput() {
        RelDataType varchar = nullable(SqlTypeName.VARCHAR);
        RelDataType rowType = typeFactory.builder().add("keyword", varchar).add("unused", varchar).build();
        RelNode scan = scan(rowType, List.of(storage("keyword", FieldType.KEYWORD), storage("unused", FieldType.KEYWORD)));
        RelNode project = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("keyword"));

        LuceneFragmentConvertor.ArrowSourceShape shape = LuceneFragmentConvertor.extractArrowSourceShape(project);

        assertNotNull(shape);
        assertEquals(List.of("keyword"), shape.inputColumns().stream().map(ArrowBatchSourceFactory.InputColumn::name).toList());
        assertEquals(ArrowBatchSourceFactory.ColumnKind.KEYWORD, shape.inputColumns().getFirst().kind());
    }

    public void testTimestampProjectionUsesTimestampColumnKind() {
        RelDataType timestamp = nullable(SqlTypeName.TIMESTAMP);
        RelNode scan = scan(typeFactory.builder().add("event_time", timestamp).build(), List.of(storage("event_time", FieldType.DATE)));
        RelNode project = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("event_time"));

        LuceneFragmentConvertor.ArrowSourceShape shape = LuceneFragmentConvertor.extractArrowSourceShape(project);

        assertNotNull(shape);
        assertEquals(ArrowBatchSourceFactory.ColumnKind.TIMESTAMP, shape.inputColumns().getFirst().kind());
    }

    public void testUnsupportedInputTypeDoesNotCreateSourcePlan() {
        RelDataType integer = nullable(SqlTypeName.INTEGER);
        RelNode scan = scan(typeFactory.builder().add("value", integer).build(), List.of(storage("value", FieldType.INTEGER)));
        RelNode project = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("value"));

        assertNull(LuceneFragmentConvertor.extractArrowSourceShape(project));
    }

    public void testAttachedOperatorUpdatesCompiledPlanAndOutputNames() throws Exception {
        RelDataType bigint = nullable(SqlTypeName.BIGINT);
        RelNode scan = scan(typeFactory.builder().add("metric", bigint).build(), List.of(storage("metric", FieldType.LONG)));
        AggregateCall sum = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(0), -1, scan, bigint, "sum_metric");
        RelNode aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sum));
        RelNode project = LogicalProject.create(aggregate, List.of(), List.of(rexBuilder.makeInputRef(aggregate, 0)), List.of("renamed"));
        RecordingExecutor executor = new RecordingExecutor();
        ArrowBatchSourceExecutorHolder.install(executor);
        try {
            LuceneFragmentConvertor convertor = new LuceneFragmentConvertor(Map.of());
            byte[] inner = convertor.convertFragment(aggregate);
            byte[] attached = convertor.attachFragmentOnTop(project, inner);

            assertNotNull(executor.attachedFragment);
            try (StreamInput input = StreamInput.wrap(attached)) {
                List<String> metadata = input.readStringList();
                assertArrayEquals(new byte[] { 4, 5, 6 }, java.util.Base64.getDecoder().decode(metadata.get(1)));
                assertEquals("1", metadata.get(6));
                assertEquals("renamed", metadata.get(7));
                assertFalse(input.readBoolean());
            }
        } finally {
            ArrowBatchSourceExecutorHolder.remove(executor);
        }
    }

    public void testPartialAggregateCompilesAndSerializesArrowSourcePlan() throws Exception {
        RelDataType bigint = nullable(SqlTypeName.BIGINT);
        RelNode scan = scan(typeFactory.builder().add("metric", bigint).build(), List.of(storage("metric", FieldType.LONG)));
        AggregateCall sum = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(0), -1, scan, bigint, "sum_metric");
        RelNode aggregate = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sum));
        RecordingExecutor executor = new RecordingExecutor();
        ArrowBatchSourceExecutorHolder.install(executor);
        try {
            LuceneFragmentConvertor convertor = new LuceneFragmentConvertor(Map.of());
            byte[] inner = convertor.convertFragment(scan);
            byte[] bytes = convertor.attachPartialAggOnTop(aggregate, inner);

            assertTrue(executor.partialAggregate);
            assertNotNull(executor.compiledFragment);
            try (StreamInput input = StreamInput.wrap(bytes)) {
                List<String> metadata = input.readStringList();
                assertEquals(LuceneFragmentConvertor.ARROW_SOURCE_PLAN_MARKER, metadata.getFirst());
                assertEquals("input-0", metadata.get(2));
                assertEquals("1", metadata.get(3));
                assertEquals("metric", metadata.get(4));
                assertEquals("LONG", metadata.get(5));
                assertFalse(input.readBoolean());
            }
        } finally {
            ArrowBatchSourceExecutorHolder.remove(executor);
        }
    }

    private RelNode scan(RelDataType rowType, List<FieldStorageInfo> storage) {
        return new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType, List.of("lucene"), storage);
    }

    private FieldStorageInfo storage(String name, FieldType type) {
        return new FieldStorageInfo(
            name,
            type.name().toLowerCase(java.util.Locale.ROOT),
            type,
            List.of("lucene"),
            List.of(),
            List.of(),
            false
        );
    }

    private RelDataType nullable(SqlTypeName type) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(type), true);
    }

    private static final class RecordingExecutor implements ArrowBatchSourceExecutor {
        private RelNode compiledFragment;
        private RelNode attachedFragment;
        private boolean partialAggregate;

        @Override
        public byte[] compile(RelNode fragment, boolean partial) {
            compiledFragment = fragment;
            partialAggregate = partial;
            return new byte[] { 1, 2, 3 };
        }

        @Override
        public byte[] attachFragment(RelNode fragment, byte[] innerPlanBytes) {
            attachedFragment = fragment;
            assertArrayEquals(new byte[] { 1, 2, 3 }, innerPlanBytes);
            return new byte[] { 4, 5, 6 };
        }

        @Override
        public EngineResultStream execute(
            BufferAllocator resultAllocator,
            ArrowBatchSourcePlan plan,
            ArrowBatchSourceFactory sourceFactory,
            Task task,
            DelegationThreadTracker threadTracker
        ) {
            throw new UnsupportedOperationException();
        }
    }
}
