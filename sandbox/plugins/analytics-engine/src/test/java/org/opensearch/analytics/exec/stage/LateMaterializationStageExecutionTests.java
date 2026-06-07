/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LateMaterializationStageExecution}.
 *
 * <p>Focused on the K=0 short-circuit path: when the child Sort+Limit reduce produces zero
 * rows, the LM stage must NOT close its parent's input sink. Closing it pre-emptively races
 * the parent {@code ReduceStageExecution}'s reduce() task — once the JVM thread pools warm
 * up, the reduce task runs after the close and the backend sink throws
 * {@code IllegalStateException("sink closed before reduce")}, which the IT surfaces as
 * HTTP 500 / {@code NoSuchElementException} on the wire.
 */
public class LateMaterializationStageExecutionTests extends OpenSearchTestCase {

    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * Empty-K (zero rows from child) must NOT close {@code parentSink}. The parent stage
     * (e.g. {@code ReduceStageExecution}) owns the sink's lifecycle via its own
     * {@code onTerminalTransition}; closing it from the LM stage races the parent's
     * {@code reduce()} task and produces "sink closed before reduce" on warm runtimes.
     */
    public void testKZero_doesNotCloseParentSink() {
        CapturingSink parentSink = new CapturingSink();
        LateMaterializationStageExecution exec = newLmStage(parentSink);

        scheduleAndDispatch(exec);

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertFalse("LM stage must not close parentSink on K=0; parent stage owns close via onTerminalTransition", parentSink.closed);
    }

    // ── Fixture builders ─────────────────────────────────────────────────────

    /**
     * Construct a Stage whose fragment is exactly an OpenSearchLateMaterialization wrapping
     * an empty LogicalValues source. Simulates the LM stage's own fragment with no child
     * input batches yet supplied — drainAndGroupByUgsi will see total=0 and the K=0 path
     * fires.
     */
    private LateMaterializationStageExecution newLmStage(ExchangeSink parentSink) {
        RelDataType rowType = cluster.getTypeFactory()
            .builder()
            .add(OpenSearchLateMaterialization.ROW_ID_FIELD, cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add(OpenSearchLateMaterialization.UGSI_FIELD, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER))
            .build();
        Values emptyInput = (Values) LogicalValues.createEmpty(cluster, rowType);

        // Wrapper above-anchor fields = a single string column. The K=0 branch never
        // dereferences them, so a minimal list is enough.
        List<RelDataTypeField> aboveFields = List.of(rowType.getFieldList().get(0));
        List<FieldStorageInfo> aboveStorage = List.of(
            FieldStorageInfo.derivedColumn(OpenSearchLateMaterialization.ROW_ID_FIELD, SqlTypeName.BIGINT)
        );

        OpenSearchLateMaterialization wrapper = new OpenSearchLateMaterialization(
            cluster,
            RelTraitSet.createEmpty(),
            emptyInput,
            aboveFields,
            aboveStorage,
            List.of("datafusion")
        );

        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(2);
        when(stage.getFragment()).thenReturn((RelNode) wrapper);

        QueryContext config = mock(QueryContext.class);
        when(config.queryId()).thenReturn("test-query");
        when(config.operationListeners()).thenReturn(List.of());
        when(config.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));
        when(config.localTaskExecutor()).thenReturn(inlineExecutor());

        return new LateMaterializationStageExecution(
            stage,
            config,
            parentSink,
            mock(ClusterService.class),
            mock(AnalyticsSearchTransportService.class),
            /* shardStageId */ 0,
            /* fetchBackendId */ "datafusion"
        );
    }

    private static ExecutorService inlineExecutor() {
        return mock(ExecutorService.class, invocation -> {
            if ("execute".equals(invocation.getMethod().getName())) {
                ((Runnable) invocation.getArgument(0)).run();
                return null;
            }
            return null;
        });
    }

    private static void scheduleAndDispatch(LateMaterializationStageExecution exec) {
        exec.start();
        @SuppressWarnings("unchecked")
        org.opensearch.analytics.exec.task.TaskRunner<StageTask> dispatcher = (org.opensearch.analytics.exec.task.TaskRunner<
            StageTask>) exec.taskRunner();
        if (dispatcher == null) return;
        for (StageTask task : exec.tasks()) {
            task.transitionTo(StageTaskState.RUNNING);
            dispatcher.run(task, new org.opensearch.core.action.ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    task.transitionTo(StageTaskState.FINISHED);
                    exec.onTaskTerminal(task, null);
                }

                @Override
                public void onFailure(Exception cause) {
                    task.transitionTo(StageTaskState.FAILED);
                    exec.onTaskTerminal(task, cause);
                }
            });
        }
    }

    /** Bare ExchangeSink that records whether it has been closed. */
    private static final class CapturingSink implements ExchangeSink {
        boolean closed = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
