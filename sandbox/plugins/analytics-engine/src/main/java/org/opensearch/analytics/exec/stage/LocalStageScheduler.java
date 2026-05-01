/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;

import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages —
 * those that run at the coordinator with a backend-provided {@link ExchangeSink}.
 * Creates the sink via {@link Stage#getExchangeSinkProvider()} using an
 * {@link ExchangeSinkContext} carrying the plan bytes, allocator, input
 * schema (derived from the single child stage), and downstream sink. Hands
 * the resulting sink to {@link LocalStageExecution}.
 *
 * <p>Single-sink simplification: assumes exactly one child stage. Multi-child
 * (joins, set ops) will require per-child sink routing in a follow-up.
 *
 * @opensearch.internal
 */
final class LocalStageScheduler implements StageScheduler {

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            chosenBytes(stage),
            config.bufferAllocator(),
            deriveInputSchema(stage),
            sink
        );
        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create exchange sink for stageId=" + stage.getStageId(), e);
        }
        return new LocalStageExecution(stage, backendSink, sink);
    }

    /** Picks the plan-alternative bytes bound to the stage's exchange sink provider. */
    private static byte[] chosenBytes(Stage stage) {
        assert stage.getPlanAlternatives().size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one plan alternative, got "
            + stage.getPlanAlternatives().size();
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }

    /**
     * Derives the backend's input Arrow schema from the single child stage's
     * fragment rowtype. Multi-child support (joins, set ops with heterogeneous
     * inputs) is deferred.
     */
    private static Schema deriveInputSchema(Stage stage) {
        List<Stage> children = stage.getChildStages();
        assert children.size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one child stage, got "
            + children.size();
        Stage child = children.getFirst();
        RelNode childFragment = child.getPlanAlternatives().isEmpty()
            ? child.getFragment()
            : child.getPlanAlternatives().getFirst().resolvedFragment();

        // Find the aggregate in the child fragment to check for functions with
        // non-trivial intermediate state (e.g. approx_count_distinct emits binary).
        Aggregate agg = findAggregate(childFragment);
        if (agg == null) {
            return ArrowSchemaFromCalcite.arrowSchemaFromRowType(childFragment.getRowType());
        }

        // Build the Arrow schema, overriding types for functions with intermediate state.
        List<org.apache.arrow.vector.types.pojo.Field> fields = new java.util.ArrayList<>();
        int groupCount = agg.getGroupSet().cardinality();
        // Group-by columns use the Calcite type
        for (int i = 0; i < groupCount; i++) {
            org.apache.calcite.rel.type.RelDataTypeField f = childFragment.getRowType().getFieldList().get(i);
            fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
        }
        // Aggregate output columns: check for intermediate-state functions
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall call = agg.getAggCallList().get(i);
            org.apache.calcite.rel.type.RelDataTypeField f = childFragment.getRowType().getFieldList().get(groupCount + i);
            if (call.getAggregation().getKind() == SqlKind.COUNT && call.isApproximate()) {
                // approx_count_distinct partial emits binary (HLL sketch)
                fields.add(new org.apache.arrow.vector.types.pojo.Field(
                    f.getName(),
                    new org.apache.arrow.vector.types.pojo.FieldType(true, org.apache.arrow.vector.types.pojo.ArrowType.Binary.INSTANCE, null),
                    null
                ));
            } else {
                fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
            }
        }
        return new Schema(fields);
    }

    private static Aggregate findAggregate(RelNode node) {
        if (node instanceof Aggregate agg) return agg;
        for (RelNode input : node.getInputs()) {
            Aggregate found = findAggregate(input);
            if (found != null) return found;
        }
        return null;
    }
}
