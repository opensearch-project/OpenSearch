/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.FullTextFunctions;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.POINT_RANGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.STORED_FIELDS;

/**
 * Tests for multiple plan generation via {@link PlanForker}.
 * Verifies plan count, backend grouping, and annotation narrowing.
 */
public class MultiplePlanGenerationTests extends BasePlannerRulesTests {

    /**
     * 5 predicates each viable on [DF, Lucene]. Should produce 2 plans
     * (all-DF group, all-Lucene group) — NOT 2^5 = 32.
     */
    public void testFivePredicatesGroupedNotCartesian() {
        // 5 integer fields, each with an equality predicate
        Map<String, Map<String, Object>> fields = Map.of(
            "f0", Map.of("type", "integer"),
            "f1", Map.of("type", "integer"),
            "f2", Map.of("type", "integer"),
            "f3", Map.of("type", "integer"),
            "f4", Map.of("type", "integer")
        );

        String[] fieldNames = { "f0", "f1", "f2", "f3", "f4" };
        SqlTypeName[] fieldTypes = {
            SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER,
            SqlTypeName.INTEGER, SqlTypeName.INTEGER
        };

        RexNode condition = makeAnd(
            makeEquals(0, SqlTypeName.INTEGER, 1),
            makeEquals(1, SqlTypeName.INTEGER, 2),
            makeEquals(2, SqlTypeName.INTEGER, 3),
            makeEquals(3, SqlTypeName.INTEGER, 4),
            makeEquals(4, SqlTypeName.INTEGER, 5)
        );

        Stage dataStage = buildAndFork(fields, fieldNames, fieldTypes, condition);

        List<StagePlan> plans = dataStage.getPlanAlternatives();
        logger.info("Plan count: {}", plans.size());
        for (int idx = 0; idx < plans.size(); idx++) {
            logger.info("Plan [{}]:\n{}", idx, RelOptUtil.toString(plans.get(idx).resolvedFragment()));
        }

        // Grouped: one plan per distinct backend across annotations = 2 (DF, Lucene)
        // NOT 2^5 = 32
        assertEquals("Expected 2 grouped plans, not cartesian product", 2, plans.size());

        // Verify each plan has all predicates narrowed to the same backend
        for (StagePlan plan : plans) {
            OpenSearchFilter filter = findFilter(plan.resolvedFragment());
            assertNotNull("Plan should contain a filter", filter);
            List<String> predicateBackends = filter.getAnnotations().stream()
                .map(annotation -> annotation.getViableBackends().getFirst())
                .distinct()
                .toList();
            assertEquals("All predicates in a plan should target the same backend", 1, predicateBackends.size());
        }
    }

    /**
     * Mixed predicates: 3 viable on [DF, Lucene], 2 viable on [Lucene] only (full-text).
     * With DF as primary (delegation enabled), should produce 2 plans:
     * - all-Lucene group (all 5 → Lucene)
     * - mixed group: 3 integers → DF (native), 2 full-text → Lucene (only viable target)
     */
    public void testMixedPredicatesGrouped() {
        Map<String, Map<String, Object>> fields = Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer"),
            "count", Map.of("type", "integer"),
            "message", Map.of("type", "keyword"),
            "body", Map.of("type", "keyword")
        );

        String[] fieldNames = { "status", "size", "count", "message", "body" };
        SqlTypeName[] fieldTypes = {
            SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER,
            SqlTypeName.VARCHAR, SqlTypeName.VARCHAR
        };

        // 3 integer equals (both backends) + 2 full-text MATCH (Lucene only)
        RexNode condition = makeAnd(
            makeEquals(0, SqlTypeName.INTEGER, 200),
            makeEquals(1, SqlTypeName.INTEGER, 100),
            makeEquals(2, SqlTypeName.INTEGER, 50),
            makeFullTextCall(org.opensearch.analytics.planner.rel.FullTextFunctions.MATCH, 3, "hello"),
            makeFullTextCall(org.opensearch.analytics.planner.rel.FullTextFunctions.MATCH, 4, "world")
        );

        Stage dataStage = buildAndForkWithDelegation(fields, fieldNames, fieldTypes, condition);

        List<StagePlan> plans = dataStage.getPlanAlternatives();
        logger.info("Plan count: {}", plans.size());
        for (int idx = 0; idx < plans.size(); idx++) {
            logger.info("Plan [{}]:\n{}", idx, RelOptUtil.toString(plans.get(idx).resolvedFragment()));
        }

        // 2 distinct backends across all annotations: Lucene and DF
        // Lucene group: all 5 → Lucene
        // DF group: 3 integers → DF (native), 2 full-text → Lucene (only option, fallback)
        assertEquals("Expected 2 grouped plans", 2, plans.size());
    }

    /**
     * Scan viable on [DF, Lucene]. Filter with 7 predicates: 5 integer equals
     * viable on both, 2 full-text MATCH viable on Lucene only. Aggregate with
     * 5 SUM calls viable on both. DF delegates filter to Lucene.
     * Should produce bounded plans, not combinatorial explosion.
     */
    public void testScanFilterAggregateWithDelegation() {
        Map<String, Map<String, Object>> fields = Map.ofEntries(
            Map.entry("f0", Map.of("type", "integer")),
            Map.entry("f1", Map.of("type", "integer")),
            Map.entry("f2", Map.of("type", "integer")),
            Map.entry("f3", Map.of("type", "integer")),
            Map.entry("f4", Map.of("type", "integer")),
            Map.entry("msg0", Map.of("type", "keyword")),
            Map.entry("msg1", Map.of("type", "keyword")),
            Map.entry("v0", Map.of("type", "integer")),
            Map.entry("v1", Map.of("type", "integer")),
            Map.entry("v2", Map.of("type", "integer")),
            Map.entry("v3", Map.of("type", "integer")),
            Map.entry("v4", Map.of("type", "integer"))
        );

        String[] fieldNames = { "f0", "f1", "f2", "f3", "f4", "msg0", "msg1",
            "v0", "v1", "v2", "v3", "v4" };
        SqlTypeName[] fieldTypes = {
            SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER,
            SqlTypeName.INTEGER, SqlTypeName.INTEGER,
            SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
            SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.INTEGER,
            SqlTypeName.INTEGER, SqlTypeName.INTEGER
        };

        // 5 integer equals (both backends) + 2 full-text MATCH (Lucene only)
        RexNode condition = makeAnd(
            makeEquals(0, SqlTypeName.INTEGER, 1),
            makeEquals(1, SqlTypeName.INTEGER, 2),
            makeEquals(2, SqlTypeName.INTEGER, 3),
            makeEquals(3, SqlTypeName.INTEGER, 4),
            makeEquals(4, SqlTypeName.INTEGER, 5),
            makeFullTextCall(FullTextFunctions.MATCH, 5, "hello"),
            makeFullTextCall(FullTextFunctions.MATCH_PHRASE, 6, "world")
        );

        // 5 SUM aggregate calls on v0..v4 (both backends viable)
        PlannerContext context = buildContext("parquet", 1, fields, hybridDelegationBackends());
        RelOptTable table = mockTable("test_index", fieldNames, fieldTypes);
        RelNode scan = stubScan(table);
        RelNode filter = LogicalFilter.create(scan, condition);

        List<AggregateCall> aggCalls = new ArrayList<>();
        for (int idx = 0; idx < 5; idx++) {
            aggCalls.add(AggregateCall.create(
                SqlStdOperatorTable.SUM, false, false, false, List.of(),
                List.of(7 + idx), -1, null, RelCollations.EMPTY,
                typeFactory.createSqlType(SqlTypeName.INTEGER), "sum_v" + idx
            ));
        }
        RelNode aggregate = LogicalAggregate.create(
            filter, List.of(), ImmutableBitSet.of(0), null, aggCalls
        );

        RelNode cboOutput = runPlanner(aggregate, context);
        logger.info("CBO output:\n{}", RelOptUtil.toString(cboOutput));

        QueryDAG dag = DAGBuilder.build(cboOutput);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        logger.info("DAG after forking:\n{}", dag);

        // Single shard → one stage
        Stage stage = dag.rootStage();
        List<StagePlan> plans = stage.getPlanAlternatives();
        logger.info("Plan count: {}", plans.size());
        for (int idx = 0; idx < plans.size(); idx++) {
            logger.info("Plan [{}]:\n{}", idx, RelOptUtil.toString(plans.get(idx).resolvedFragment()));
        }

        // Without grouping: scan(2) × filter(2^7) × aggregate(2^5) = 8192 plans
        // With grouping + arrow compatibility + pipeline inheritance = 6 plans:
        //
        // Plan | Scan   | Filter(op) | Preds 0-4 (int) | Preds 5-6 (FT) | Agg
        // -----|--------|------------|-----------------|----------------|-----
        //   0  | DF     | DF         | Lucene          | Lucene         | DF
        //   1  | DF     | DF         | DF              | Lucene         | DF
        //   2  | Lucene | DF*        | Lucene          | Lucene         | DF
        //   3  | Lucene | DF*        | DF              | Lucene         | DF
        //   4  | Lucene | Lucene     | Lucene          | Lucene         | DF
        //   5  | Lucene | Lucene     | DF              | Lucene         | DF
        //
        // *DF is arrow-compatible for FILTER so it can operate on Lucene's scan output.
        // Aggregate is always DF — only viable backend. Inherits pipeline, no branching.
        assertEquals("Expected 6 plans", 6, plans.size());
    }

    // ---- Helpers ----

    private Stage buildAndFork(Map<String, Map<String, Object>> fields,
                               String[] fieldNames, SqlTypeName[] fieldTypes,
                               RexNode condition) {
        return buildAndFork(fields, fieldNames, fieldTypes, condition, List.of(DATAFUSION, LUCENE));
    }

    private Stage buildAndForkWithDelegation(Map<String, Map<String, Object>> fields,
                                             String[] fieldNames, SqlTypeName[] fieldTypes,
                                             RexNode condition) {
        return buildAndFork(fields, fieldNames, fieldTypes, condition, delegationBackends());
    }

    private List<AnalyticsSearchBackendPlugin> delegationBackends() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        MockLuceneBackend luceneWithDelegation = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        return List.of(dfWithDelegation, luceneWithDelegation);
    }

    /** Backends where both can scan (Lucene has COLUMNAR_STORAGE) and filter delegation is enabled. */
    private List<AnalyticsSearchBackendPlugin> hybridDelegationBackends() {
        MockDataFusionBackend dfBackend = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        MockLuceneBackend luceneBackend = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public List<DataFormat> getSupportedFormats() {
                return List.of(new DataFormat() {
                    @Override public String name() { return LUCENE_DATA_FORMAT; }
                    @Override public long priority() { return 0; }
                    @Override public Set<FieldTypeCapabilities> supportedFields() {
                        return Set.of(
                            new FieldTypeCapabilities("integer", Set.of(COLUMNAR_STORAGE, POINT_RANGE, STORED_FIELDS)),
                            new FieldTypeCapabilities("long", Set.of(COLUMNAR_STORAGE, POINT_RANGE, STORED_FIELDS)),
                            new FieldTypeCapabilities("keyword", Set.of(COLUMNAR_STORAGE, FULL_TEXT_SEARCH, STORED_FIELDS)),
                            new FieldTypeCapabilities("text", Set.of(COLUMNAR_STORAGE, FULL_TEXT_SEARCH, STORED_FIELDS)),
                            new FieldTypeCapabilities("boolean", Set.of(COLUMNAR_STORAGE, STORED_FIELDS)),
                            new FieldTypeCapabilities("date", Set.of(COLUMNAR_STORAGE, POINT_RANGE, STORED_FIELDS))
                        );
                    }
                });
            }
        };
        return List.of(dfBackend, luceneBackend);
    }

    private Stage buildAndFork(Map<String, Map<String, Object>> fields,
                               String[] fieldNames, SqlTypeName[] fieldTypes,
                               RexNode condition, List<AnalyticsSearchBackendPlugin> backends) {
        PlannerContext context = buildContext("parquet", fields, backends);
        RelOptTable table = mockTable("test_index", fieldNames, fieldTypes);
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        RelNode cboOutput = runPlanner(filter, context);
        logger.info("CBO output:\n{}", RelOptUtil.toString(cboOutput));

        QueryDAG dag = DAGBuilder.build(cboOutput);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        logger.info("DAG after forking:\n{}", dag);

        // Return the data node stage (child of root)
        assertFalse("Expected child stages", dag.rootStage().getChildStages().isEmpty());
        return dag.rootStage().getChildStages().getFirst();
    }

    private OpenSearchFilter findFilter(RelNode node) {
        if (node instanceof OpenSearchFilter filter) {
            return filter;
        }
        for (RelNode input : node.getInputs()) {
            OpenSearchFilter found = findFilter(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
