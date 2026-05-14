/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Core orchestrator: PPL text → RelNode → QueryPlanExecutor → PPLResponse.
 *
 * <p>Passes the logical RelNode directly to the back-end engine (e.g. DataFusion)
 * which handles optimization and execution natively via Substrait. No Janino
 * code generation needed.
 */
public class UnifiedQueryService {

    private static final Logger logger = LogManager.getLogger(UnifiedQueryService.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;
    private final EngineContext engineContext;

    public UnifiedQueryService(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor, EngineContext engineContext) {
        this.planExecutor = planExecutor;
        this.engineContext = engineContext;
    }

    /**
     * Executes a PPL query through the simplified pipeline:
     * PPL text → RelNode → planExecutor.execute() → PPLResponse.
     */
    public PPLResponse execute(String pplText) {
        // Extract tables from the SchemaPlus into a plain AbstractSchema.
        // SchemaPlus wraps CalciteSchema — passing it to catalog() causes double-nesting
        // where tables become inaccessible. A plain Schema avoids this.
        SchemaPlus schemaPlus = engineContext.getSchema();
        Map<String, Table> tableMap = new HashMap<>();
        for (String tableName : schemaPlus.getTableNames()) {
            tableMap.put(tableName, schemaPlus.getTable(tableName));
        }
        AbstractSchema flatSchema = new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        };

        logger.info(
            "[UnifiedQueryService] schemaPlus class: {}, tableNames: {}, tableMap: {}, engineContext class: {}",
            schemaPlus.getClass().getName(),
            schemaPlus.getTableNames(),
            tableMap.keySet(),
            engineContext.getClass().getName()
        );

        try (
            UnifiedQueryContext context = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog(DEFAULT_CATALOG, flatSchema)
                .defaultNamespace(DEFAULT_CATALOG)
                // The unified PPL parser reuses the v2 AstBuilder, which gates Calcite-only
                // commands (table, regex, rex, convert) on plugins.calcite.enabled. The unified
                // path is by definition Calcite-based — flag it on so those commands lower
                // through the same Project/Filter RelNodes as their non-aliased counterparts.
                .setting("plugins.calcite.enabled", true)
                .build()
        ) {

            // Log what the context's root schema looks like
            logger.info("[UnifiedQueryService] Context built, planning PPL: {}", pplText);
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(pplText);

            // Execute directly via the back-end engine — no Janino compilation needed.
            // The executor API is async; this test frontend keeps a sync surface, so we bridge
            // via PlainActionFuture. The block happens off the transport thread (the executor
            // forks to SEARCH internally), so this is safe for test/IT use.
            PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
            planExecutor.execute(logicalPlan, null, future);
            Iterable<Object[]> results = future.actionGet();

            // Extract column names from the RelNode's row type
            List<RelDataTypeField> fields = logicalPlan.getRowType().getFieldList();
            List<String> columns = new ArrayList<>(fields.size());
            for (RelDataTypeField field : fields) {
                columns.add(field.getName());
            }

            // Collect result rows
            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : results) {
                rows.add(row);
            }

            return new PPLResponse(columns, rows);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to execute PPL query: " + e.getMessage(), e);
        }
    }

    /**
     * Executes a PPL query with profiling enabled. Calls
     * {@link DefaultPlanExecutor#executeWithProfile} which runs the full query and
     * captures per-stage timing from the coordinator's perspective.
     */
    public PPLResponse executeWithProfile(String pplText) {
        if ((planExecutor instanceof DefaultPlanExecutor) == false) {
            throw new UnsupportedOperationException(
                "executeWithProfile requires DefaultPlanExecutor, got " + planExecutor.getClass().getSimpleName()
            );
        }
        DefaultPlanExecutor executor = (DefaultPlanExecutor) planExecutor;
        RelNode logicalPlan = buildLogicalPlan(pplText);

        PlainActionFuture<ProfiledResult> future = new PlainActionFuture<>();
        executor.executeWithProfile(logicalPlan, null, future);
        ProfiledResult result = future.actionGet();

        if (result.isSuccess() == false) {
            Throwable failure = result.failure();
            if (failure instanceof RuntimeException re) throw re;
            throw new RuntimeException("Query failed: " + failure.getMessage(), failure);
        }

        QueryProfile profile = result.profile();
        logger.info("[UnifiedQueryService] Profile result:\n  query_id={}\n  total_elapsed_ms={}\n  stages={}",
            profile.queryId(), profile.totalElapsedMs(), profile.stages().size());
        for (var stage : profile.stages()) {
            logger.info("  Stage {} [{}] state={} elapsed={}ms rows={} tasks={}",
                stage.stageId(), stage.executionType(), stage.state(),
                stage.elapsedMs(), stage.rowsProcessed(), stage.tasks().size());
        }
        if (profile.fullPlan() != null && profile.fullPlan().isEmpty() == false) {
            logger.info("[UnifiedQueryService] Full plan:\n{}", String.join("\n", profile.fullPlan()));
        }

        // Build response with columns + rows + profile
        List<RelDataTypeField> fields = logicalPlan.getRowType().getFieldList();
        List<String> columns = new ArrayList<>(fields.size());
        for (RelDataTypeField field : fields) {
            columns.add(field.getName());
        }
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : result.rows()) {
            rows.add(row);
        }

        return new PPLResponse(columns, rows, profile);
    }

    private RelNode buildLogicalPlan(String pplText) {
        SchemaPlus schemaPlus = engineContext.getSchema();
        Map<String, Table> tableMap = new HashMap<>();
        for (String tableName : schemaPlus.getTableNames()) {
            tableMap.put(tableName, schemaPlus.getTable(tableName));
        }
        AbstractSchema flatSchema = new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        };

        try (
            UnifiedQueryContext context = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog(DEFAULT_CATALOG, flatSchema)
                .defaultNamespace(DEFAULT_CATALOG)
                .setting("plugins.calcite.enabled", true)
                .build()
        ) {
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            return planner.plan(pplText);
        } catch (Exception e) {
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException("Failed to build logical plan", e);
        }
    }
}
