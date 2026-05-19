/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Reusable test harness that turns a query into a {@code RelNode} and hands it to a
 * {@link QueryPlanExecutor} for execution. Two entry points:
 * <ul>
 *   <li>{@link #executeSql(String)} — parse + validate + execute, for shapes that the
 *       Calcite SQL parser produces cleanly.</li>
 *   <li>{@link #executeRel(RelNode)} with {@link #relBuilder()} — execute a pre-built
 *       {@code RelNode}. Use this when the SQL parser's lowering of a feature differs
 *       from what the engine wants (e.g. {@code SUM(...) OVER ()} wrapped in a CASE for
 *       SQL NULL-on-empty semantics).</li>
 * </ul>
 *
 * <p>Schema is built fresh on each call from {@link ClusterService}'s current state via
 * {@link OpenSearchSchemaBuilder}. Operator table is the Calcite standard table.
 *
 * <p>Synchronous by design — execute methods block on a {@link PlainActionFuture} so tests
 * can assert on results directly. The executor forks to its own SEARCH executor internally,
 * so blocking the calling thread is safe.
 *
 * @opensearch.internal
 */
public final class SqlPlanRunner {

    private final ClusterService clusterService;
    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

    public SqlPlanRunner(ClusterService clusterService, QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
    }

    /**
     * Parses, validates, and converts {@code sql} to a {@code RelNode}, then submits it to
     * the executor and returns the materialised rows.
     */
    public java.util.List<Object[]> executeSql(String sql) {
        return executeRel(parseToRel(sql));
    }

    /** Executes a pre-built {@code RelNode} and returns the materialised rows. Useful when
     *  the SQL parser's lowering of a feature (e.g. {@code SUM(...) OVER ()} with its
     *  CASE-wrap for SQL NULL-on-empty semantics) differs from the shape the engine wants —
     *  tests can construct the {@code RelNode} directly via {@link #relBuilder()}. */
    public java.util.List<Object[]> executeRel(RelNode plan) {
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        planExecutor.execute(plan, null, future);
        Iterable<Object[]> rows = future.actionGet();
        java.util.List<Object[]> out = new ArrayList<>();
        for (Object[] row : rows) {
            out.add(row);
        }
        return out;
    }

    /** Returns a Calcite {@link RelBuilder} backed by the current cluster schema, for tests
     *  that construct RelNodes programmatically rather than parsing SQL. */
    public RelBuilder relBuilder() {
        return RelBuilder.create(frameworkConfig());
    }

    private FrameworkConfig frameworkConfig() {
        SchemaPlus rootSchema = OpenSearchSchemaBuilder.buildSchema(clusterService.state());
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        properties.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), "UNCHANGED");
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        Context plannerContext = Contexts.of(connectionConfig);
        return Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.config().withCaseSensitive(false))
            .defaultSchema(rootSchema)
            .operatorTable(SqlStdOperatorTable.instance())
            .context(plannerContext)
            .build();
    }

    /** Parses + validates + converts {@code sql} into a {@code RelNode} without executing it. */
    public RelNode parseToRel(String sql) {
        Planner planner = Frameworks.getPlanner(frameworkConfig());
        try {
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);
            RelRoot root = planner.rel(validated);
            return root.project();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to plan SQL: [" + sql + "]", e);
        } finally {
            planner.close();
        }
    }
}
