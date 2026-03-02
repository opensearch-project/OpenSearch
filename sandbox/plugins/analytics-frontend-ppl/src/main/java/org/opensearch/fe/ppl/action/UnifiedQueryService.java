/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.cluster.ClusterState;
import org.opensearch.fe.ppl.OpenSearchSchemaBuilder;
import org.opensearch.fe.ppl.compiler.OpenSearchQueryCompiler;
import org.opensearch.fe.ppl.planner.PushDownPlanner;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Core orchestrator that ties together PushDownPlanner
 * and OpenSearchQueryCompiler into a single execution pipeline.
 *
 * <p>Pipeline: PPL text → RelNode → push-down optimization → compile → execute → response.
 */
public class UnifiedQueryService {

    private static final String DEFAULT_CATALOG = "opensearch";

    private final PushDownPlanner pushDownPlanner;

    public UnifiedQueryService(PushDownPlanner pushDownPlanner) {
        this.pushDownPlanner = pushDownPlanner;
    }

    /**
     * Executes a PPL query through the full pipeline.
     *
     * @param pplText      the PPL query text
     * @param clusterState the current cluster state for schema resolution
     * @return a UnifiedPPLResponse containing column names and result rows
     */
    public UnifiedPPLResponse execute(String pplText, ClusterState clusterState) {
        // Step 1: Convert PPL to RelNode
        SchemaPlus schemaPlus = OpenSearchSchemaBuilder.buildSchema(clusterState);

        UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(DEFAULT_CATALOG, schemaPlus)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();

        try {
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(pplText);
            RelNode mixedPlan = pushDownPlanner.plan(logicalPlan);

            // Step 2: Push-down optimization

            // Step 3: Build context and compile
            PreparedStatement statement = compileAndPrepare(context, mixedPlan);
            try (statement) {
                ResultSet rs = statement.executeQuery();

                // Step 4: Extract column metadata
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<String> columns = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(metaData.getColumnName(i));
                }

                // Step 5: Extract rows
                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 1; i <= columnCount; i++) {
                        row[i - 1] = rs.getObject(i);
                    }
                    rows.add(row);
                }

                return new UnifiedPPLResponse(columns, rows);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to execute PPL query: " + e.getMessage(), e);
        } finally {
            try {
                context.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }

    /**
     * Compiles the mixed plan into a PreparedStatement. Protected for testability.
     */
    protected PreparedStatement compileAndPrepare(UnifiedQueryContext context, RelNode mixedPlan) throws Exception {
        OpenSearchQueryCompiler compiler = new OpenSearchQueryCompiler(context);
        return compiler.compile(mixedPlan);
    }
}
