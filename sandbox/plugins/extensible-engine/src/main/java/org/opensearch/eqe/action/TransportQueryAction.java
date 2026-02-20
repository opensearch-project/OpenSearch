/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.eqe.calcite.QueryPlanner;
import org.opensearch.eqe.coordinator.DefaultQueryCoordinator;
import org.opensearch.eqe.coordinator.QueryCoordinator;
import org.opensearch.eqe.engine.EngineContext;
import org.opensearch.tasks.Task;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Coordinator-level transport action for query execution.
 *
 * <p>Accepts {@link QueryRequest} containing a serialized RelNode JSON plan,
 * resolves it via {@link QueryPlanner} into a Calcite {@code RelNode}, then
 * invokes {@link QueryCoordinator#execute} to fan out to data nodes and return
 * aggregated {@link QueryResponse}.
 */
public class TransportQueryAction
        extends HandledTransportAction<QueryRequest, QueryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportQueryAction.class);

    private final QueryPlanner queryPlanner;
    private final QueryCoordinator coordinator;

    @Inject
    public TransportQueryAction(
            TransportService transportService,
            StreamTransportService streamTransportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            EngineContext engineContext) {
        super(QueryAction.NAME, transportService, actionFilters, QueryRequest::new);
        this.queryPlanner = new QueryPlanner(clusterService);
        this.coordinator = new DefaultQueryCoordinator(
            streamTransportService, clusterService, engineContext.getAllocator());
    }

    @Override
    protected void doExecute(Task task, QueryRequest request,
                             ActionListener<QueryResponse> listener) {
        try {
            RelNode logicalPlan = queryPlanner.resolve(request.getJsonPlan());

            coordinator.execute(logicalPlan, task)
                .whenComplete((root, error) -> {
                    if (error != null) {
                        logger.error("Query execution failed", error);
                        listener.onFailure(error instanceof Exception
                            ? (Exception) error : new RuntimeException(error));
                    } else {
                        try {
                            listener.onResponse(convertToResponse(root));
                        } finally {
                            root.close();
                        }
                    }
                });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private QueryResponse convertToResponse(VectorSchemaRoot root) {
        List<String> columns = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            columns.add(vector.getName());
        }

        List<Object[]> rows = new ArrayList<>();
        int rowCount = root.getRowCount();
        int columnCount = root.getFieldVectors().size();
        for (int r = 0; r < rowCount; r++) {
            Object[] row = new Object[columnCount];
            for (int c = 0; c < columnCount; c++) {
                FieldVector vector = root.getFieldVectors().get(c);
                Object value = vector.getObject(r);
                if (value != null && !(value instanceof String)
                    && !(value instanceof Number) && !(value instanceof Boolean)) {
                    value = value.toString();
                }
                row[c] = value;
            }
            rows.add(row);
        }
        return new QueryResponse(columns, rows);
    }
}
