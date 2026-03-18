/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts Calcite {@link RexNode} expressions into OpenSearch {@link QueryBuilder} instances.
 *
 * <p>Delegates to {@link PredicateHandler} implementations registered in
 * {@link PredicateHandlerRegistry}. When a {@link MapperService} is available
 * (data node, post-initialize), field type validation uses actual OpenSearch
 * mappings. When null (coordinator), falls back to Calcite type checks.
 */
public class RexToQueryBuilderConverter extends RexVisitorImpl<QueryBuilder> {

    private final RelDataType inputRowType;
    private final MapperService mapperService;

    /**
     * Coordinator-mode constructor (no MapperService available).
     */
    public RexToQueryBuilderConverter(RelDataType inputRowType) {
        this(inputRowType, null);
    }

    /**
     * Data-node-mode constructor with MapperService for field type validation.
     */
    public RexToQueryBuilderConverter(RelDataType inputRowType, MapperService mapperService) {
        super(true);
        this.inputRowType = inputRowType;
        this.mapperService = mapperService;
    }

    /**
     * Entry point: converts a {@link RexNode} into a {@link QueryBuilder}.
     */
    public QueryBuilder convert(RexNode node) {
        QueryBuilder result = node.accept(this);
        if (result == null) {
            throw new IllegalArgumentException("Unsupported RexNode type: " + node.getClass().getSimpleName());
        }
        return result;
    }

    @Override
    public QueryBuilder visitCall(RexCall call) {
        SqlKind kind = call.getKind();
        PredicateHandler handler = PredicateHandlerRegistry.getHandler(kind);
        if (handler == null) {
            throw new IllegalArgumentException("Unsupported operator: " + call.getOperator().getName());
        }
        if (handler.canHandle(call, inputRowType, mapperService) == false) {
            throw new IllegalArgumentException(
                "Operator " + call.getOperator().getName() + " is not supported for the given field types"
            );
        }
        return handler.convert(call, inputRowType, mapperService);
    }
}
