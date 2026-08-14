/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.validate.query.QueryExplanation;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.List;

/**
 * Validates a query against the DSL-to-Calcite conversion pipeline without executing it.
 *
 * <p>Vanilla {@code _validate/query} checks whether Lucene can parse the query — the wrong
 * question on this path, where a query succeeds only if the converter accepts it. Validation
 * here is conversion without execution: {@link SearchSourceConverter#convert} either produces
 * plans (valid) or throws {@link ConversionException} (invalid, message reported as the error).
 *
 * <p>Scope notes:
 * <ul>
 *   <li>Query types without a registered translator convert into {@code UnresolvedQueryCall}
 *       for the engine's optimizer to resolve or reject at execution time — they validate as
 *       valid here. Conversion-level validation catches schema errors (unknown fields, unknown
 *       index), not engine capability limits.</li>
 *   <li>Like vanilla, explanations are returned only when {@code explain}, {@code rewrite}, or
 *       {@code all_shards} is set. The explanation text is the converted plan
 *       ({@code RelNode.explain()}) rather than a rewritten Lucene query.</li>
 *   <li>Conversion is a coordinator concern — there is no shard fan-out, so shard counts are
 *       reported as 1/1 and {@code all_shards} adds nothing beyond {@code explain}.</li>
 * </ul>
 */
public class TransportDslValidateAction extends HandledTransportAction<ValidateQueryRequest, ValidateQueryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDslValidateAction.class);

    private final EngineContextProvider contextProvider;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;

    /**
     * Guice-injected constructor — receives the analytics engine context for schema access.
     *
     * @param transportService transport service
     * @param actionFilters action filters
     * @param contextProvider analytics engine context providing the Calcite schema
     * @param clusterService cluster service for resolving index aliases
     * @param indexNameExpressionResolver resolves aliases and wildcards to concrete indices
     * @param threadPool thread pool for offloading conversion work
     */
    @Inject
    public TransportDslValidateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContextProvider contextProvider,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ThreadPool threadPool
    ) {
        super(DslValidateAction.NAME, transportService, actionFilters, ValidateQueryRequest::new);
        this.contextProvider = contextProvider;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                listener.onResponse(validate(request));
            } catch (Exception e) {
                logger.error("DSL validation failed", e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Runs the conversion pipeline and maps the outcome to a {@link ValidateQueryResponse}.
     * A {@link ConversionException} means the query is invalid on this path; any other
     * exception (index not found, multiple indices) propagates as a request failure,
     * matching how {@code _search} reports the same conditions.
     */
    private ValidateQueryResponse validate(ValidateQueryRequest request) {
        String indexName = resolveToSingleIndex(request);
        SearchSourceBuilder source = new SearchSourceBuilder();
        if (request.query() != null) {
            source.query(request.query());
        }

        boolean valid;
        String explanation = null;
        String error = null;
        try {
            SearchSourceConverter converter = new SearchSourceConverter(contextProvider.getContext().schema());
            QueryPlans plans = converter.convert(source, indexName);
            valid = true;
            explanation = plans.getAll().get(0).relNode().explain().trim();
        } catch (ConversionException e) {
            valid = false;
            error = e.getMessage();
        }

        // Like vanilla, detail is returned only when explicitly requested.
        List<QueryExplanation> explanations = request.explain() || request.rewrite() || request.allShards()
            ? List.of(new QueryExplanation(indexName, QueryExplanation.RANDOM_SHARD, valid, valid ? explanation : null, error))
            : null;
        return new ValidateQueryResponse(valid, explanations, 1, 1, 0, List.of());
    }

    /**
     * Resolves the request's indices to a single concrete index, mirroring
     * {@link TransportDslExecuteAction}'s single-index constraint.
     */
    private String resolveToSingleIndex(ValidateQueryRequest request) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterService.state(), request);
        if (concreteIndices.length != 1) {
            throw new IllegalArgumentException(
                "DSL validation currently supports exactly one concrete index, but resolved to " + concreteIndices.length + " indices"
            );
        }
        return concreteIndices[0].getName();
    }
}
