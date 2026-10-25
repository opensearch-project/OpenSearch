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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.stream.Collectors;

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
public class TransportValidateAction extends HandledTransportAction<ValidateQueryRequest, ValidateQueryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportValidateAction.class);

    private final EngineContextProvider contextProvider;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexResolutionStrategy indexResolutionStrategy = new MultiIndexResolutionStrategy();
    private final ThreadPool threadPool;

    /**
     * Guice-injected constructor — receives the analytics engine context for schema access.
     *
     * @param transportService transport service
     * @param actionFilters action filters
     * @param contextProvider analytics engine context providing the Calcite schema
     * @param clusterService cluster service for resolving index aliases
     * @param indicesService creates the request-scoped MapperService for mapping-aware validation
     * @param indexNameExpressionResolver resolves aliases and wildcards to concrete indices
     * @param threadPool thread pool for offloading conversion work
     */
    @Inject
    public TransportValidateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContextProvider contextProvider,
        ClusterService clusterService,
        IndicesService indicesService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ThreadPool threadPool
    ) {
        super(ValidateAction.NAME, transportService, actionFilters, ValidateQueryRequest::new);
        this.contextProvider = contextProvider;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
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
     *
     * <p>Conversion is set up exactly as {@link TransportExecuteAction} does — same cluster-state
     * snapshot, same mapping-aware {@link SearchSourceConverter} — so a query validates here if and
     * only if execution would accept it.
     */
    private ValidateQueryResponse validate(ValidateQueryRequest request) {
        // One snapshot per request: index resolution, the engine schema, and mapping resolution
        // all derive from the same immutable cluster state.
        final ClusterState state = clusterService.state();
        final List<IndexMetadata> resolvedIndices = indexResolutionStrategy.resolve(indexNameExpressionResolver, state, request);
        // Reject filtering aliases the engine cannot honor (including hidden aliases reached via
        // expand_wildcards=open,hidden) before conversion — matching the execution path's 400 divergence.
        FilteringAliasGuard.check(indexNameExpressionResolver, state, request.indices(), request.indicesOptions(), resolvedIndices);
        // A comma-list of the resolved concrete names resolves to the schema's cross-index union
        // table; at one index it is that single name, matching the single-index path.
        final String indexExpression = resolvedIndices.stream().map(index -> index.getIndex().getName()).collect(Collectors.joining(","));

        // Like vanilla, detail (the plan explanation) is returned only when explicitly requested.
        final boolean detailRequested = request.explain() || request.rewrite() || request.allShards();

        // Legitimate empty resolution (allow_no_indices=true matching nothing) is trivially valid —
        // nothing to convert — mirroring the execution path's 200-empty short-circuit.
        // allow_no_indices=false with no match already threw IndexNotFoundException above.
        if (resolvedIndices.isEmpty()) {
            List<QueryExplanation> explanations = detailRequested
                ? List.of(new QueryExplanation(indexExpression, QueryExplanation.RANDOM_SHARD, true, null, null))
                : null;
            return new ValidateQueryResponse(true, explanations, 1, 1, 0, List.of());
        }

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(request.query());

        boolean valid;
        String explanation = null;
        String error = null;
        // Mapping pinned at request start, mirroring execution, and released when validation ends.
        try (
            RequestScopedMapperService mapperService = new RequestScopedMapperService(
                resolvedIndices,
                indicesService::createIndexMapperService
            )
        ) {
            SearchSourceConverter converter = new SearchSourceConverter(
                contextProvider.getContext(state, request.indicesOptions()).schema(),
                mapperService::fieldType
            );
            QueryPlans plans = converter.convert(source, indexExpression);
            // Mirror execution's schema-equivalence gate so a query validates here iff execution
            // would accept it; a no-op at a single index. A divergent multi-index query surfaces
            // as a request failure.
            if (resolvedIndices.size() > 1) {
                SchemaEquivalenceGate.check(
                    resolvedIndices,
                    mapperService::fieldType,
                    PlanFieldReferences.referencedFields(plans),
                    PlanFieldReferences.aggregatedBucketFields(source)
                );
            }
            valid = true;
            if (detailRequested) {
                explanation = plans.first().map(plan -> plan.relNode().explain().trim()).orElse(null);
            }
        } catch (ConversionException e) {
            valid = false;
            error = e.getMessage();
        }

        List<QueryExplanation> explanations = detailRequested
            ? List.of(new QueryExplanation(indexExpression, QueryExplanation.RANDOM_SHARD, valid, valid ? explanation : null, error))
            : null;
        return new ValidateQueryResponse(valid, explanations, 1, 1, 0, List.of());
    }
}
