/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.DerivedFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.CancellableTask;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Lists the terms of one field on one shard copy.
 * <p>
 * The caller must have waited for the shard to become search-active, so that the searcher acquired here sees every
 * pending refresh. Terms are read only through {@code acquireSearcher("terms_enum")}, which applies any reader
 * wrapper such as the security plugin's, and the thread context is never stashed. A field that the node's field
 * filter hides is reported as absent before its type is asked to enumerate anything.
 *
 * @opensearch.internal
 */
final class TermsEnumShardOperation {

    private final IndicesService indicesService;
    private final SearchService searchService;
    private final LongSupplier nanoClock;

    /**
     * Creates the operation.
     *
     * @param indicesService resolves shards, field filters and metadata fields
     * @param searchService rules shards out through the index filter
     * @param nanoClock a monotonic clock in nanoseconds, the same one that produced {@code startNanos}
     */
    TermsEnumShardOperation(IndicesService indicesService, SearchService searchService, LongSupplier nanoClock) {
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.nanoClock = nanoClock;
    }

    /**
     * Lists the terms of one shard.
     *
     * @param request the shard request
     * @param task the shard task, checked for cancellation
     * @param startNanos the {@code nanoClock} reading taken when the request was received; the shard's budget runs from here
     * @return the shard's outcome
     * @throws TaskCancelledException if the task is cancelled
     * @throws IOException if reading the shard fails
     */
    TermsEnumShardResponse execute(TermsEnumShardRequest request, CancellableTask task, long startNanos) throws IOException {
        final long deadline = startNanos + TimeUnit.MILLISECONDS.toNanos(request.timeoutMillis());
        throwIfCancelled(task);
        final ShardId shardId = request.shardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        if (canMatch(request) == false) {
            return TermsEnumShardResponse.skipped();
        }
        try (Engine.Searcher searcher = indexService.getShard(shardId.id()).acquireSearcher("terms_enum")) {
            QueryShardContext context = indexService.newQueryShardContext(shardId.id(), searcher, request::nowInMillis, null);
            context.setAllowUnmappedFields(true);
            MappedFieldType fieldType = resolveField(indexService, context, request.field());
            if (fieldType == null) {
                return TermsEnumShardResponse.fieldAbsent();
            }
            // the field filter is the only field-level security guard for types that never read the terms dictionary
            if (indicesService.isMetadataField(fieldType.name()) == false
                && indicesService.getFieldFilter().apply(shardId.getIndexName()).test(fieldType.name()) == false) {
                return TermsEnumShardResponse.fieldAbsent();
            }
            BytesRef searchAfter = request.searchAfter() == null ? null : new BytesRef(request.searchAfter());
            final TermsEnum candidates;
            try {
                candidates = fieldType.termsEnum(
                    searcher.getIndexReader(),
                    request.string(),
                    request.caseInsensitive(),
                    searchAfter,
                    context
                );
            } catch (IllegalArgumentException e) {
                return TermsEnumShardResponse.fieldUnsupported(e.getMessage());
            }
            // stop at the deadline or on cancellation; a cancelled task is reported as such below, not as a partial result
            BooleanSupplier stop = () -> task.isCancelled() || nanoClock.getAsLong() - deadline >= 0;
            TermsEnumCollector.Result result = TermsEnumCollector.collect(
                fieldType,
                candidates,
                searcher.getIndexReader(),
                searchAfter,
                request.size(),
                stop
            );
            throwIfCancelled(task);
            return TermsEnumShardResponse.ok(result.terms(), result.timedOut());
        }
    }

    private boolean canMatch(TermsEnumShardRequest request) throws IOException {
        QueryBuilder filter = request.indexFilter();
        if (filter == null || filter instanceof MatchAllQueryBuilder) {
            return true;
        }
        ShardSearchRequest searchRequest = new ShardSearchRequest(request.shardId(), request.nowInMillis(), AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(filter));
        return searchService.canMatch(searchRequest).canMatch();
    }

    /**
     * Resolves aliases and multi-fields to the concrete field type. A derived field defined in the mapping is returned
     * as is: the context hides it when derived fields are disabled on the index, which would report it as absent
     * rather than unsupported.
     */
    private static MappedFieldType resolveField(IndexService indexService, QueryShardContext context, String field) {
        MappedFieldType mapped = indexService.mapperService().fieldType(field);
        if (mapped != null && mapped.unwrap() instanceof DerivedFieldType) {
            return mapped;
        }
        return context.fieldMapper(field);
    }

    private static void throwIfCancelled(CancellableTask task) {
        if (task.isCancelled()) {
            throw new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
        }
    }
}
