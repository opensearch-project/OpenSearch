/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.ColumnResolver;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A search whose matches are streamed into an {@link ArrowBatchSink} as the doc values of {@code docvalue_fields}.
 * <p>
 * It is a regular {@link SearchRequest}, so security and other action filters treat it as a search. Phase one supports
 * filtering only: matches are unscored and unordered. {@code size}, when set, caps the total number of rows,
 * {@code terminate_after} caps the rows per shard, {@code timeout} bounds the time each slice of a shard spends computing,
 * and {@code post_filter} filters like the query. Options that need scores, ordering, paging, fetching or aggregations are
 * rejected rather than ignored. The request holds a local sink, so it is never serialized.
 */
public final class ArrowSearchRequest extends SearchRequest {

    private final ArrowBatchSink sink;
    private final TimeValue defaultCancelAfterTimeInterval;

    /**
     * Creates an Arrow search that runs {@code searchRequest} and streams its results into {@code sink}.
     *
     * @param searchRequest                  the search to run; this request takes it over
     * @param sink                           receives the results
     * @param defaultCancelAfterTimeInterval the cancellation timeout when the search does not set one
     */
    public ArrowSearchRequest(SearchRequest searchRequest, ArrowBatchSink sink, TimeValue defaultCancelAfterTimeInterval) {
        super(searchRequest);
        // The copy constructor drops the pipeline. Keep it so that validation rejects it instead of silently ignoring it.
        pipeline(searchRequest.pipeline());
        this.sink = Objects.requireNonNull(sink, "sink");
        this.defaultCancelAfterTimeInterval = defaultCancelAfterTimeInterval;
    }

    /**
     * Returns the sink that receives the results.
     */
    public ArrowBatchSink sink() {
        return sink;
    }

    /**
     * Returns the requested field names and patterns, in output column order.
     */
    public List<ColumnResolver.RequestedField> requestedFields() {
        return source().docValueFields()
            .stream()
            .map(f -> new ColumnResolver.RequestedField(f.field, Column.LIST_FORMAT.equals(f.format)))
            .toList();
    }

    /**
     * Returns the row limit of the search, set by {@code size}, or {@link Long#MAX_VALUE} if unlimited.
     */
    public long rowLimit() {
        final int size = source().size();
        return size < 0 ? Long.MAX_VALUE : size;
    }

    /**
     * Returns the row limit of each shard, the smaller of {@code terminate_after} and {@link #rowLimit}, or
     * {@link Long#MAX_VALUE} if unlimited.
     */
    public long shardRowLimit() {
        final int terminateAfter = source().terminateAfter();
        return terminateAfter == SearchContext.DEFAULT_TERMINATE_AFTER ? rowLimit() : Math.min(terminateAfter, rowLimit());
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        final SearchSourceBuilder source = source();
        if (source == null || source.docValueFields() == null || source.docValueFields().isEmpty()) {
            return addValidationError("[docvalue_fields] must name the fields to stream", e);
        }
        for (FieldAndFormat field : source.docValueFields()) {
            if (field.format != null && Column.LIST_FORMAT.equals(field.format) == false) {
                e = addValidationError(
                    "[docvalue_fields] format ["
                        + field.format
                        + "] is not supported: values keep their native Arrow type, and format ["
                        + Column.LIST_FORMAT
                        + "] reads every value of a document into a list",
                    e
                );
                break;
            }
        }
        e = reject(e, scroll() != null, "scroll");
        e = reject(e, pointInTimeBuilder() != null || source.pointInTimeBuilder() != null, "pit");
        e = reject(e, searchType() != SearchType.QUERY_THEN_FETCH, "search_type");
        e = reject(e, source.aggregations() != null && source.aggregations().count() > 0, "aggregations");
        e = reject(e, source.suggest() != null, "suggest");
        e = reject(e, source.highlighter() != null, "highlight");
        e = reject(e, source.rescores() != null && source.rescores().isEmpty() == false, "rescore");
        e = reject(e, source.collapse() != null, "collapse");
        e = reject(e, source.searchAfter() != null, "search_after");
        e = reject(e, source.from() > 0, "from");
        e = reject(e, source.sorts() != null && source.sorts().isEmpty() == false, "sort");
        e = reject(e, source.slice() != null, "slice");
        e = reject(e, source.minScore() != null, "min_score");
        e = reject(e, source.trackScores(), "track_scores");
        e = reject(e, Boolean.TRUE.equals(source.explain()), "explain");
        e = reject(e, source.profile(), "profile");
        e = reject(e, Boolean.TRUE.equals(source.version()), "version");
        e = reject(e, Boolean.TRUE.equals(source.seqNoAndPrimaryTerm()), "seq_no_primary_term");
        e = reject(e, source.scriptFields() != null && source.scriptFields().isEmpty() == false, "script_fields");
        e = reject(e, source.fetchFields() != null && source.fetchFields().isEmpty() == false, "fields");
        e = reject(e, source.storedFields() != null, "stored_fields");
        // An Arrow search fetches no source, so it accepts _source: false.
        final FetchSourceContext fetchSource = source.fetchSource();
        e = reject(
            e,
            fetchSource != null && (fetchSource.fetchSource() || fetchSource.includes().length > 0 || fetchSource.excludes().length > 0),
            "_source"
        );
        e = reject(e, hasInnerHits(source.query()) || hasInnerHits(source.postFilter()), "inner_hits");
        e = reject(e, source.indexBoosts() != null && source.indexBoosts().isEmpty() == false, "indices_boost");
        e = reject(e, source.ext() != null && source.ext().isEmpty() == false, "ext");
        e = reject(
            e,
            (source.getDerivedFields() != null && source.getDerivedFields().isEmpty() == false)
                || (source.getDerivedFieldsObject() != null && source.getDerivedFieldsObject().isEmpty() == false),
            "derived"
        );
        e = reject(e, pipeline() != null || source.pipeline() != null || source.searchPipelineSource() != null, "search_pipeline");
        return e;
    }

    private static ActionRequestValidationException reject(ActionRequestValidationException e, boolean present, String option) {
        return present ? addValidationError("[" + option + "] is not supported by Arrow search", e) : e;
    }

    private static boolean hasInnerHits(@Nullable QueryBuilder query) {
        if (query == null) {
            return false;
        }
        final Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
        try {
            InnerHitContextBuilder.extractInnerHits(query, innerHits);
        } catch (IllegalArgumentException e) {
            // Thrown when two inner hits of the query share a name, so the query has inner hits.
            return true;
        }
        return innerHits.isEmpty() == false;
    }

    @Override
    public ArrowSearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        final TimeValue requested = getCancelAfterTimeInterval();
        return new ArrowSearchTask(
            id,
            type,
            action,
            this::getDescription,
            parentTaskId,
            headers,
            requested != null ? requested : defaultCancelAfterTimeInterval
        );
    }

    @Override
    public String getDescription() {
        // The description includes the source, which lists the requested fields.
        return "arrow " + buildDescription();
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("Arrow search requests execute on the local node only");
    }
}
