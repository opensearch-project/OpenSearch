/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.SearchContextId;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.proto.request.common.FetchSourceContextProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.suggest.TermSuggestionBuilderProtoUtils;

import java.io.IOException;
import java.util.function.IntConsumer;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.unit.TimeValue.parseTimeValue;
import static org.opensearch.search.suggest.SuggestBuilders.termSuggestion;

/**
 * Utility class for converting SearchRequest Protocol Buffers to objects
 */
public class SearchRequestProtoUtils {

    private SearchRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Prepare the request for execution.
     * <p>
     * Similar to {@link RestSearchAction#prepareRequest(RestRequest, NodeClient)}
     * Please ensure to keep both implementations consistent.
     *
     * @param request the Protocol Buffer SearchRequest to execute
     * @param client the client to use for execution
     * @param queryUtils the query utils instance for parsing queries
     * @return the SearchRequest to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for
     *                     execution
     */
    public static org.opensearch.action.search.SearchRequest prepareRequest(
        org.opensearch.protobufs.SearchRequest request,
        Client client,
        AbstractQueryBuilderProtoUtils queryUtils
    ) throws IOException {
        org.opensearch.action.search.SearchRequest searchRequest = new org.opensearch.action.search.SearchRequest();

        /*
         * We have to pull out the call to `source().size(size)` because
         * _update_by_query and _delete_by_query uses this same parsing
         * path but sets a different variable when it sees the `size`
         * url parameter.
         *
         * Note that we can't use `searchRequest.source()::size` because
         * `searchRequest.source()` is null right now. We don't have to
         * guard against it being null in the IntConsumer because it can't
         * be null later. If that is confusing to you then you are in good
         * company.
         */
        IntConsumer setSize = size -> searchRequest.source().size(size);
        // TODO avoid hidden cast to NodeClient here
        parseSearchRequest(searchRequest, request, ((NodeClient) client).getNamedWriteableRegistry(), setSize, queryUtils);
        return searchRequest;
    }

    /**
     * Parses a protobuf {@link org.opensearch.protobufs.SearchRequest} to a {@link org.opensearch.action.search.SearchRequest}.
     * This method is similar to the logic in {@link RestSearchAction#parseSearchRequest(org.opensearch.action.search.SearchRequest, RestRequest, XContentParser, NamedWriteableRegistry, IntConsumer)}
     * Specifically, this method handles the URL parameters, and internally calls {@link SearchSourceBuilderProtoUtils#parseProto(SearchSourceBuilder, SearchRequestBody, AbstractQueryBuilderProtoUtils)}
     *
     * @param searchRequest the SearchRequest to populate
     * @param request the Protocol Buffer SearchRequest to parse
     * @param namedWriteableRegistry the registry for named writeables
     * @param setSize consumer for setting the size parameter
     * @param queryUtils the query utils instance for parsing queries
     * @throws IOException if an I/O exception occurred during parsing
     */
    protected static void parseSearchRequest(
        org.opensearch.action.search.SearchRequest searchRequest,
        org.opensearch.protobufs.SearchRequest request,
        NamedWriteableRegistry namedWriteableRegistry,
        IntConsumer setSize,
        AbstractQueryBuilderProtoUtils queryUtils
    ) throws IOException {
        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }

        String[] indexArr = new String[request.getIndexCount()];
        for (int i = 0; i < request.getIndexCount(); i++) {
            indexArr[i] = request.getIndex(i);
        }
        searchRequest.indices(indexArr);

        SearchSourceBuilderProtoUtils.parseProto(searchRequest.source(), request.getSearchRequestBody(), queryUtils);

        final int batchedReduceSize = request.hasBatchedReduceSize()
            ? request.getBatchedReduceSize()
            : searchRequest.getBatchedReduceSize();
        searchRequest.setBatchedReduceSize(batchedReduceSize);

        if (request.hasPreFilterShardSize()) {
            searchRequest.setPreFilterShardSize(request.getPreFilterShardSize());
        }

        if (request.hasMaxConcurrentShardRequests()) {
            // only set if we have the parameter since we auto adjust the max concurrency on the coordinator
            // based on the number of nodes in the cluster
            searchRequest.setMaxConcurrentShardRequests(request.getMaxConcurrentShardRequests());
        }

        if (request.hasAllowPartialSearchResults()) {
            // only set if we have the parameter passed to override the cluster-level default
            searchRequest.allowPartialSearchResults(request.getAllowPartialSearchResults());
        }
        if (request.hasPhaseTook()) {
            // only set if we have the parameter passed to override the cluster-level default
            // else phaseTook = null
            searchRequest.setPhaseTook(request.getPhaseTook());
        }
        // do not allow 'query_and_fetch' or 'dfs_query_and_fetch' search types
        // from the REST layer. these modes are an internal optimization and should
        // not be specified explicitly by the user.
        if (request.hasSearchType()) {
            searchRequest.searchType(SearchTypeProtoUtils.fromProto(request));
        }
        parseSearchSource(searchRequest.source(), request, setSize);

        if (request.hasRequestCache()) {
            searchRequest.requestCache(request.getRequestCache());
        }

        if (request.hasScroll()) {
            searchRequest.scroll(new Scroll(parseTimeValue(request.getScroll(), null, "scroll")));
        }

        if (request.getRoutingCount() > 0) {
            // Pass to {@link SearchRequest#routing(String ... routings)}
            searchRequest.routing(request.getRoutingList().toArray(new String[0]));
        } else {
            // Pass to {@link SearchRequest#routing(String routing)}
            searchRequest.routing((String) null);
        }
        searchRequest.preference(request.hasPreference() ? request.getPreference() : null);
        searchRequest.indicesOptions(IndicesOptionsProtoUtils.fromRequest(request, searchRequest.indicesOptions()));

        checkProtoTotalHits(request, searchRequest);

        // TODO what does this line do?
        // request.paramAsBoolean(INCLUDE_NAMED_QUERIES_SCORE_PARAM, false);

        if (searchRequest.pointInTimeBuilder() != null) {
            preparePointInTime(searchRequest, request, namedWriteableRegistry);
        } else {
            searchRequest.setCcsMinimizeRoundtrips(
                request.hasCcsMinimizeRoundtrips() ? request.getCcsMinimizeRoundtrips() : searchRequest.isCcsMinimizeRoundtrips()
            );
        }

        searchRequest.setCancelAfterTimeInterval(
            request.hasCancelAfterTimeInterval()
                ? parseTimeValue(request.getCancelAfterTimeInterval(), null, "cancel_after_time_interval")
                : null
        );
    }

    /**
     * Parses the search source from a Protocol Buffer SearchRequest.
     * Similar to {@link RestSearchAction#parseSearchSource(SearchSourceBuilder, RestRequest, IntConsumer)}
     *
     * @param searchSourceBuilder the SearchSourceBuilder to populate
     * @param request the Protocol Buffer SearchRequest to parse
     * @param setSize consumer for setting the size parameter
     */
    protected static void parseSearchSource(
        final SearchSourceBuilder searchSourceBuilder,
        org.opensearch.protobufs.SearchRequest request,
        IntConsumer setSize
    ) {
        QueryBuilder queryBuilder = ProtoActionsProtoUtils.urlParamsToQueryBuilder(request);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }

        if (request.getDocvalueFieldsCount() > 0) {
            for (String field : request.getDocvalueFieldsList()) {
                searchSourceBuilder.docValueField(field, null);
            }
        }
        FetchSourceContext fetchSourceContext = FetchSourceContextProtoUtils.parseFromProtoRequest(request);
        if (fetchSourceContext != null) {
            searchSourceBuilder.fetchSource(fetchSourceContext);
        }

        if (request.hasSuggestField()) {
            String suggestField = request.getSuggestField();
            String suggestText = request.hasSuggestText() ? request.getSuggestText() : request.getQ();
            int suggestSize = request.hasSuggestSize() ? request.getSuggestSize() : 5;
            org.opensearch.protobufs.SuggestMode suggestMode = request.getSuggestMode();
            searchSourceBuilder.suggest(
                new SuggestBuilder().addSuggestion(
                    suggestField,
                    termSuggestion(suggestField).text(suggestText)
                        .size(suggestSize)
                        .suggestMode(TermSuggestionBuilderProtoUtils.resolve(suggestMode))
                )
            );
        }
    }

    /**
     * Prepares a point in time search request.
     * Similar to {@link RestSearchAction#preparePointInTime(org.opensearch.action.search.SearchRequest, RestRequest, NamedWriteableRegistry)}
     *
     * @param request the SearchRequest to prepare
     * @param protoRequest the Protocol Buffer SearchRequest
     * @param namedWriteableRegistry the registry for named writeables
     */
    private static void preparePointInTime(
        org.opensearch.action.search.SearchRequest request,
        org.opensearch.protobufs.SearchRequest protoRequest,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        assert request.pointInTimeBuilder() != null;
        ActionRequestValidationException validationException = null;
        if (request.indices().length > 0) {
            validationException = addValidationError("[indices] cannot be used with point in time", validationException);
        }
        if (request.indicesOptions() != org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS) {
            validationException = addValidationError("[indicesOptions] cannot be used with point in time", validationException);
        }
        if (request.routing() != null) {
            validationException = addValidationError("[routing] cannot be used with point in time", validationException);
        }
        if (request.preference() != null) {
            validationException = addValidationError("[preference] cannot be used with point in time", validationException);
        }
        if (protoRequest.hasCcsMinimizeRoundtrips() && protoRequest.getCcsMinimizeRoundtrips()) {
            validationException = addValidationError("[ccs_minimize_roundtrips] cannot be used with point in time", validationException);
            request.setCcsMinimizeRoundtrips(false);
        }
        ExceptionsHelper.reThrowIfNotNull(validationException);

        final IndicesOptions indicesOptions = request.indicesOptions();
        final IndicesOptions stricterIndicesOptions = IndicesOptions.fromOptions(
            indicesOptions.ignoreUnavailable(),
            indicesOptions.allowNoIndices(),
            false,
            false,
            false,
            true,
            true,
            indicesOptions.ignoreThrottled()
        );
        request.indicesOptions(stricterIndicesOptions);
        final SearchContextId searchContextId = SearchContextId.decode(namedWriteableRegistry, request.pointInTimeBuilder().getId());
        request.indices(searchContextId.getActualIndices());
    }

    /**
     * Checks and configures total hits tracking in the search request.
     * Keep implementation consistent with {@link RestSearchAction#checkRestTotalHits(RestRequest, org.opensearch.action.search.SearchRequest)}
     *
     * @param protoRequest the Protocol Buffer SearchRequest
     * @param searchRequest the SearchRequest to configure
     */
    protected static void checkProtoTotalHits(SearchRequest protoRequest, org.opensearch.action.search.SearchRequest searchRequest) {

        boolean totalHitsAsInt = protoRequest.hasTotalHitsAsInt() ? protoRequest.getTotalHitsAsInt() : false;
        if (totalHitsAsInt == false) {
            return;
        }
        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }
        Integer trackTotalHitsUpTo = searchRequest.source().trackTotalHitsUpTo();
        if (trackTotalHitsUpTo == null) {
            searchRequest.source().trackTotalHits(true);
        } else if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE
            && trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                throw new IllegalArgumentException(
                    "["
                        + "rest_total_hits_as_int"
                        + "] cannot be used "
                        + "if the tracking of total hits is not accurate, got "
                        + trackTotalHitsUpTo
                );
            }
    }
}
