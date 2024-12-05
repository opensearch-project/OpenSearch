/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.GetSourceRequest;
import org.opensearch.client.core.MultiTermVectorsRequest;
import org.opensearch.client.core.TermVectorsRequest;
import org.opensearch.client.indices.AnalyzeRequest;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.rankeval.RankEvalRequest;
import org.opensearch.index.reindex.AbstractBulkByScrollRequest;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.action.search.RestCreatePitAction;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.script.mustache.MultiSearchTemplateRequest;
import org.opensearch.script.mustache.SearchTemplateRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Converts OpenSearch writeable requests to an HTTP Request
 *
 * @opensearch.api
 */
final class RequestConverters {
    static final MediaType REQUEST_BODY_CONTENT_TYPE = MediaTypeRegistry.JSON;

    private RequestConverters() {
        // Contains only status utility methods
    }

    static Request delete(DeleteRequest deleteRequest) {
        String endpoint = endpoint(deleteRequest.index(), deleteRequest.id());
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        Params parameters = new Params();
        parameters.withRouting(deleteRequest.routing());
        parameters.withTimeout(deleteRequest.timeout());
        parameters.withVersion(deleteRequest.version());
        parameters.withVersionType(deleteRequest.versionType());
        parameters.withIfSeqNo(deleteRequest.ifSeqNo());
        parameters.withIfPrimaryTerm(deleteRequest.ifPrimaryTerm());
        parameters.withRefreshPolicy(deleteRequest.getRefreshPolicy());
        parameters.withWaitForActiveShards(deleteRequest.waitForActiveShards());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request info() {
        return new Request(HttpGet.METHOD_NAME, "/");
    }

    static Request bulk(BulkRequest bulkRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_bulk");

        Params parameters = new Params();
        parameters.withTimeout(bulkRequest.timeout());
        parameters.withRefreshPolicy(bulkRequest.getRefreshPolicy());
        parameters.withPipeline(bulkRequest.pipeline());
        parameters.withRouting(bulkRequest.routing());
        if (bulkRequest.requireAlias() != null) {
            parameters.withRequireAlias(bulkRequest.requireAlias());
        }
        // Bulk API only supports newline delimited JSON or Smile. Before executing
        // the bulk, we need to check that all requests have the same content-type
        // and this content-type is supported by the Bulk API.
        MediaType bulkContentType = null;
        for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
            DocWriteRequest<?> action = bulkRequest.requests().get(i);

            DocWriteRequest.OpType opType = action.opType();
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                bulkContentType = enforceSameContentType((IndexRequest) action, bulkContentType);

            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateRequest updateRequest = (UpdateRequest) action;
                if (updateRequest.doc() != null) {
                    bulkContentType = enforceSameContentType(updateRequest.doc(), bulkContentType);
                }
                if (updateRequest.upsertRequest() != null) {
                    bulkContentType = enforceSameContentType(updateRequest.upsertRequest(), bulkContentType);
                }
            }
        }

        if (bulkContentType == null) {
            bulkContentType = MediaTypeRegistry.JSON;
        }

        final byte separator = bulkContentType.xContent().streamSeparator();
        final ContentType requestContentType = createContentType(bulkContentType);

        ByteArrayOutputStream content = new ByteArrayOutputStream();
        for (DocWriteRequest<?> action : bulkRequest.requests()) {
            DocWriteRequest.OpType opType = action.opType();

            try (XContentBuilder metadata = XContentBuilder.builder(bulkContentType.xContent())) {
                metadata.startObject();
                {
                    metadata.startObject(opType.getLowercase());
                    if (Strings.hasLength(action.index())) {
                        metadata.field("_index", action.index());
                    }
                    if (Strings.hasLength(action.id())) {
                        metadata.field("_id", action.id());
                    }
                    if (Strings.hasLength(action.routing())) {
                        metadata.field("routing", action.routing());
                    }
                    if (action.version() != Versions.MATCH_ANY) {
                        metadata.field("version", action.version());
                    }

                    VersionType versionType = action.versionType();
                    if (versionType != VersionType.INTERNAL) {
                        if (versionType == VersionType.EXTERNAL) {
                            metadata.field("version_type", "external");
                        } else if (versionType == VersionType.EXTERNAL_GTE) {
                            metadata.field("version_type", "external_gte");
                        }
                    }

                    if (action.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        metadata.field("if_seq_no", action.ifSeqNo());
                        metadata.field("if_primary_term", action.ifPrimaryTerm());
                    }

                    if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                        IndexRequest indexRequest = (IndexRequest) action;
                        if (Strings.hasLength(indexRequest.getPipeline())) {
                            metadata.field("pipeline", indexRequest.getPipeline());
                        }
                    } else if (opType == DocWriteRequest.OpType.UPDATE) {
                        UpdateRequest updateRequest = (UpdateRequest) action;
                        if (updateRequest.retryOnConflict() > 0) {
                            metadata.field("retry_on_conflict", updateRequest.retryOnConflict());
                        }
                        if (updateRequest.fetchSource() != null) {
                            metadata.field("_source", updateRequest.fetchSource());
                        }
                    }

                    if (action.isRequireAlias()) {
                        metadata.field("require_alias", action.isRequireAlias());
                    }
                    metadata.endObject();
                }
                metadata.endObject();

                BytesRef metadataSource = BytesReference.bytes(metadata).toBytesRef();
                content.write(metadataSource.bytes, metadataSource.offset, metadataSource.length);
                content.write(separator);
            }

            BytesRef source = null;
            if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest indexRequest = (IndexRequest) action;
                BytesReference indexSource = indexRequest.source();
                MediaType mediaType = indexRequest.getContentType();

                try (
                    XContentParser parser = XContentHelper.createParser(
                        /*
                         * EMPTY and THROW are fine here because we just call
                         * copyCurrentStructure which doesn't touch the
                         * registry or deprecation.
                         */
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        indexSource,
                        mediaType
                    )
                ) {
                    try (XContentBuilder builder = XContentBuilder.builder(bulkContentType.xContent())) {
                        builder.copyCurrentStructure(parser);
                        source = BytesReference.bytes(builder).toBytesRef();
                    }
                }
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                source = org.opensearch.core.xcontent.XContentHelper.toXContent(
                    (UpdateRequest) action,
                    bulkContentType,
                    ToXContent.EMPTY_PARAMS,
                    false
                ).toBytesRef();
            }

            if (source != null) {
                content.write(source.bytes, source.offset, source.length);
                content.write(separator);
            }
        }
        request.addParameters(parameters.asMap());
        request.setEntity(new ByteArrayEntity(content.toByteArray(), 0, content.size(), requestContentType));
        return request;
    }

    static Request exists(GetRequest getRequest) {
        return getStyleRequest(HttpHead.METHOD_NAME, getRequest);
    }

    static Request get(GetRequest getRequest) {
        return getStyleRequest(HttpGet.METHOD_NAME, getRequest);
    }

    private static Request getStyleRequest(String method, GetRequest getRequest) {
        Request request = new Request(method, endpoint(getRequest.index(), getRequest.id()));

        Params parameters = new Params();
        parameters.withPreference(getRequest.preference());
        parameters.withRouting(getRequest.routing());
        parameters.withRefresh(getRequest.refresh());
        parameters.withRealtime(getRequest.realtime());
        parameters.withStoredFields(getRequest.storedFields());
        parameters.withVersion(getRequest.version());
        parameters.withVersionType(getRequest.versionType());
        parameters.withFetchSourceContext(getRequest.fetchSourceContext());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request sourceExists(GetSourceRequest getSourceRequest) {
        return sourceRequest(getSourceRequest, HttpHead.METHOD_NAME);
    }

    static Request getSource(GetSourceRequest getSourceRequest) {
        return sourceRequest(getSourceRequest, HttpGet.METHOD_NAME);
    }

    private static Request sourceRequest(GetSourceRequest getSourceRequest, String httpMethodName) {
        Params parameters = new Params();
        parameters.withPreference(getSourceRequest.preference());
        parameters.withRouting(getSourceRequest.routing());
        parameters.withRefresh(getSourceRequest.refresh());
        parameters.withRealtime(getSourceRequest.realtime());
        parameters.withFetchSourceContext(getSourceRequest.fetchSourceContext());

        String endpoint = endpoint(getSourceRequest.index(), "_source", getSourceRequest.id());
        Request request = new Request(httpMethodName, endpoint);
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request multiGet(MultiGetRequest multiGetRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_mget");

        Params parameters = new Params();
        parameters.withPreference(multiGetRequest.preference());
        parameters.withRealtime(multiGetRequest.realtime());
        parameters.withRefresh(multiGetRequest.refresh());
        request.addParameters(parameters.asMap());
        request.setEntity(createEntity(multiGetRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request index(IndexRequest indexRequest) {
        String method = Strings.hasLength(indexRequest.id()) ? HttpPut.METHOD_NAME : HttpPost.METHOD_NAME;

        String endpoint;
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            endpoint = endpoint(indexRequest.index(), "_create", indexRequest.id());
        } else {
            endpoint = endpoint(indexRequest.index(), indexRequest.id());
        }

        Request request = new Request(method, endpoint);

        Params parameters = new Params();
        parameters.withRouting(indexRequest.routing());
        parameters.withTimeout(indexRequest.timeout());
        parameters.withVersion(indexRequest.version());
        parameters.withVersionType(indexRequest.versionType());
        parameters.withIfSeqNo(indexRequest.ifSeqNo());
        parameters.withIfPrimaryTerm(indexRequest.ifPrimaryTerm());
        parameters.withPipeline(indexRequest.getPipeline());
        parameters.withRefreshPolicy(indexRequest.getRefreshPolicy());
        parameters.withWaitForActiveShards(indexRequest.waitForActiveShards());
        parameters.withRequireAlias(indexRequest.isRequireAlias());

        BytesRef source = indexRequest.source().toBytesRef();
        ContentType contentType = createContentType(indexRequest.getContentType());
        request.addParameters(parameters.asMap());
        request.setEntity(new ByteArrayEntity(source.bytes, source.offset, source.length, contentType));
        return request;
    }

    static Request ping() {
        return new Request(HttpHead.METHOD_NAME, "/");
    }

    static Request update(UpdateRequest updateRequest) throws IOException {
        String endpoint = endpoint(updateRequest.index(), "_update", updateRequest.id());
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        Params parameters = new Params();
        parameters.withRouting(updateRequest.routing());
        parameters.withTimeout(updateRequest.timeout());
        parameters.withRefreshPolicy(updateRequest.getRefreshPolicy());
        parameters.withWaitForActiveShards(updateRequest.waitForActiveShards());
        parameters.withDocAsUpsert(updateRequest.docAsUpsert());
        parameters.withFetchSourceContext(updateRequest.fetchSource());
        parameters.withRetryOnConflict(updateRequest.retryOnConflict());
        parameters.withVersion(updateRequest.version());
        parameters.withVersionType(updateRequest.versionType());
        parameters.withRequireAlias(updateRequest.isRequireAlias());

        // The Java API allows update requests with different content types
        // set for the partial document and the upsert document. This client
        // only accepts update requests that have the same content types set
        // for both doc and upsert.
        MediaType mediaType = null;
        if (updateRequest.doc() != null) {
            mediaType = updateRequest.doc().getContentType();
        }
        if (updateRequest.upsertRequest() != null) {
            MediaType upsertContentType = updateRequest.upsertRequest().getContentType();
            if ((mediaType != null) && (mediaType != upsertContentType)) {
                throw new IllegalStateException(
                    "Update request cannot have different content types for doc ["
                        + mediaType
                        + "]"
                        + " and upsert ["
                        + upsertContentType
                        + "] documents"
                );
            } else {
                mediaType = upsertContentType;
            }
        }
        if (mediaType == null) {
            mediaType = Requests.INDEX_CONTENT_TYPE;
        }
        request.addParameters(parameters.asMap());
        request.setEntity(createEntity(updateRequest, mediaType));
        return request;
    }

    /**
     * Convert a {@linkplain SearchRequest} into a {@linkplain Request}.
     * @param searchRequest the request to convert
     * @param searchEndpoint the name of the search endpoint. {@literal _search}
     *    for standard searches
     */
    static Request search(SearchRequest searchRequest, String searchEndpoint) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, endpoint(searchRequest.indices(), searchEndpoint));

        Params params = new Params();
        addSearchRequestParams(params, searchRequest);

        if (searchRequest.source() != null) {
            request.setEntity(createEntity(searchRequest.source(), REQUEST_BODY_CONTENT_TYPE));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static void addSearchRequestParams(Params params, SearchRequest searchRequest) {
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withRouting(searchRequest.routing());
        params.withPreference(searchRequest.preference());
        if (searchRequest.pointInTimeBuilder() == null) {
            params.withIndicesOptions(searchRequest.indicesOptions());
        }
        params.withSearchType(searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        /*
          Merging search responses as part of CCS flow to reduce roundtrips is not supported for point in time -
          refer to org.opensearch.action.search.SearchResponseMerger
         */
        if (searchRequest.pointInTimeBuilder() != null) {
            params.putParam("ccs_minimize_roundtrips", "false");
        } else {
            params.putParam("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        }
        if (searchRequest.getPreFilterShardSize() != null) {
            params.putParam("pre_filter_shard_size", Integer.toString(searchRequest.getPreFilterShardSize()));
        }
        params.withMaxConcurrentShardRequests(searchRequest.getMaxConcurrentShardRequests());
        if (searchRequest.requestCache() != null) {
            params.withRequestCache(searchRequest.requestCache());
        }
        if (searchRequest.allowPartialSearchResults() != null) {
            params.withAllowPartialResults(searchRequest.allowPartialSearchResults());
        }
        params.withBatchedReduceSize(searchRequest.getBatchedReduceSize());
        if (searchRequest.scroll() != null) {
            params.putParam("scroll", searchRequest.scroll().keepAlive());
        }
    }

    static Request searchScroll(SearchScrollRequest searchScrollRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_search/scroll");
        request.setEntity(createEntity(searchScrollRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request clearScroll(ClearScrollRequest clearScrollRequest) throws IOException {
        Request request = new Request(HttpDelete.METHOD_NAME, "/_search/scroll");
        request.setEntity(createEntity(clearScrollRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request createPit(CreatePitRequest createPitRequest) throws IOException {
        Params params = new Params();
        params.putParam(RestCreatePitAction.ALLOW_PARTIAL_PIT_CREATION, Boolean.toString(createPitRequest.shouldAllowPartialPitCreation()));
        params.putParam(RestCreatePitAction.KEEP_ALIVE, createPitRequest.getKeepAlive());
        params.withIndicesOptions(createPitRequest.indicesOptions());
        Request request = new Request(HttpPost.METHOD_NAME, endpoint(createPitRequest.indices(), "_search/point_in_time"));
        request.addParameters(params.asMap());
        request.setEntity(createEntity(createPitRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deletePit(DeletePitRequest deletePitRequest) throws IOException {
        Request request = new Request(HttpDelete.METHOD_NAME, "/_search/point_in_time");
        request.setEntity(createEntity(deletePitRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteAllPits() {
        return new Request(HttpDelete.METHOD_NAME, "/_search/point_in_time/_all");
    }

    static Request getAllPits() {
        return new Request(HttpGet.METHOD_NAME, "/_search/point_in_time/_all");
    }

    static Request multiSearch(MultiSearchRequest multiSearchRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_msearch");

        Params params = new Params();
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (multiSearchRequest.maxConcurrentSearchRequests() != MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
            params.putParam("max_concurrent_searches", Integer.toString(multiSearchRequest.maxConcurrentSearchRequests()));
        }

        XContent xContent = REQUEST_BODY_CONTENT_TYPE.xContent();
        byte[] source = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, xContent);
        request.addParameters(params.asMap());
        request.setEntity(new ByteArrayEntity(source, createContentType(xContent.mediaType())));
        return request;
    }

    static Request searchTemplate(SearchTemplateRequest searchTemplateRequest) throws IOException {
        Request request;

        if (searchTemplateRequest.isSimulate()) {
            request = new Request(HttpGet.METHOD_NAME, "/_render/template");
        } else {
            SearchRequest searchRequest = searchTemplateRequest.getRequest();
            String endpoint = endpoint(searchRequest.indices(), "_search/template");
            request = new Request(HttpGet.METHOD_NAME, endpoint);

            Params params = new Params();
            addSearchRequestParams(params, searchRequest);
            request.addParameters(params.asMap());
        }

        request.setEntity(createEntity(searchTemplateRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request multiSearchTemplate(MultiSearchTemplateRequest multiSearchTemplateRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "/_msearch/template");

        Params params = new Params();
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (multiSearchTemplateRequest.maxConcurrentSearchRequests() != MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
            params.putParam("max_concurrent_searches", Integer.toString(multiSearchTemplateRequest.maxConcurrentSearchRequests()));
        }
        request.addParameters(params.asMap());

        XContent xContent = REQUEST_BODY_CONTENT_TYPE.xContent();
        byte[] source = MultiSearchTemplateRequest.writeMultiLineFormat(multiSearchTemplateRequest, xContent);
        request.setEntity(new ByteArrayEntity(source, createContentType(xContent.mediaType())));
        return request;
    }

    static Request count(CountRequest countRequest) throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, endpoint(countRequest.indices(), countRequest.types(), "_count"));
        Params params = new Params();
        params.withRouting(countRequest.routing());
        params.withPreference(countRequest.preference());
        params.withIndicesOptions(countRequest.indicesOptions());
        if (countRequest.terminateAfter() != 0) {
            params.withTerminateAfter(countRequest.terminateAfter());
        }
        if (countRequest.minScore() != null) {
            params.putParam("min_score", String.valueOf(countRequest.minScore()));
        }
        request.addParameters(params.asMap());
        request.setEntity(createEntity(countRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request explain(ExplainRequest explainRequest) throws IOException {
        String endpoint = endpoint(explainRequest.index(), "_explain", explainRequest.id());
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        Params params = new Params();
        params.withStoredFields(explainRequest.storedFields());
        params.withFetchSourceContext(explainRequest.fetchSourceContext());
        params.withRouting(explainRequest.routing());
        params.withPreference(explainRequest.preference());
        request.addParameters(params.asMap());
        request.setEntity(createEntity(explainRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest) throws IOException {
        String methodName = fieldCapabilitiesRequest.indexFilter() != null ? HttpPost.METHOD_NAME : HttpGet.METHOD_NAME;
        Request request = new Request(methodName, endpoint(fieldCapabilitiesRequest.indices(), "_field_caps"));

        Params params = new Params();
        params.withFields(fieldCapabilitiesRequest.fields());
        params.withIndicesOptions(fieldCapabilitiesRequest.indicesOptions());
        request.addParameters(params.asMap());
        if (fieldCapabilitiesRequest.indexFilter() != null) {
            request.setEntity(createEntity(fieldCapabilitiesRequest, REQUEST_BODY_CONTENT_TYPE));
        }
        return request;
    }

    static Request rankEval(RankEvalRequest rankEvalRequest) throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, endpoint(rankEvalRequest.indices(), Strings.EMPTY_ARRAY, "_rank_eval"));

        Params params = new Params();
        params.withIndicesOptions(rankEvalRequest.indicesOptions());
        params.putParam("search_type", rankEvalRequest.searchType().name().toLowerCase(Locale.ROOT));
        request.addParameters(params.asMap());
        request.setEntity(createEntity(rankEvalRequest.getRankEvalSpec(), REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request reindex(ReindexRequest reindexRequest) throws IOException {
        return prepareReindexRequest(reindexRequest, true);
    }

    static Request submitReindex(ReindexRequest reindexRequest) throws IOException {
        return prepareReindexRequest(reindexRequest, false);
    }

    static Request deleteByQuery(DeleteByQueryRequest deleteByQueryRequest) throws IOException {
        return prepareDeleteByQueryRequest(deleteByQueryRequest, true);
    }

    static Request submitDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest) throws IOException {
        return prepareDeleteByQueryRequest(deleteByQueryRequest, false);
    }

    static Request updateByQuery(UpdateByQueryRequest updateByQueryRequest) throws IOException {
        return prepareUpdateByQueryRequest(updateByQueryRequest, true);
    }

    static Request submitUpdateByQuery(UpdateByQueryRequest updateByQueryRequest) throws IOException {
        return prepareUpdateByQueryRequest(updateByQueryRequest, false);
    }

    private static Request prepareReindexRequest(ReindexRequest reindexRequest, boolean waitForCompletion) throws IOException {
        String endpoint = new EndpointBuilder().addPathPart("_reindex").build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params params = new Params().withWaitForCompletion(waitForCompletion)
            .withRefresh(reindexRequest.isRefresh())
            .withTimeout(reindexRequest.getTimeout())
            .withWaitForActiveShards(reindexRequest.getWaitForActiveShards())
            .withRequestsPerSecond(reindexRequest.getRequestsPerSecond())
            .withRequireAlias(reindexRequest.getDestination().isRequireAlias())
            .withSlices(reindexRequest.getSlices());

        if (reindexRequest.getScrollTime() != null) {
            params.putParam("scroll", reindexRequest.getScrollTime());
        }
        request.addParameters(params.asMap());
        request.setEntity(createEntity(reindexRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    private static Request prepareDeleteByQueryRequest(DeleteByQueryRequest deleteByQueryRequest, boolean waitForCompletion)
        throws IOException {
        String endpoint = endpoint(deleteByQueryRequest.indices(), "_delete_by_query");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params params = new Params().withRouting(deleteByQueryRequest.getRouting())
            .withRefresh(deleteByQueryRequest.isRefresh())
            .withTimeout(deleteByQueryRequest.getTimeout())
            .withWaitForActiveShards(deleteByQueryRequest.getWaitForActiveShards())
            .withRequestsPerSecond(deleteByQueryRequest.getRequestsPerSecond())
            .withIndicesOptions(deleteByQueryRequest.indicesOptions())
            .withWaitForCompletion(waitForCompletion)
            .withSlices(deleteByQueryRequest.getSlices());
        if (deleteByQueryRequest.isAbortOnVersionConflict() == false) {
            params.putParam("conflicts", "proceed");
        }
        if (deleteByQueryRequest.getBatchSize() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE) {
            params.putParam("scroll_size", Integer.toString(deleteByQueryRequest.getBatchSize()));
        }
        if (deleteByQueryRequest.getScrollTime() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT) {
            params.putParam("scroll", deleteByQueryRequest.getScrollTime());
        }
        if (deleteByQueryRequest.getMaxDocs() > 0) {
            params.putParam("max_docs", Integer.toString(deleteByQueryRequest.getMaxDocs()));
        }
        request.addParameters(params.asMap());
        request.setEntity(createEntity(deleteByQueryRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request prepareUpdateByQueryRequest(UpdateByQueryRequest updateByQueryRequest, boolean waitForCompletion) throws IOException {
        String endpoint = endpoint(updateByQueryRequest.indices(), "_update_by_query");
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params params = new Params().withRouting(updateByQueryRequest.getRouting())
            .withPipeline(updateByQueryRequest.getPipeline())
            .withRefresh(updateByQueryRequest.isRefresh())
            .withTimeout(updateByQueryRequest.getTimeout())
            .withWaitForActiveShards(updateByQueryRequest.getWaitForActiveShards())
            .withRequestsPerSecond(updateByQueryRequest.getRequestsPerSecond())
            .withIndicesOptions(updateByQueryRequest.indicesOptions())
            .withWaitForCompletion(waitForCompletion)
            .withSlices(updateByQueryRequest.getSlices());
        if (updateByQueryRequest.isAbortOnVersionConflict() == false) {
            params.putParam("conflicts", "proceed");
        }
        if (updateByQueryRequest.getBatchSize() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE) {
            params.putParam("scroll_size", Integer.toString(updateByQueryRequest.getBatchSize()));
        }
        if (updateByQueryRequest.getScrollTime() != AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT) {
            params.putParam("scroll", updateByQueryRequest.getScrollTime());
        }
        if (updateByQueryRequest.getMaxDocs() > 0) {
            params.putParam("max_docs", Integer.toString(updateByQueryRequest.getMaxDocs()));
        }
        request.addParameters(params.asMap());
        request.setEntity(createEntity(updateByQueryRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request rethrottleReindex(RethrottleRequest rethrottleRequest) {
        return rethrottle(rethrottleRequest, "_reindex");
    }

    static Request rethrottleUpdateByQuery(RethrottleRequest rethrottleRequest) {
        return rethrottle(rethrottleRequest, "_update_by_query");
    }

    static Request rethrottleDeleteByQuery(RethrottleRequest rethrottleRequest) {
        return rethrottle(rethrottleRequest, "_delete_by_query");
    }

    private static Request rethrottle(RethrottleRequest rethrottleRequest, String firstPathPart) {
        String endpoint = new EndpointBuilder().addPathPart(firstPathPart)
            .addPathPart(rethrottleRequest.getTaskId().toString())
            .addPathPart("_rethrottle")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params params = new Params().withRequestsPerSecond(rethrottleRequest.getRequestsPerSecond());
        // we set "group_by" to "none" because this is the response format we can parse back
        params.putParam("group_by", "none");
        request.addParameters(params.asMap());
        return request;
    }

    static Request putScript(PutStoredScriptRequest putStoredScriptRequest) throws IOException {
        String endpoint = new EndpointBuilder().addPathPartAsIs("_scripts").addPathPart(putStoredScriptRequest.id()).build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        Params params = new Params();
        params.withTimeout(putStoredScriptRequest.timeout());
        params.withClusterManagerTimeout(putStoredScriptRequest.clusterManagerNodeTimeout());
        if (Strings.hasText(putStoredScriptRequest.context())) {
            params.putParam("context", putStoredScriptRequest.context());
        }
        request.addParameters(params.asMap());
        request.setEntity(createEntity(putStoredScriptRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request analyze(AnalyzeRequest request) throws IOException {
        EndpointBuilder builder = new EndpointBuilder();
        String index = request.index();
        if (index != null) {
            builder.addPathPart(index);
        }
        builder.addPathPartAsIs("_analyze");
        Request req = new Request(HttpGet.METHOD_NAME, builder.build());
        req.setEntity(createEntity(request, REQUEST_BODY_CONTENT_TYPE));
        return req;
    }

    static Request termVectors(TermVectorsRequest tvrequest) throws IOException {
        String endpoint;
        if (tvrequest.getType() != null) {
            endpoint = new EndpointBuilder().addPathPart(tvrequest.getIndex(), tvrequest.getType(), tvrequest.getId())
                .addPathPartAsIs("_termvectors")
                .build();
        } else {
            endpoint = new EndpointBuilder().addPathPart(tvrequest.getIndex())
                .addPathPartAsIs("_termvectors")
                .addPathPart(tvrequest.getId())
                .build();
        }

        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        Params params = new Params();
        params.withRouting(tvrequest.getRouting());
        params.withPreference(tvrequest.getPreference());
        params.withRealtime(tvrequest.getRealtime());
        request.addParameters(params.asMap());
        request.setEntity(createEntity(tvrequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request mtermVectors(MultiTermVectorsRequest mtvrequest) throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "/_mtermvectors");
        request.setEntity(createEntity(mtvrequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getScript(GetStoredScriptRequest getStoredScriptRequest) {
        String endpoint = new EndpointBuilder().addPathPartAsIs("_scripts").addPathPart(getStoredScriptRequest.id()).build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        Params params = new Params();
        params.withClusterManagerTimeout(getStoredScriptRequest.clusterManagerNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteScript(DeleteStoredScriptRequest deleteStoredScriptRequest) {
        String endpoint = new EndpointBuilder().addPathPartAsIs("_scripts").addPathPart(deleteStoredScriptRequest.id()).build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        Params params = new Params();
        params.withTimeout(deleteStoredScriptRequest.timeout());
        params.withClusterManagerTimeout(deleteStoredScriptRequest.clusterManagerNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static HttpEntity createEntity(ToXContent toXContent, MediaType mediaType) throws IOException {
        return createEntity(toXContent, mediaType, ToXContent.EMPTY_PARAMS);
    }

    static HttpEntity createEntity(ToXContent toXContent, MediaType mediaType, ToXContent.Params toXContentParams) throws IOException {
        BytesRef source = org.opensearch.core.xcontent.XContentHelper.toXContent(toXContent, mediaType, toXContentParams, false)
            .toBytesRef();
        return new ByteArrayEntity(source.bytes, source.offset, source.length, createContentType(mediaType));
    }

    static String endpoint(String index, String id) {
        return new EndpointBuilder().addPathPart(index, MapperService.SINGLE_MAPPING_NAME, id).build();
    }

    @Deprecated
    static String endpoint(String index, String type, String id) {
        return new EndpointBuilder().addPathPart(index, type, id).build();
    }

    @Deprecated
    static String endpoint(String index, String type, String id, String endpoint) {
        return new EndpointBuilder().addPathPart(index, type, id).addPathPartAsIs(endpoint).build();
    }

    static String endpoint(String[] indices) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices).build();
    }

    static String endpoint(String[] indices, String endpoint) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint).build();
    }

    @Deprecated
    static String endpoint(String[] indices, String[] types, String endpoint) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices)
            .addCommaSeparatedPathParts(types)
            .addPathPartAsIs(endpoint)
            .build();
    }

    static String endpoint(String[] indices, String endpoint, String[] suffixes) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices)
            .addPathPartAsIs(endpoint)
            .addCommaSeparatedPathParts(suffixes)
            .build();
    }

    @Deprecated
    static String endpoint(String[] indices, String endpoint, String type) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices).addPathPartAsIs(endpoint).addPathPart(type).build();
    }

    /**
     * Returns a {@link ContentType} from a given {@link MediaType}.
     *
     * @param mediaType the {@link MediaType}
     * @return the {@link ContentType}
     */
    @SuppressForbidden(reason = "Only allowed place to convert a MediaType to a ContentType")
    public static ContentType createContentType(final MediaType mediaType) {
        return ContentType.create(mediaType.mediaTypeWithoutParameters(), (Charset) null);
    }

    /**
     * Utility class to help with common parameter names and patterns. Wraps
     * a {@link Request} and adds the parameters to it directly.
     */
    static class Params {
        private final Map<String, String> parameters = new HashMap<>();

        Params() {}

        Params putParam(String name, String value) {
            if (Strings.hasLength(value)) {
                parameters.put(name, value);
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.getStringRep());
            }
            return this;
        }

        Map<String, String> asMap() {
            return parameters;
        }

        Params withDocAsUpsert(boolean docAsUpsert) {
            if (docAsUpsert) {
                return putParam("doc_as_upsert", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withFetchSourceContext(FetchSourceContext fetchSourceContext) {
            if (fetchSourceContext != null) {
                if (fetchSourceContext.fetchSource() == false) {
                    putParam("_source", Boolean.FALSE.toString());
                }
                if (CollectionUtils.isEmpty(fetchSourceContext.includes()) == false) {
                    putParam("_source_includes", String.join(",", fetchSourceContext.includes()));
                }
                if (CollectionUtils.isEmpty(fetchSourceContext.excludes()) == false) {
                    putParam("_source_excludes", String.join(",", fetchSourceContext.excludes()));
                }
            }
            return this;
        }

        Params withFields(String[] fields) {
            if (CollectionUtils.isEmpty(fields) == false) {
                return putParam("fields", String.join(",", fields));
            }
            return this;
        }

        /**
         * @deprecated As of 2.0, because supporting inclusive language, replaced by {@link #withClusterManagerTimeout(TimeValue)}
         */
        @Deprecated
        Params withMasterTimeout(TimeValue clusterManagerTimeout) {
            return putParam("master_timeout", clusterManagerTimeout);
        }

        Params withClusterManagerTimeout(TimeValue clusterManagerTimeout) {
            return putParam("cluster_manager_timeout", clusterManagerTimeout);
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withPreference(String preference) {
            return putParam("preference", preference);
        }

        Params withSearchType(String searchType) {
            return putParam("search_type", searchType);
        }

        Params withMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
            return putParam("max_concurrent_shard_requests", Integer.toString(maxConcurrentShardRequests));
        }

        Params withBatchedReduceSize(int batchedReduceSize) {
            return putParam("batched_reduce_size", Integer.toString(batchedReduceSize));
        }

        Params withRequestCache(boolean requestCache) {
            return putParam("request_cache", Boolean.toString(requestCache));
        }

        Params withAllowPartialResults(boolean allowPartialSearchResults) {
            return putParam("allow_partial_search_results", Boolean.toString(allowPartialSearchResults));
        }

        Params withRealtime(boolean realtime) {
            if (realtime == false) {
                return putParam("realtime", Boolean.FALSE.toString());
            }
            return this;
        }

        Params withRefresh(boolean refresh) {
            if (refresh) {
                return withRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            return this;
        }

        /**
         *  @deprecated
         */
        @Deprecated
        Params withRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                return putParam("refresh", refreshPolicy.getValue());
            }
            return this;
        }

        Params withRequestsPerSecond(float requestsPerSecond) {
            // the default in AbstractBulkByScrollRequest is Float.POSITIVE_INFINITY,
            // but we don't want to add that to the URL parameters, instead we use -1
            if (Float.isFinite(requestsPerSecond)) {
                return putParam(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, Float.toString(requestsPerSecond));
            } else {
                return putParam(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, "-1");
            }
        }

        Params withRequireAlias(boolean requireAlias) {
            if (requireAlias) {
                return putParam("require_alias", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withRetryOnConflict(int retryOnConflict) {
            if (retryOnConflict > 0) {
                return putParam("retry_on_conflict", String.valueOf(retryOnConflict));
            }
            return this;
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withSlices(int slices) {
            if (slices == 0) {
                // translate to "auto" value in rest request so the receiving end doesn't throw error
                return putParam("slices", AbstractBulkByScrollRequest.AUTO_SLICES_VALUE);
            }
            return putParam("slices", String.valueOf(slices));
        }

        Params withStoredFields(String[] storedFields) {
            if (CollectionUtils.isEmpty(storedFields) == false) {
                return putParam("stored_fields", String.join(",", storedFields));
            }
            return this;
        }

        Params withTerminateAfter(int terminateAfter) {
            return putParam("terminate_after", String.valueOf(terminateAfter));
        }

        Params withTimeout(TimeValue timeout) {
            return putParam("timeout", timeout);
        }

        Params withVersion(long version) {
            if (version != Versions.MATCH_ANY) {
                return putParam("version", Long.toString(version));
            }
            return this;
        }

        Params withVersionType(VersionType versionType) {
            if (versionType != VersionType.INTERNAL) {
                return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIfSeqNo(long ifSeqNo) {
            if (ifSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return putParam("if_seq_no", Long.toString(ifSeqNo));
            }
            return this;
        }

        Params withIfPrimaryTerm(long ifPrimaryTerm) {
            if (ifPrimaryTerm != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                return putParam("if_primary_term", Long.toString(ifPrimaryTerm));
            }
            return this;
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount) {
            return withWaitForActiveShards(activeShardCount, ActiveShardCount.DEFAULT);
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount, ActiveShardCount defaultActiveShardCount) {
            if (activeShardCount != null && activeShardCount != defaultActiveShardCount) {
                return putParam("wait_for_active_shards", activeShardCount.toString().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIndicesOptions(IndicesOptions indicesOptions) {
            if (indicesOptions != null) {
                withIgnoreUnavailable(indicesOptions.ignoreUnavailable());
                putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
                String expandWildcards;
                if (indicesOptions.expandWildcardsOpen() == false && indicesOptions.expandWildcardsClosed() == false) {
                    expandWildcards = "none";
                } else {
                    StringJoiner joiner = new StringJoiner(",");
                    if (indicesOptions.expandWildcardsOpen()) {
                        joiner.add("open");
                    }
                    if (indicesOptions.expandWildcardsClosed()) {
                        joiner.add("closed");
                    }
                    expandWildcards = joiner.toString();
                }
                putParam("expand_wildcards", expandWildcards);
                putParam("ignore_throttled", Boolean.toString(indicesOptions.ignoreThrottled()));
            }
            return this;
        }

        Params withIgnoreUnavailable(boolean ignoreUnavailable) {
            // Always explicitly place the ignore_unavailable value.
            putParam("ignore_unavailable", Boolean.toString(ignoreUnavailable));
            return this;
        }

        Params withHuman(boolean human) {
            if (human) {
                putParam("human", Boolean.toString(human));
            }
            return this;
        }

        Params withLocal(boolean local) {
            if (local) {
                putParam("local", Boolean.toString(local));
            }
            return this;
        }

        Params withIncludeDefaults(boolean includeDefaults) {
            if (includeDefaults) {
                return putParam("include_defaults", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withPreserveExisting(boolean preserveExisting) {
            if (preserveExisting) {
                return putParam("preserve_existing", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withDetailed(boolean detailed) {
            if (detailed) {
                return putParam("detailed", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withWaitForCompletion(Boolean waitForCompletion) {
            return putParam("wait_for_completion", waitForCompletion.toString());
        }

        Params withNodes(String[] nodes) {
            return withNodes(Arrays.asList(nodes));
        }

        Params withNodes(List<String> nodes) {
            if (nodes != null && nodes.size() > 0) {
                return putParam("nodes", String.join(",", nodes));
            }
            return this;
        }

        Params withActions(String[] actions) {
            return withActions(Arrays.asList(actions));
        }

        Params withActions(List<String> actions) {
            if (actions != null && actions.size() > 0) {
                return putParam("actions", String.join(",", actions));
            }
            return this;
        }

        Params withTaskId(org.opensearch.core.tasks.TaskId taskId) {
            if (taskId != null && taskId.isSet()) {
                return putParam("task_id", taskId.toString());
            }
            return this;
        }

        Params withParentTaskId(org.opensearch.core.tasks.TaskId parentTaskId) {
            if (parentTaskId != null && parentTaskId.isSet()) {
                return putParam("parent_task_id", parentTaskId.toString());
            }
            return this;
        }

        Params withTaskId(TaskId taskId) {
            if (taskId != null && taskId.isSet()) {
                return putParam("task_id", taskId.toString());
            }
            return this;
        }

        Params withParentTaskId(TaskId parentTaskId) {
            if (parentTaskId != null && parentTaskId.isSet()) {
                return putParam("parent_task_id", parentTaskId.toString());
            }
            return this;
        }

        Params withWaitForStatus(ClusterHealthStatus status) {
            if (status != null) {
                return putParam("wait_for_status", status.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withWaitForNoRelocatingShards(boolean waitNoRelocatingShards) {
            if (waitNoRelocatingShards) {
                return putParam("wait_for_no_relocating_shards", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withWaitForNoInitializingShards(boolean waitNoInitShards) {
            if (waitNoInitShards) {
                return putParam("wait_for_no_initializing_shards", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withWaitForNodes(String waitForNodes) {
            return putParam("wait_for_nodes", waitForNodes);
        }

        Params withLevel(ClusterHealthRequest.Level level) {
            return putParam("level", level.name().toLowerCase(Locale.ROOT));
        }

        Params withWaitForEvents(Priority waitForEvents) {
            if (waitForEvents != null) {
                return putParam("wait_for_events", waitForEvents.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }
    }

    /**
     * Ensure that the {@link IndexRequest}'s content type is supported by the Bulk API and that it conforms
     * to the current {@link BulkRequest}'s content type (if it's known at the time of this method get called).
     *
     * @return the {@link IndexRequest}'s content type
     */
    static MediaType enforceSameContentType(IndexRequest indexRequest, @Nullable MediaType mediaType) {
        MediaType requestContentType = indexRequest.getContentType();
        if (requestContentType != MediaTypeRegistry.JSON && requestContentType != MediaTypeRegistry.fromFormat("smile")) {
            throw new IllegalArgumentException(
                "Unsupported content-type found for request with content-type ["
                    + requestContentType
                    + "], only JSON and SMILE are supported"
            );
        }
        if (mediaType == null) {
            return requestContentType;
        }
        if (requestContentType != mediaType) {
            throw new IllegalArgumentException(
                "Mismatching content-type found for request with content-type ["
                    + requestContentType
                    + "], previous requests have content-type ["
                    + mediaType
                    + "]"
            );
        }
        return mediaType;
    }

    /**
     * Utility class to build request's endpoint given its parts as strings
     */
    static class EndpointBuilder {

        private final StringJoiner joiner = new StringJoiner("/", "/", "");

        EndpointBuilder addPathPart(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(encodePart(part));
                }
            }
            return this;
        }

        EndpointBuilder addCommaSeparatedPathParts(String[] parts) {
            addPathPart(String.join(",", parts));
            return this;
        }

        EndpointBuilder addCommaSeparatedPathParts(List<String> parts) {
            addPathPart(String.join(",", parts));
            return this;
        }

        EndpointBuilder addPathPartAsIs(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(part);
                }
            }
            return this;
        }

        String build() {
            return joiner.toString();
        }

        private static String encodePart(String pathPart) {
            try {
                // encode each part (e.g. index, type and id) separately before merging them into the path
                // we prepend "/" to the path part to make this path absolute, otherwise there can be issues with
                // paths that start with `-` or contain `:`
                // the authority must be an empty string and not null, else paths that being with slashes could have them
                // misinterpreted as part of the authority.
                URI uri = new URI(null, "", "/" + pathPart, null, null);
                // manually encode any slash that each part may contain
                return uri.getRawPath().substring(1).replaceAll("/", "%2F");
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Path part [" + pathPart + "] couldn't be encoded", e);
            }
        }
    }
}
