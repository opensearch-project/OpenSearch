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

import org.apache.hc.core5.http.HttpEntity;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.core.GetSourceRequest;
import org.opensearch.client.core.GetSourceResponse;
import org.opensearch.client.core.MainRequest;
import org.opensearch.client.core.MainResponse;
import org.opensearch.client.core.MultiTermVectorsRequest;
import org.opensearch.client.core.MultiTermVectorsResponse;
import org.opensearch.client.core.TermVectorsRequest;
import org.opensearch.client.core.TermVectorsResponse;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.rankeval.RankEvalRequest;
import org.opensearch.index.rankeval.RankEvalResponse;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.plugins.spi.NamedXContentProvider;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.mustache.MultiSearchTemplateRequest;
import org.opensearch.script.mustache.MultiSearchTemplateResponse;
import org.opensearch.script.mustache.SearchTemplateRequest;
import org.opensearch.script.mustache.SearchTemplateResponse;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.opensearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.ParsedComposite;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilters;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.ParsedGlobal;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.ParsedAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.opensearch.search.aggregations.bucket.histogram.ParsedVariableWidthHistogram;
import org.opensearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.ParsedMissing;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.ParsedNested;
import org.opensearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.opensearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.opensearch.search.aggregations.bucket.range.ParsedDateRange;
import org.opensearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.opensearch.search.aggregations.bucket.range.ParsedRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.opensearch.search.aggregations.bucket.terms.LongRareTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongRareTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringRareTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.StringRareTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ParsedAvg;
import org.opensearch.search.aggregations.metrics.ParsedCardinality;
import org.opensearch.search.aggregations.metrics.ParsedExtendedStats;
import org.opensearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.opensearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMedianAbsoluteDeviation;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.opensearch.search.aggregations.metrics.ParsedStats;
import org.opensearch.search.aggregations.metrics.ParsedSum;
import org.opensearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.ParsedTopHits;
import org.opensearch.search.aggregations.metrics.ParsedValueCount;
import org.opensearch.search.aggregations.metrics.ParsedWeightedAvg;
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.ParsedDerivative;
import org.opensearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.opensearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.opensearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.completion.CompletionSuggestion;
import org.opensearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.opensearch.search.suggest.phrase.PhraseSuggestion;
import org.opensearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.opensearch.search.suggest.term.TermSuggestion;
import org.opensearch.search.suggest.term.TermSuggestionBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

/**
 * High level REST client that wraps an instance of the low level {@link RestClient} and allows to build requests and read responses. The
 * {@link RestClient} instance is internally built based on the provided {@link RestClientBuilder} and it gets closed automatically when
 * closing the {@link RestHighLevelClient} instance that wraps it.
 * <p>
 *
 * In case an already existing instance of a low-level REST client needs to be provided, this class can be subclassed and the
 * {@link #RestHighLevelClient(RestClient, CheckedConsumer, List)} constructor can be used.
 * <p>
 *
 * This class can also be sub-classed to expose additional client methods that make use of endpoints added to OpenSearch through plugins,
 * or to add support for custom response sections, again added to OpenSearch through plugins.
 * <p>
 *
 * The majority of the methods in this class come in two flavors, a blocking and an asynchronous version (e.g.
 * {@link #search(SearchRequest, RequestOptions)} and {@link #searchAsync(SearchRequest, RequestOptions, ActionListener)}, where the later
 * takes an implementation of an {@link ActionListener} as an argument that needs to implement methods that handle successful responses and
 * failure scenarios. Most of the blocking calls can throw an {@link IOException} or an unchecked {@link OpenSearchException} in the
 * following cases:
 *
 * <ul>
 * <li>an {@link IOException} is usually thrown in case of failing to parse the REST response in the high-level REST client, the request
 * times out or similar cases where there is no response coming back from the OpenSearch server</li>
 * <li>an {@link OpenSearchException} is usually thrown in case where the server returns a 4xx or 5xx error code. The high-level client
 * then tries to parse the response body error details into a generic OpenSearchException and suppresses the original
 * {@link ResponseException}</li>
 * </ul>
 *
 */
public class RestHighLevelClient implements Closeable {

    private final RestClient client;
    private final NamedXContentRegistry registry;
    private final CheckedConsumer<RestClient, IOException> doClose;

    private final IndicesClient indicesClient = new IndicesClient(this);
    private final ClusterClient clusterClient = new ClusterClient(this);
    private final IngestClient ingestClient = new IngestClient(this);
    private final SnapshotClient snapshotClient = new SnapshotClient(this);
    private final TasksClient tasksClient = new TasksClient(this);

    /**
     * Creates a {@link RestHighLevelClient} given the low level {@link RestClientBuilder} that allows to build the
     * {@link RestClient} to be used to perform requests.
     */
    public RestHighLevelClient(RestClientBuilder restClientBuilder) {
        this(restClientBuilder, Collections.emptyList());
    }

    /**
     * Creates a {@link RestHighLevelClient} given the low level {@link RestClientBuilder} that allows to build the
     * {@link RestClient} to be used to perform requests and parsers for custom response sections added to OpenSearch through plugins.
     */
    protected RestHighLevelClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this(restClientBuilder.build(), RestClient::close, namedXContentEntries);
    }

    /**
     * Creates a {@link RestHighLevelClient} given the low level {@link RestClient} that it should use to perform requests and
     * a list of entries that allow to parse custom response sections added to OpenSearch through plugins.
     * This constructor can be called by subclasses in case an externally created low-level REST client needs to be provided.
     * The consumer argument allows to control what needs to be done when the {@link #close()} method is called.
     * Also subclasses can provide parsers for custom response sections added to OpenSearch through plugins.
     */
    protected RestHighLevelClient(
        RestClient restClient,
        CheckedConsumer<RestClient, IOException> doClose,
        List<NamedXContentRegistry.Entry> namedXContentEntries
    ) {
        this.client = Objects.requireNonNull(restClient, "restClient must not be null");
        this.doClose = Objects.requireNonNull(doClose, "doClose consumer must not be null");
        this.registry = new NamedXContentRegistry(
            Stream.of(getDefaultNamedXContents().stream(), getProvidedNamedXContents().stream(), namedXContentEntries.stream())
                .flatMap(Function.identity())
                .collect(toList())
        );
    }

    /**
     * Returns the low-level client that the current high-level client instance is using to perform requests
     */
    public final RestClient getLowLevelClient() {
        return client;
    }

    @Override
    public final void close() throws IOException {
        doClose.accept(client);
    }

    /**
     * Provides an {@link IndicesClient} which can be used to access the Indices API.
     *
     */
    public final IndicesClient indices() {
        return indicesClient;
    }

    /**
     * Provides a {@link ClusterClient} which can be used to access the Cluster API.
     */
    public final ClusterClient cluster() {
        return clusterClient;
    }

    /**
     * Provides a {@link IngestClient} which can be used to access the Ingest API.
     */
    public final IngestClient ingest() {
        return ingestClient;
    }

    /**
     * Provides a {@link SnapshotClient} which can be used to access the Snapshot API.
     */
    public final SnapshotClient snapshot() {
        return snapshotClient;
    }

    /**
     * Provides a {@link TasksClient} which can be used to access the Tasks API.
     */
    public final TasksClient tasks() {
        return tasksClient;
    }

    /**
     * Executes a bulk request using the Bulk API.
     * @param bulkRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(bulkRequest, RequestConverters::bulk, options, BulkResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously executes a bulk request using the Bulk API.
     * @param bulkRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable bulkAsync(BulkRequest bulkRequest, RequestOptions options, ActionListener<BulkResponse> listener) {
        return performRequestAsyncAndParseEntity(
            bulkRequest,
            RequestConverters::bulk,
            options,
            BulkResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a reindex request.
     * @param reindexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final BulkByScrollResponse reindex(ReindexRequest reindexRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            reindexRequest,
            RequestConverters::reindex,
            options,
            BulkByScrollResponse::fromXContent,
            singleton(409)
        );
    }

    /**
     * Submits a reindex task.
     * @param reindexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the submission response
     */
    public final TaskSubmissionResponse submitReindexTask(ReindexRequest reindexRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            reindexRequest,
            RequestConverters::submitReindex,
            options,
            TaskSubmissionResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a reindex request.
     * @param reindexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable reindexAsync(
        ReindexRequest reindexRequest,
        RequestOptions options,
        ActionListener<BulkByScrollResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            reindexRequest,
            RequestConverters::reindex,
            options,
            BulkByScrollResponse::fromXContent,
            listener,
            singleton(409)
        );
    }

    /**
     * Executes a update by query request.
     *
     * @param updateByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final BulkByScrollResponse updateByQuery(UpdateByQueryRequest updateByQueryRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            updateByQueryRequest,
            RequestConverters::updateByQuery,
            options,
            BulkByScrollResponse::fromXContent,
            singleton(409)
        );
    }

    /**
     * Submits a update by query task.
     *
     * @param updateByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the submission response
     */
    public final TaskSubmissionResponse submitUpdateByQueryTask(UpdateByQueryRequest updateByQueryRequest, RequestOptions options)
        throws IOException {
        return performRequestAndParseEntity(
            updateByQueryRequest,
            RequestConverters::submitUpdateByQuery,
            options,
            TaskSubmissionResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously executes an update by query request.
     *
     * @param updateByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable updateByQueryAsync(
        UpdateByQueryRequest updateByQueryRequest,
        RequestOptions options,
        ActionListener<BulkByScrollResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            updateByQueryRequest,
            RequestConverters::updateByQuery,
            options,
            BulkByScrollResponse::fromXContent,
            listener,
            singleton(409)
        );
    }

    /**
     * Executes a delete by query request.
     *
     * @param deleteByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final BulkByScrollResponse deleteByQuery(DeleteByQueryRequest deleteByQueryRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            deleteByQueryRequest,
            RequestConverters::deleteByQuery,
            options,
            BulkByScrollResponse::fromXContent,
            singleton(409)
        );
    }

    /**
     * Submits a delete by query task
     *
     * @param deleteByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the submission response
     */
    public final TaskSubmissionResponse submitDeleteByQueryTask(DeleteByQueryRequest deleteByQueryRequest, RequestOptions options)
        throws IOException {
        return performRequestAndParseEntity(
            deleteByQueryRequest,
            RequestConverters::submitDeleteByQuery,
            options,
            TaskSubmissionResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a delete by query request.
     *
     * @param deleteByQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable deleteByQueryAsync(
        DeleteByQueryRequest deleteByQueryRequest,
        RequestOptions options,
        ActionListener<BulkByScrollResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            deleteByQueryRequest,
            RequestConverters::deleteByQuery,
            options,
            BulkByScrollResponse::fromXContent,
            listener,
            singleton(409)
        );
    }

    /**
     * Executes a delete by query rethrottle request.
     *
     * @param rethrottleRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final ListTasksResponse deleteByQueryRethrottle(RethrottleRequest rethrottleRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            rethrottleRequest,
            RequestConverters::rethrottleDeleteByQuery,
            options,
            ListTasksResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously execute an delete by query rethrottle request.
     *
     * @param rethrottleRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable deleteByQueryRethrottleAsync(
        RethrottleRequest rethrottleRequest,
        RequestOptions options,
        ActionListener<ListTasksResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            rethrottleRequest,
            RequestConverters::rethrottleDeleteByQuery,
            options,
            ListTasksResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a update by query rethrottle request.
     *
     * @param rethrottleRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final ListTasksResponse updateByQueryRethrottle(RethrottleRequest rethrottleRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            rethrottleRequest,
            RequestConverters::rethrottleUpdateByQuery,
            options,
            ListTasksResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously execute an update by query rethrottle request.
     *
     * @param rethrottleRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable updateByQueryRethrottleAsync(
        RethrottleRequest rethrottleRequest,
        RequestOptions options,
        ActionListener<ListTasksResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            rethrottleRequest,
            RequestConverters::rethrottleUpdateByQuery,
            options,
            ListTasksResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a reindex rethrottling request.
     *
     * @param rethrottleRequest the request
     * @param options           the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final ListTasksResponse reindexRethrottle(RethrottleRequest rethrottleRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            rethrottleRequest,
            RequestConverters::rethrottleReindex,
            options,
            ListTasksResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Executes a reindex rethrottling request.
     *
     * @param rethrottleRequest the request
     * @param options           the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener          the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable reindexRethrottleAsync(
        RethrottleRequest rethrottleRequest,
        RequestOptions options,
        ActionListener<ListTasksResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            rethrottleRequest,
            RequestConverters::rethrottleReindex,
            options,
            ListTasksResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Pings the remote OpenSearch cluster and returns true if the ping succeeded, false otherwise
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return <code>true</code> if the ping succeeded, false otherwise
     */
    public final boolean ping(RequestOptions options) throws IOException {
        return performRequest(
            new MainRequest(),
            (request) -> RequestConverters.ping(),
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Get the cluster info otherwise provided when sending an HTTP request to '/'
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final MainResponse info(RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            new MainRequest(),
            (request) -> RequestConverters.info(),
            options,
            MainResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Retrieves a document by id using the Get API.
     *
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(getRequest, RequestConverters::get, options, GetResponse::fromXContent, singleton(404));
    }

    /**
     * Asynchronously retrieves a document by id using the Get API.
     *
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable getAsync(GetRequest getRequest, RequestOptions options, ActionListener<GetResponse> listener) {
        return performRequestAsyncAndParseEntity(
            getRequest,
            RequestConverters::get,
            options,
            GetResponse::fromXContent,
            listener,
            singleton(404)
        );
    }

    /**
     * Retrieves multiple documents by id using the Multi Get API.
     *
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @deprecated use {@link #mget(MultiGetRequest, RequestOptions)} instead
     */
    @Deprecated
    public final MultiGetResponse multiGet(MultiGetRequest multiGetRequest, RequestOptions options) throws IOException {
        return mget(multiGetRequest, options);
    }

    /**
     * Retrieves multiple documents by id using the Multi Get API.
     *
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final MultiGetResponse mget(MultiGetRequest multiGetRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            multiGetRequest,
            RequestConverters::multiGet,
            options,
            MultiGetResponse::fromXContent,
            singleton(404)
        );
    }

    /**
     * Asynchronously retrieves multiple documents by id using the Multi Get API.
     *
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #mgetAsync(MultiGetRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public final Cancellable multiGetAsync(
        MultiGetRequest multiGetRequest,
        RequestOptions options,
        ActionListener<MultiGetResponse> listener
    ) {
        return mgetAsync(multiGetRequest, options, listener);
    }

    /**
     * Asynchronously retrieves multiple documents by id using the Multi Get API.
     *
     * @param multiGetRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable mgetAsync(MultiGetRequest multiGetRequest, RequestOptions options, ActionListener<MultiGetResponse> listener) {
        return performRequestAsyncAndParseEntity(
            multiGetRequest,
            RequestConverters::multiGet,
            options,
            MultiGetResponse::fromXContent,
            listener,
            singleton(404)
        );
    }

    /**
     * Checks for the existence of a document. Returns true if it exists, false otherwise.
     *
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return <code>true</code> if the document exists, <code>false</code> otherwise
     */
    public final boolean exists(GetRequest getRequest, RequestOptions options) throws IOException {
        return performRequest(getRequest, RequestConverters::exists, options, RestHighLevelClient::convertExistsResponse, emptySet());
    }

    /**
     * Asynchronously checks for the existence of a document. Returns true if it exists, false otherwise.
     *
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable existsAsync(GetRequest getRequest, RequestOptions options, ActionListener<Boolean> listener) {
        return performRequestAsync(
            getRequest,
            RequestConverters::exists,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }

    /**
     * Checks for the existence of a document with a "_source" field. Returns true if it exists, false otherwise.
     *
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return <code>true</code> if the document and _source field exists, <code>false</code> otherwise
     * @deprecated use {@link #existsSource(GetSourceRequest, RequestOptions)} instead
     */
    @Deprecated
    public boolean existsSource(GetRequest getRequest, RequestOptions options) throws IOException {
        GetSourceRequest getSourceRequest = GetSourceRequest.from(getRequest);
        return performRequest(
            getSourceRequest,
            RequestConverters::sourceExists,
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Asynchronously checks for the existence of a document with a "_source" field. Returns true if it exists, false otherwise.
     *
     * @param getRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     * @deprecated use {@link #existsSourceAsync(GetSourceRequest, RequestOptions, ActionListener)} instead
     */
    @Deprecated
    public final Cancellable existsSourceAsync(GetRequest getRequest, RequestOptions options, ActionListener<Boolean> listener) {
        GetSourceRequest getSourceRequest = GetSourceRequest.from(getRequest);
        return performRequestAsync(
            getSourceRequest,
            RequestConverters::sourceExists,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }

    /**
     * Checks for the existence of a document with a "_source" field. Returns true if it exists, false otherwise.
     *
     * @param getSourceRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return <code>true</code> if the document and _source field exists, <code>false</code> otherwise
     */
    public boolean existsSource(GetSourceRequest getSourceRequest, RequestOptions options) throws IOException {
        return performRequest(
            getSourceRequest,
            RequestConverters::sourceExists,
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Asynchronously checks for the existence of a document with a "_source" field. Returns true if it exists, false otherwise.
     *
     * @param getSourceRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable existsSourceAsync(
        GetSourceRequest getSourceRequest,
        RequestOptions options,
        ActionListener<Boolean> listener
    ) {
        return performRequestAsync(
            getSourceRequest,
            RequestConverters::sourceExists,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }

    /**
     * Retrieves the source field only of a document using GetSource API.
     *
     * @param getSourceRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public GetSourceResponse getSource(GetSourceRequest getSourceRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            getSourceRequest,
            RequestConverters::getSource,
            options,
            GetSourceResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously retrieves the source field only of a document using GetSource API.
     *
     * @param getSourceRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable getSourceAsync(
        GetSourceRequest getSourceRequest,
        RequestOptions options,
        ActionListener<GetSourceResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            getSourceRequest,
            RequestConverters::getSource,
            options,
            GetSourceResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Index a document using the Index API.
     *
     * @param indexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(indexRequest, RequestConverters::index, options, IndexResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously index a document using the Index API.
     *
     * @param indexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable indexAsync(IndexRequest indexRequest, RequestOptions options, ActionListener<IndexResponse> listener) {
        return performRequestAsyncAndParseEntity(
            indexRequest,
            RequestConverters::index,
            options,
            IndexResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a count request using the Count API.
     *
     * @param countRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final CountResponse count(CountRequest countRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(countRequest, RequestConverters::count, options, CountResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously executes a count request using the Count API.
     *
     * @param countRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable countAsync(CountRequest countRequest, RequestOptions options, ActionListener<CountResponse> listener) {
        return performRequestAsyncAndParseEntity(
            countRequest,
            RequestConverters::count,
            options,
            CountResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Updates a document using the Update API.
     *
     * @param updateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final UpdateResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(updateRequest, RequestConverters::update, options, UpdateResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously updates a document using the Update API.
     *
     * @param updateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable updateAsync(UpdateRequest updateRequest, RequestOptions options, ActionListener<UpdateResponse> listener) {
        return performRequestAsyncAndParseEntity(
            updateRequest,
            RequestConverters::update,
            options,
            UpdateResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Deletes a document by id using the Delete API.
     *
     * @param deleteRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            deleteRequest,
            RequestConverters::delete,
            options,
            DeleteResponse::fromXContent,
            singleton(404)
        );
    }

    /**
     * Asynchronously deletes a document by id using the Delete API.
     *
     * @param deleteRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable deleteAsync(DeleteRequest deleteRequest, RequestOptions options, ActionListener<DeleteResponse> listener) {
        return performRequestAsyncAndParseEntity(
            deleteRequest,
            RequestConverters::delete,
            options,
            DeleteResponse::fromXContent,
            listener,
            Collections.singleton(404)
        );
    }

    /**
     * Executes a search request using the Search API.
     *
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            searchRequest,
            r -> RequestConverters.search(r, "_search"),
            options,
            SearchResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a search using the Search API.
     *
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable searchAsync(SearchRequest searchRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
        return performRequestAsyncAndParseEntity(
            searchRequest,
            r -> RequestConverters.search(r, "_search"),
            options,
            SearchResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a multi search using the msearch API.
     *
     * @param multiSearchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @deprecated use {@link #msearch(MultiSearchRequest, RequestOptions)} instead
     */
    @Deprecated
    public final MultiSearchResponse multiSearch(MultiSearchRequest multiSearchRequest, RequestOptions options) throws IOException {
        return msearch(multiSearchRequest, options);
    }

    /**
     * Executes a multi search using the msearch API.
     *
     * @param multiSearchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final MultiSearchResponse msearch(MultiSearchRequest multiSearchRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            multiSearchRequest,
            RequestConverters::multiSearch,
            options,
            MultiSearchResponse::fromXContext,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a multi search using the msearch API.
     *
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #msearchAsync(MultiSearchRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public final Cancellable multiSearchAsync(
        MultiSearchRequest searchRequest,
        RequestOptions options,
        ActionListener<MultiSearchResponse> listener
    ) {
        return msearchAsync(searchRequest, options, listener);
    }

    /**
     * Asynchronously executes a multi search using the msearch API.
     *
     * @param searchRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable msearchAsync(
        MultiSearchRequest searchRequest,
        RequestOptions options,
        ActionListener<MultiSearchResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            searchRequest,
            RequestConverters::multiSearch,
            options,
            MultiSearchResponse::fromXContext,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a search using the Search Scroll API.
     *
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @deprecated use {@link #scroll(SearchScrollRequest, RequestOptions)} instead
     */
    @Deprecated
    public final SearchResponse searchScroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return scroll(searchScrollRequest, options);
    }

    /**
     * Executes a search using the Search Scroll API.
     *
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            searchScrollRequest,
            RequestConverters::searchScroll,
            options,
            SearchResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a search using the Search Scroll API.
     *
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #scrollAsync(SearchScrollRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public final Cancellable searchScrollAsync(
        SearchScrollRequest searchScrollRequest,
        RequestOptions options,
        ActionListener<SearchResponse> listener
    ) {
        return scrollAsync(searchScrollRequest, options, listener);
    }

    /**
     * Asynchronously executes a search using the Search Scroll API.
     *
     * @param searchScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable scrollAsync(
        SearchScrollRequest searchScrollRequest,
        RequestOptions options,
        ActionListener<SearchResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            searchScrollRequest,
            RequestConverters::searchScroll,
            options,
            SearchResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Create PIT context using create PIT API
     *
     * @param createPitRequest the request
     * @param options          the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final CreatePitResponse createPit(CreatePitRequest createPitRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            createPitRequest,
            RequestConverters::createPit,
            options,
            CreatePitResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously Create PIT context using create PIT API
     *
     * @param createPitRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return the response
     */
    public final Cancellable createPitAsync(
        CreatePitRequest createPitRequest,
        RequestOptions options,
        ActionListener<CreatePitResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            createPitRequest,
            RequestConverters::createPit,
            options,
            CreatePitResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Delete point in time searches using delete PIT API
     *
     * @param deletePitRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final DeletePitResponse deletePit(DeletePitRequest deletePitRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            deletePitRequest,
            RequestConverters::deletePit,
            options,
            DeletePitResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously Delete point in time searches using delete PIT API
     *
     * @param deletePitRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return the response
     */
    public final Cancellable deletePitAsync(
        DeletePitRequest deletePitRequest,
        RequestOptions options,
        ActionListener<DeletePitResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            deletePitRequest,
            RequestConverters::deletePit,
            options,
            DeletePitResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Delete all point in time searches using delete all PITs API
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final DeletePitResponse deleteAllPits(RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            new MainRequest(),
            (request) -> RequestConverters.deleteAllPits(),
            options,
            DeletePitResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously Delete all point in time searches using delete all PITs API
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return the response
     */
    public final Cancellable deleteAllPitsAsync(RequestOptions options, ActionListener<DeletePitResponse> listener) {
        return performRequestAsyncAndParseEntity(
            new MainRequest(),
            (request) -> RequestConverters.deleteAllPits(),
            options,
            DeletePitResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Get all point in time searches using list all PITs API
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final GetAllPitNodesResponse getAllPits(RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            new MainRequest(),
            (request) -> RequestConverters.getAllPits(),
            options,
            GetAllPitNodesResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously get all point in time searches using list all PITs API
     *
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return the response
     */
    public final Cancellable getAllPitsAsync(RequestOptions options, ActionListener<GetAllPitNodesResponse> listener) {
        return performRequestAsyncAndParseEntity(
            new MainRequest(),
            (request) -> RequestConverters.getAllPits(),
            options,
            GetAllPitNodesResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Clears one or more scroll ids using the Clear Scroll API.
     *
     * @param clearScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            clearScrollRequest,
            RequestConverters::clearScroll,
            options,
            ClearScrollResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously clears one or more scroll ids using the Clear Scroll API.
     *
     * @param clearScrollRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable clearScrollAsync(
        ClearScrollRequest clearScrollRequest,
        RequestOptions options,
        ActionListener<ClearScrollResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            clearScrollRequest,
            RequestConverters::clearScroll,
            options,
            ClearScrollResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a request using the Search Template API.
     *
     * @param searchTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final SearchTemplateResponse searchTemplate(SearchTemplateRequest searchTemplateRequest, RequestOptions options)
        throws IOException {
        return performRequestAndParseEntity(
            searchTemplateRequest,
            RequestConverters::searchTemplate,
            options,
            SearchTemplateResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a request using the Search Template API.
     *
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable searchTemplateAsync(
        SearchTemplateRequest searchTemplateRequest,
        RequestOptions options,
        ActionListener<SearchTemplateResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            searchTemplateRequest,
            RequestConverters::searchTemplate,
            options,
            SearchTemplateResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a request using the Explain API.
     *
     * @param explainRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final ExplainResponse explain(ExplainRequest explainRequest, RequestOptions options) throws IOException {
        return performRequest(explainRequest, RequestConverters::explain, options, response -> {
            CheckedFunction<XContentParser, ExplainResponse, IOException> entityParser = parser -> ExplainResponse.fromXContent(
                parser,
                convertExistsResponse(response)
            );
            return parseEntity(response.getEntity(), entityParser);
        }, singleton(404));
    }

    /**
     * Asynchronously executes a request using the Explain API.
     *
     * @param explainRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable explainAsync(ExplainRequest explainRequest, RequestOptions options, ActionListener<ExplainResponse> listener) {
        return performRequestAsync(explainRequest, RequestConverters::explain, options, response -> {
            CheckedFunction<XContentParser, ExplainResponse, IOException> entityParser = parser -> ExplainResponse.fromXContent(
                parser,
                convertExistsResponse(response)
            );
            return parseEntity(response.getEntity(), entityParser);
        }, listener, singleton(404));
    }

    /**
     * Calls the Term Vectors API
     *
     * @param request   the request
     * @param options   the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public final TermVectorsResponse termvectors(TermVectorsRequest request, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            request,
            RequestConverters::termVectors,
            options,
            TermVectorsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the Term Vectors API
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable termvectorsAsync(
        TermVectorsRequest request,
        RequestOptions options,
        ActionListener<TermVectorsResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            request,
            RequestConverters::termVectors,
            options,
            TermVectorsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Calls the Multi Term Vectors API
     *
     * @param request   the request
     * @param options   the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public final MultiTermVectorsResponse mtermvectors(MultiTermVectorsRequest request, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            request,
            RequestConverters::mtermVectors,
            options,
            MultiTermVectorsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the Multi Term Vectors API
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable mtermvectorsAsync(
        MultiTermVectorsRequest request,
        RequestOptions options,
        ActionListener<MultiTermVectorsResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            request,
            RequestConverters::mtermVectors,
            options,
            MultiTermVectorsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a request using the Ranking Evaluation API.
     *
     * @param rankEvalRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final RankEvalResponse rankEval(RankEvalRequest rankEvalRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            rankEvalRequest,
            RequestConverters::rankEval,
            options,
            RankEvalResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Executes a request using the Multi Search Template API.
     *
     */
    public final MultiSearchTemplateResponse msearchTemplate(MultiSearchTemplateRequest multiSearchTemplateRequest, RequestOptions options)
        throws IOException {
        return performRequestAndParseEntity(
            multiSearchTemplateRequest,
            RequestConverters::multiSearchTemplate,
            options,
            MultiSearchTemplateResponse::fromXContext,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a request using the Multi Search Template API
     *
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable msearchTemplateAsync(
        MultiSearchTemplateRequest multiSearchTemplateRequest,
        RequestOptions options,
        ActionListener<MultiSearchTemplateResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            multiSearchTemplateRequest,
            RequestConverters::multiSearchTemplate,
            options,
            MultiSearchTemplateResponse::fromXContext,
            listener,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a request using the Ranking Evaluation API.
     *
     * @param rankEvalRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable rankEvalAsync(
        RankEvalRequest rankEvalRequest,
        RequestOptions options,
        ActionListener<RankEvalResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            rankEvalRequest,
            RequestConverters::rankEval,
            options,
            RankEvalResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Executes a request using the Field Capabilities API.
     *
     * @param fieldCapabilitiesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public final FieldCapabilitiesResponse fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest, RequestOptions options)
        throws IOException {
        return performRequestAndParseEntity(
            fieldCapabilitiesRequest,
            RequestConverters::fieldCaps,
            options,
            FieldCapabilitiesResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Get stored script by id.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public GetStoredScriptResponse getScript(GetStoredScriptRequest request, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            request,
            RequestConverters::getScript,
            options,
            GetStoredScriptResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously get stored script by id.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getScriptAsync(
        GetStoredScriptRequest request,
        RequestOptions options,
        ActionListener<GetStoredScriptResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            request,
            RequestConverters::getScript,
            options,
            GetStoredScriptResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Delete stored script by id.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public AcknowledgedResponse deleteScript(DeleteStoredScriptRequest request, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            request,
            RequestConverters::deleteScript,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously delete stored script by id.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteScriptAsync(
        DeleteStoredScriptRequest request,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            request,
            RequestConverters::deleteScript,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Puts an stored script using the Scripting API.
     *
     * @param putStoredScriptRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     */
    public AcknowledgedResponse putScript(PutStoredScriptRequest putStoredScriptRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(
            putStoredScriptRequest,
            RequestConverters::putScript,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously puts an stored script using the Scripting API.
     *
     * @param putStoredScriptRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putScriptAsync(
        PutStoredScriptRequest putStoredScriptRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            putStoredScriptRequest,
            RequestConverters::putScript,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Asynchronously executes a request using the Field Capabilities API.
     *
     * @param fieldCapabilitiesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable fieldCapsAsync(
        FieldCapabilitiesRequest fieldCapabilitiesRequest,
        RequestOptions options,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        return performRequestAsyncAndParseEntity(
            fieldCapabilitiesRequest,
            RequestConverters::fieldCaps,
            options,
            FieldCapabilitiesResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * @deprecated If creating a new HLRC ReST API call, consider creating new actions instead of reusing server actions. The Validation
     * layer has been added to the ReST client, and requests should extend {@link Validatable} instead of {@link ActionRequest}.
     */
    @Deprecated
    protected final <Req extends ActionRequest, Resp> Resp performRequestAndParseEntity(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<XContentParser, Resp, IOException> entityParser,
        Set<Integer> ignores
    ) throws IOException {
        return performRequest(request, requestConverter, options, response -> parseEntity(response.getEntity(), entityParser), ignores);
    }

    /**
     * Defines a helper method for performing a request and then parsing the returned entity using the provided entityParser.
     */
    protected final <Req extends Validatable, Resp> Resp performRequestAndParseEntity(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<XContentParser, Resp, IOException> entityParser,
        Set<Integer> ignores
    ) throws IOException {
        return performRequest(request, requestConverter, options, response -> parseEntity(response.getEntity(), entityParser), ignores);
    }

    /**
     * @deprecated If creating a new HLRC ReST API call, consider creating new actions instead of reusing server actions. The Validation
     * layer has been added to the ReST client, and requests should extend {@link Validatable} instead of {@link ActionRequest}.
     */
    @Deprecated
    protected final <Req extends ActionRequest, Resp> Resp performRequest(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<Response, Resp, IOException> responseConverter,
        Set<Integer> ignores
    ) throws IOException {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null && validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return internalPerformRequest(request, requestConverter, options, responseConverter, ignores);
    }

    /**
     * Defines a helper method for performing a request.
     */
    protected final <Req extends Validatable, Resp> Resp performRequest(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<Response, Resp, IOException> responseConverter,
        Set<Integer> ignores
    ) throws IOException {
        Optional<ValidationException> validationException = request.validate();
        if (validationException != null && validationException.isPresent()) {
            throw validationException.get();
        }
        return internalPerformRequest(request, requestConverter, options, responseConverter, ignores);
    }

    /**
     * Provides common functionality for performing a request.
     */
    private <Req, Resp> Resp internalPerformRequest(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<Response, Resp, IOException> responseConverter,
        Set<Integer> ignores
    ) throws IOException {
        Request req = requestConverter.apply(request);
        req.setOptions(options);
        Response response;
        try {
            response = client.performRequest(req);
        } catch (ResponseException e) {
            if (ignores.contains(e.getResponse().getStatusLine().getStatusCode())) {
                try {
                    return responseConverter.apply(e.getResponse());
                } catch (Exception innerException) {
                    // the exception is ignored as we now try to parse the response as an error.
                    // this covers cases like get where 404 can either be a valid document not found response,
                    // or an error for which parsing is completely different. We try to consider the 404 response as a valid one
                    // first. If parsing of the response breaks, we fall back to parsing it as an error.
                    throw parseResponseException(e);
                }
            }
            throw parseResponseException(e);
        }

        try {
            return responseConverter.apply(response);
        } catch (Exception e) {
            throw new IOException("Unable to parse response body for " + response, e);
        }
    }

    /**
     * Defines a helper method for requests that can 404 and in which case will return an empty Optional
     * otherwise tries to parse the response body
     */
    protected final <Req extends Validatable, Resp> Optional<Resp> performRequestAndParseOptionalEntity(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<XContentParser, Resp, IOException> entityParser
    ) throws IOException {
        Optional<ValidationException> validationException = request.validate();
        if (validationException != null && validationException.isPresent()) {
            throw validationException.get();
        }
        Request req = requestConverter.apply(request);
        req.setOptions(options);
        Response response;
        try {
            response = client.performRequest(req);
        } catch (ResponseException e) {
            if (RestStatus.NOT_FOUND.getStatus() == e.getResponse().getStatusLine().getStatusCode()) {
                return Optional.empty();
            }
            throw parseResponseException(e);
        }

        try {
            return Optional.of(parseEntity(response.getEntity(), entityParser));
        } catch (Exception e) {
            throw new IOException("Unable to parse response body for " + response, e);
        }
    }

    /**
     * @deprecated If creating a new HLRC ReST API call, consider creating new actions instead of reusing server actions. The Validation
     * layer has been added to the ReST client, and requests should extend {@link Validatable} instead of {@link ActionRequest}.
     * @return Cancellable instance that may be used to cancel the request
     */
    @Deprecated
    protected final <Req extends ActionRequest, Resp> Cancellable performRequestAsyncAndParseEntity(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<XContentParser, Resp, IOException> entityParser,
        ActionListener<Resp> listener,
        Set<Integer> ignores
    ) {
        return performRequestAsync(
            request,
            requestConverter,
            options,
            response -> parseEntity(response.getEntity(), entityParser),
            listener,
            ignores
        );
    }

    /**
     * Defines a helper method for asynchronously performing a request.
     * @return Cancellable instance that may be used to cancel the request
     */
    protected final <Req extends Validatable, Resp> Cancellable performRequestAsyncAndParseEntity(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<XContentParser, Resp, IOException> entityParser,
        ActionListener<Resp> listener,
        Set<Integer> ignores
    ) {
        return performRequestAsync(
            request,
            requestConverter,
            options,
            response -> parseEntity(response.getEntity(), entityParser),
            listener,
            ignores
        );
    }

    /**
     * @deprecated If creating a new HLRC ReST API call, consider creating new actions instead of reusing server actions. The Validation
     * layer has been added to the ReST client, and requests should extend {@link Validatable} instead of {@link ActionRequest}.
     * @return Cancellable instance that may be used to cancel the request
     */
    @Deprecated
    protected final <Req extends ActionRequest, Resp> Cancellable performRequestAsync(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<Response, Resp, IOException> responseConverter,
        ActionListener<Resp> listener,
        Set<Integer> ignores
    ) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null && validationException.validationErrors().isEmpty() == false) {
            listener.onFailure(validationException);
            return Cancellable.NO_OP;
        }
        return internalPerformRequestAsync(request, requestConverter, options, responseConverter, listener, ignores);
    }

    /**
     * Defines a helper method for asynchronously performing a request.
     * @return Cancellable instance that may be used to cancel the request
     */
    protected final <Req extends Validatable, Resp> Cancellable performRequestAsync(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<Response, Resp, IOException> responseConverter,
        ActionListener<Resp> listener,
        Set<Integer> ignores
    ) {
        Optional<ValidationException> validationException = request.validate();
        if (validationException != null && validationException.isPresent()) {
            listener.onFailure(validationException.get());
            return Cancellable.NO_OP;
        }
        return internalPerformRequestAsync(request, requestConverter, options, responseConverter, listener, ignores);
    }

    /**
     * Provides common functionality for asynchronously performing a request.
     * @return Cancellable instance that may be used to cancel the request
     */
    private <Req, Resp> Cancellable internalPerformRequestAsync(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<Response, Resp, IOException> responseConverter,
        ActionListener<Resp> listener,
        Set<Integer> ignores
    ) {
        if (listener == null) {
            throw new IllegalArgumentException("The listener is required and cannot be null");
        }

        Request req;
        try {
            req = requestConverter.apply(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return Cancellable.NO_OP;
        }
        req.setOptions(options);

        ResponseListener responseListener = wrapResponseListener(responseConverter, listener, ignores);
        return client.performRequestAsync(req, responseListener);
    }

    final <Resp> ResponseListener wrapResponseListener(
        CheckedFunction<Response, Resp, IOException> responseConverter,
        ActionListener<Resp> actionListener,
        Set<Integer> ignores
    ) {
        return new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(responseConverter.apply(response));
                } catch (Exception e) {
                    IOException ioe = new IOException("Unable to parse response body for " + response, e);
                    onFailure(ioe);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    Response response = responseException.getResponse();
                    if (ignores.contains(response.getStatusLine().getStatusCode())) {
                        try {
                            actionListener.onResponse(responseConverter.apply(response));
                        } catch (Exception innerException) {
                            // the exception is ignored as we now try to parse the response as an error.
                            // this covers cases like get where 404 can either be a valid document not found response,
                            // or an error for which parsing is completely different. We try to consider the 404 response as a valid one
                            // first. If parsing of the response breaks, we fall back to parsing it as an error.
                            actionListener.onFailure(parseResponseException(responseException));
                        }
                    } else {
                        actionListener.onFailure(parseResponseException(responseException));
                    }
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }

    /**
     * Asynchronous request which returns empty {@link Optional}s in the case of 404s or parses entity into an Optional
     * @return Cancellable instance that may be used to cancel the request
     */
    protected final <Req extends Validatable, Resp> Cancellable performRequestAsyncAndParseOptionalEntity(
        Req request,
        CheckedFunction<Req, Request, IOException> requestConverter,
        RequestOptions options,
        CheckedFunction<XContentParser, Resp, IOException> entityParser,
        ActionListener<Optional<Resp>> listener
    ) {
        Optional<ValidationException> validationException = request.validate();
        if (validationException != null && validationException.isPresent()) {
            listener.onFailure(validationException.get());
            return Cancellable.NO_OP;
        }
        Request req;
        try {
            req = requestConverter.apply(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return Cancellable.NO_OP;
        }
        req.setOptions(options);
        ResponseListener responseListener = wrapResponseListener404sOptional(
            response -> parseEntity(response.getEntity(), entityParser),
            listener
        );
        return client.performRequestAsync(req, responseListener);
    }

    final <Resp> ResponseListener wrapResponseListener404sOptional(
        CheckedFunction<Response, Resp, IOException> responseConverter,
        ActionListener<Optional<Resp>> actionListener
    ) {
        return new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(Optional.of(responseConverter.apply(response)));
                } catch (Exception e) {
                    IOException ioe = new IOException("Unable to parse response body for " + response, e);
                    onFailure(ioe);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    Response response = responseException.getResponse();
                    if (RestStatus.NOT_FOUND.getStatus() == response.getStatusLine().getStatusCode()) {
                        actionListener.onResponse(Optional.empty());
                    } else {
                        actionListener.onFailure(parseResponseException(responseException));
                    }
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }

    /**
     * Converts a {@link ResponseException} obtained from the low level REST client into an {@link OpenSearchException}.
     * If a response body was returned, tries to parse it as an error returned from OpenSearch.
     * If no response body was returned or anything goes wrong while parsing the error, returns a new {@link OpenSearchStatusException}
     * that wraps the original {@link ResponseException}. The potential exception obtained while parsing is added to the returned
     * exception as a suppressed exception. This method is guaranteed to not throw any exception eventually thrown while parsing.
     */
    protected final OpenSearchStatusException parseResponseException(ResponseException responseException) {
        Response response = responseException.getResponse();
        HttpEntity entity = response.getEntity();
        OpenSearchStatusException opensearchException;
        RestStatus restStatus = RestStatus.fromCode(response.getStatusLine().getStatusCode());

        if (entity == null) {
            opensearchException = new OpenSearchStatusException(responseException.getMessage(), restStatus, responseException);
        } else {
            try {
                opensearchException = parseEntity(entity, BytesRestResponse::errorFromXContent);
                opensearchException.addSuppressed(responseException);
            } catch (Exception e) {
                opensearchException = new OpenSearchStatusException("Unable to parse response body", restStatus, responseException);
                opensearchException.addSuppressed(e);
            }
        }
        return opensearchException;
    }

    protected final <Resp> Resp parseEntity(final HttpEntity entity, final CheckedFunction<XContentParser, Resp, IOException> entityParser)
        throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("OpenSearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaType(entity.getContentType());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType());
        }
        try (XContentParser parser = xContentType.xContent().createParser(registry, DEPRECATION_HANDLER, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    protected static boolean convertExistsResponse(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }

    /**
     * Ignores deprecation warnings. This is appropriate because it is only
     * used to parse responses from OpenSearch. Any deprecation warnings
     * emitted there just mean that you are talking to an old version of
     * OpenSearch. There isn't anything you can do about the deprecation.
     */
    private static final DeprecationHandler DEPRECATION_HANDLER = DeprecationHandler.IGNORE_DEPRECATIONS;

    static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MedianAbsoluteDeviationAggregationBuilder.NAME, (p, c) -> ParsedMedianAbsoluteDeviation.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(WeightedAvgAggregationBuilder.NAME, (p, c) -> ParsedWeightedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        map.put(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c));
        map.put(VariableWidthHistogramAggregationBuilder.NAME, (p, c) -> ParsedVariableWidthHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(LongRareTerms.NAME, (p, c) -> ParsedLongRareTerms.fromXContent(p, (String) c));
        map.put(StringRareTerms.NAME, (p, c) -> ParsedStringRareTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));
        map.put(MultiTermsAggregationBuilder.NAME, (p, c) -> ParsedMultiTerms.fromXContent(p, (String) c));
        List<NamedXContentRegistry.Entry> entries = map.entrySet()
            .stream()
            .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
            .collect(Collectors.toList());
        entries.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField(TermSuggestionBuilder.SUGGESTION_NAME),
                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField(PhraseSuggestionBuilder.SUGGESTION_NAME),
                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField(CompletionSuggestionBuilder.SUGGESTION_NAME),
                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)
            )
        );
        return entries;
    }

    /**
     * Loads and returns the {@link NamedXContentRegistry.Entry} parsers provided by plugins.
     */
    static List<NamedXContentRegistry.Entry> getProvidedNamedXContents() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        for (NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            entries.addAll(service.getNamedXContentParsers());
        }
        return entries;
    }
}
