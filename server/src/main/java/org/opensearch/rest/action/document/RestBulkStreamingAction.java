/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.document;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.support.XContentHttpChunk;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 *
 * @opensearch.api
 */
public class RestBulkStreamingAction extends BaseRestHandler {
    private static final BulkResponse EMPTY = new BulkResponse(new BulkItemResponse[0], 0L);
    private final boolean allowExplicitIndex;

    public RestBulkStreamingAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(POST, "/_bulk/stream"),
                new Route(PUT, "/_bulk/stream"),
                new Route(POST, "/{index}/_bulk/stream"),
                new Route(PUT, "/{index}/_bulk/stream")
            )
        );
    }

    @Override
    public String getName() {
        return "streaming_bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String defaultIndex = request.param("index");
        final String defaultRouting = request.param("routing");
        final String defaultPipeline = request.param("pipeline");
        final String waitForActiveShards = request.param("wait_for_active_shards");
        final Boolean defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, null);
        final TimeValue timeout = request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT);
        final String refresh = request.param("refresh");

        final StreamingRestChannelConsumer consumer = (channel) -> {
            final MediaType mediaType = request.getMediaType();

            // Set the content type and the status code before sending the response stream over
            channel.prepareResponse(RestStatus.OK, Map.of("Content-Type", List.of(mediaType.mediaTypeWithoutParameters())));

            // This is initial implementation at the moment which transforms each single request stream chunk into
            // individual bulk request and streams each response back. Another source of inefficiency comes from converting
            // bulk response from raw (json/yaml/...) to model and back to raw (json/yaml/...).

            // TODOs:
            // - add batching (by interval and/or count)
            // - eliminate serialization inefficiencies
            Flux.from(channel).map(chunk -> {
                FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
                BulkRequest bulkRequest = Requests.bulkRequest();
                if (waitForActiveShards != null) {
                    bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
                }

                bulkRequest.timeout(timeout);
                bulkRequest.setRefreshPolicy(refresh);

                try {
                    bulkRequest.add(
                        chunk.content(),
                        defaultIndex,
                        defaultRouting,
                        defaultFetchSourceContext,
                        defaultPipeline,
                        defaultRequireAlias,
                        allowExplicitIndex,
                        request.getMediaType()
                    );
                } catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }

                return Tuple.tuple(chunk.isLast(), bulkRequest);
            }).flatMap(tuple -> {
                final CompletableFuture<BulkResponse> f = new CompletableFuture<>();

                if (tuple.v2().requests().isEmpty()) {
                    // this is the last request with no items
                    f.complete(EMPTY);
                } else {
                    client.bulk(tuple.v2(), new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse response) {
                            f.complete(response);
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            f.completeExceptionally(ex);
                        }
                    });

                    if (tuple.v1() == true /* last chunk */ ) {
                        return Flux.just(f, CompletableFuture.completedFuture(EMPTY));
                    }
                }

                return Mono.just(f);
            }).concatMap(f -> Mono.fromFuture(f).doOnNext(r -> {
                try {
                    if (r == EMPTY) {
                        channel.sendChunk(XContentHttpChunk.last());
                    } else {
                        try (XContentBuilder builder = channel.newBuilder(mediaType, true)) {
                            channel.sendChunk(XContentHttpChunk.from(r.toXContent(builder, ToXContent.EMPTY_PARAMS)));
                        }
                    }
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            })).subscribe();
        };

        return channel -> {
            if (channel instanceof StreamingRestChannel) {
                consumer.accept((StreamingRestChannel) channel);
            } else {
                final ActionRequestValidationException validationError = new ActionRequestValidationException();
                validationError.addValidationError("Unable to initiate request / response streaming over non-streaming channel");
                channel.sendResponse(new BytesRestResponse(channel, validationError));
            }
        };
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}
