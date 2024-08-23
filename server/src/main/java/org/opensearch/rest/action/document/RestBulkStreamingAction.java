/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.document;

import com.google.protobuf.ExperimentalApi;
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
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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
@ExperimentalApi
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
        final TimeValue batch_interval = request.paramAsTime("batch_interval", null);
        final int batch_size = request.paramAsInt("batch_size", 1); /* by default, batch size of 1 */

        if (batch_interval != null && batch_interval.duration() <= 0) {
            throw new IllegalArgumentException("The batch_interval value should be non-negative [" + batch_interval.millis() + "ms].");
        }

        if (batch_size <= 0) {
            throw new IllegalArgumentException("The batch_size value should be non-negative [" + batch_size + "].");
        }

        final StreamingRestChannelConsumer consumer = (channel) -> {
            final MediaType mediaType = request.getMediaType();

            // We prepare (and more importantly, validate) the templated BulkRequest instance: in case the parameters
            // are incorrect, we are going to fail the request immediately, instead of producing a possibly large amount
            // of failed chunks.
            FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
            BulkRequest prepareBulkRequest = Requests.bulkRequest();
            if (waitForActiveShards != null) {
                prepareBulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
            }

            prepareBulkRequest.timeout(timeout);
            prepareBulkRequest.setRefreshPolicy(refresh);

            // Set the content type and the status code before sending the response stream over
            channel.prepareResponse(RestStatus.OK, Map.of("Content-Type", List.of(mediaType.mediaTypeWithoutParameters())));

            // TODOs:
            // - eliminate serialization inefficiencies
            createBufferedFlux(batch_interval, batch_size, channel).zipWith(Flux.fromStream(Stream.generate(() -> {
                BulkRequest bulkRequest = Requests.bulkRequest();
                bulkRequest.waitForActiveShards(prepareBulkRequest.waitForActiveShards());
                bulkRequest.timeout(prepareBulkRequest.timeout());
                bulkRequest.setRefreshPolicy(prepareBulkRequest.getRefreshPolicy());
                return bulkRequest;
            }))).map(t -> {
                boolean isLast = false;
                final List<HttpChunk> chunks = t.getT1();
                final BulkRequest bulkRequest = t.getT2();

                for (final HttpChunk chunk : chunks) {
                    isLast |= chunk.isLast();
                    try (chunk) {
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
                }

                return Tuple.tuple(isLast, bulkRequest);
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
            })).onErrorComplete(ex -> {
                if (ex instanceof Error) {
                    return false;
                }
                try {
                    channel.sendResponse(new BytesRestResponse(channel, (Exception) ex));
                    return true;
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).subscribe();
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

    private Flux<List<HttpChunk>> createBufferedFlux(final TimeValue batch_interval, final int batch_size, StreamingRestChannel channel) {
        if (batch_interval != null) {
            return Flux.from(channel).bufferTimeout(batch_size, Duration.ofMillis(batch_interval.millis()));
        } else {
            return Flux.from(channel).buffer(batch_size);
        }
    }
}
