/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.OpenSearchReactorNetty4IntegTestCase;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpChunk;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.reactor.ReactorNetty4Plugin;
import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCounted;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Integration tests for streaming REST channels with tracing support.
 * Tests thread context restoration and proper handling of streaming responses
 * with the tracing infrastructure enabled.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class ReactorNetty4StreamingTracingIT extends OpenSearchReactorNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReactorNetty4Plugin.class, Netty4ModulePlugin.class, MockStreamingPlugin.class);
    }

    public static class MockStreamingPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            return List.of(new MockStreamingRestHandler());
        }
    }

    public static class MockStreamingRestHandler extends BaseRestHandler {
        @Override
        public String getName() {
            return "mock_streaming_tracing_handler";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/test/_stream"));
        }

        @Override
        public boolean supportsStreaming() {
            return true;
        }

        @Override
        public boolean supportsContentStream() {
            return true;
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            return channel -> {
                if (channel instanceof StreamingRestChannel) {
                    StreamingRestChannel streamingChannel = (StreamingRestChannel) channel;

                    Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(true);

                    Map<String, List<String>> headers = Map.of(
                        "Content-Type",
                        List.of("text/event-stream"),
                        "Cache-Control",
                        List.of("no-cache"),
                        "Connection",
                        List.of("keep-alive")
                    );
                    streamingChannel.prepareResponse(RestStatus.OK, headers);

                    Flux.from(streamingChannel).ofType(HttpChunk.class).collectList().flatMap(chunks -> {
                        try (ThreadContext.StoredContext ignored = supplier.get()) {

                            String opaqueId = request.header(Task.X_OPAQUE_ID);
                            streamingChannel.sendChunk(
                                createHttpChunk("data: {\"status\":\"streaming\",\"opaque_id\":\"" + opaqueId + "\"}\n\n", false)
                            );

                            final CompletableFuture<HttpChunk> future = new CompletableFuture<>();

                            Flux.just(
                                createHttpChunk("data: {\"content\":\"test chunk 1\"}\n\n", false),
                                createHttpChunk("data: {\"content\":\"test chunk 2\"}\n\n", false),
                                createHttpChunk("data: {\"content\":\"final chunk\",\"is_last\":true}\n\n", true)
                            )
                                .delayElements(Duration.ofMillis(100))
                                .doOnNext(streamingChannel::sendChunk)
                                .doOnComplete(() -> future.complete(createHttpChunk("", true)))
                                .doOnError(future::completeExceptionally)
                                .subscribe(); // Simulate streaming delay

                            return Mono.fromCompletionStage(future);
                        } catch (Exception e) {
                            return Mono.error(e);
                        }
                    }).doOnNext(streamingChannel::sendChunk).onErrorResume(ex -> {
                        try {
                            HttpChunk errorChunk = createHttpChunk("data: {\"error\":\"" + ex.getMessage() + "\"}\n\n", true);
                            streamingChannel.sendChunk(errorChunk);
                        } catch (Exception e) {
                            // Log error
                        }
                        return Mono.empty();
                    }).subscribe();
                }
            };
        }

        private HttpChunk createHttpChunk(String sseData, boolean isLast) {
            BytesReference bytesRef = BytesReference.fromByteBuffer(ByteBuffer.wrap(sseData.getBytes(StandardCharsets.UTF_8)));
            return new HttpChunk() {
                @Override
                public void close() {
                    if (bytesRef instanceof Releasable) {
                        ((Releasable) bytesRef).close();
                    }
                }

                @Override
                public boolean isLast() {
                    return isLast;
                }

                @Override
                public BytesReference content() {
                    return bytesRef;
                }
            };
        }
    }

    public void testStreamingWithTraceEnabled() throws Exception {
        ensureGreen();

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        List<Tuple<String, CharSequence>> requests = new ArrayList<>();
        requests.add(Tuple.tuple("/test/_stream", "dummy request body"));

        try (ReactorHttpClient nettyHttpClient = ReactorHttpClient.create(Settings.EMPTY)) {
            Collection<FullHttpResponse> singleResponse = nettyHttpClient.post(transportAddress.address(), requests);
            try {
                Assert.assertEquals(1, singleResponse.size());
                FullHttpResponse response = singleResponse.iterator().next();
                String responseBody = response.content().toString(CharsetUtil.UTF_8);
                Assert.assertEquals(200, response.status().code());
            } finally {
                singleResponse.forEach(ReferenceCounted::release);
            }
        }
    }
}
