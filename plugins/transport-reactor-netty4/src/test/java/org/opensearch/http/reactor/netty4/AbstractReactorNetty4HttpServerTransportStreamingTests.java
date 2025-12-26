/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentHttpChunk;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults;
import static org.hamcrest.CoreMatchers.instanceOf;

public abstract class AbstractReactorNetty4HttpServerTransportStreamingTests extends OpenSearchTestCase {
    private static final Function<String, ToXContent> XCONTENT_CONVERTER = str -> new ToXContent() {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return builder.startObject().field("doc", str).endObject();
        }
    };

    protected HttpServerTransport.Dispatcher createStreamingDispatcher(ThreadPool threadPool, String url, String responseString) {
        return dispatcherBuilderWithDefaults().withDispatchHandler((uri, rawPath, method, params) -> Optional.of(new RestHandler() {
            @Override
            public boolean supportsStreaming() {
                return true;
            }

            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                logger.error("--> Unexpected request [{}]", request.uri());
                throw new AssertionError();
            }
        })).withDispatchRequest((request, channel, threadContext) -> {
            if (url.equals(request.uri())) {
                assertThat(channel, instanceOf(StreamingRestChannel.class));
                final StreamingRestChannel streamingChannel = (StreamingRestChannel) channel;

                // Await at most 5 seconds till channel is ready for writing the response stream, fail otherwise
                final Mono<?> ready = Mono.fromRunnable(() -> {
                    while (!streamingChannel.isWritable()) {
                        Thread.onSpinWait();
                    }
                }).timeout(Duration.ofSeconds(5));

                threadPool.executor(ThreadPool.Names.WRITE).execute(() -> Flux.concat(Flux.fromArray(newChunks(responseString)).map(e -> {
                    try (XContentBuilder builder = channel.newBuilder(XContentType.JSON, true)) {
                        return XContentHttpChunk.from(e.toXContent(builder, ToXContent.EMPTY_PARAMS));
                    } catch (final IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }), Mono.just(XContentHttpChunk.last())).delaySubscription(ready).subscribe(streamingChannel::sendChunk, null, () -> {
                    if (channel.bytesOutput() instanceof Releasable) {
                        ((Releasable) channel.bytesOutput()).close();
                    }
                }));
            } else {
                logger.error("--> Unexpected successful uri [{}]", request.uri());
                throw new AssertionError();
            }
        }).build();
    }

    protected static ToXContent[] newChunks(final String responseString) {
        final ToXContent[] chunks = new ToXContent[responseString.length() / 16];

        for (int chunk = 0; chunk < responseString.length(); chunk += 16) {
            chunks[chunk / 16] = XCONTENT_CONVERTER.apply(responseString.substring(chunk, chunk + 16));
        }

        return chunks;
    }

}
