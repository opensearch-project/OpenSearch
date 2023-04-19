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

package org.opensearch.rest.action.document;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.reactivestreams.FlowAdapters;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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

    private final boolean allowExplicitIndex;

    public RestBulkStreamingAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(POST, "/_sbulk"),
                new Route(PUT, "/_sbulk"),
                new Route(POST, "/{index}/_sbulk"),
                new Route(PUT, "/{index}/_sbulk")
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

        return channel -> {
            // Set the content type and the status code before sending the stream over
            channel.prepareResponse(RestStatus.OK, Map.of("Content-Type", List.of(request.getMediaType().mediaTypeWithoutParameters())));

            Flux.from(FlowAdapters.toPublisher(channel)).map(chunk -> {
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
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }

                return Tuple.tuple(chunk, bulkRequest);
            }).flatMap(tuple -> {
                final CompletableFuture<BulkResponse> f = new CompletableFuture<>();

                if (tuple.v2().requests().isEmpty()) {
                    // this is the last request
                    f.complete(BulkResponse.EMPTY);
                } else {
                    client.bulk(tuple.v2(), new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse response) {
                            f.complete(response);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            f.completeExceptionally(e);
                        }
                    });

                    if (tuple.v1().isLast()) {
                        return Flux.just(f, CompletableFuture.completedFuture(BulkResponse.EMPTY));
                    }
                }

                return Mono.just(f);
            }).concatMap(f -> Mono.fromFuture(f).doOnNext(r -> {
                try {
                    if (r == BulkResponse.EMPTY) {
                        channel.sendChunk(XContentBuilder.NO_CONTENT);
                    } else {
                        // TODO: need to take response's XContentType
                        try (XContentBuilder builder = channel.newBuilder(request.getMediaType(), allowExplicitIndex)) {
                            channel.sendChunk(r.toXContent(builder, ToXContent.EMPTY_PARAMS));
                        }
                    }
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            })).subscribe();
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
