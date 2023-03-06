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

package org.opensearch.index.reindex.remote;

import org.apache.hc.core5.http.ContentTooLongException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.reindex.RejectAwareActionListener;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.common.unit.TimeValue.timeValueNanos;
import static org.opensearch.index.reindex.remote.RemoteResponseParsers.MAIN_ACTION_PARSER;
import static org.opensearch.index.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;

public class RemoteScrollableHitSource extends ScrollableHitSource {
    private final RestClient client;
    private final BytesReference query;
    private final SearchRequest searchRequest;
    Version remoteVersion;

    public RemoteScrollableHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail,
        RestClient client,
        BytesReference query,
        SearchRequest searchRequest
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.query = query;
        this.searchRequest = searchRequest;
        this.client = client;
    }

    @Override
    protected void doStart(RejectAwareActionListener<Response> searchListener) {
        lookupRemoteVersion(RejectAwareActionListener.withResponseHandler(searchListener, version -> {
            remoteVersion = version;
            execute(
                RemoteRequestBuilders.initialSearch(searchRequest, query, remoteVersion),
                RESPONSE_PARSER,
                RejectAwareActionListener.withResponseHandler(searchListener, r -> onStartResponse(searchListener, r))
            );
        }));
    }

    void lookupRemoteVersion(RejectAwareActionListener<Version> listener) {
        execute(new Request("GET", ""), MAIN_ACTION_PARSER, listener);
    }

    private void onStartResponse(RejectAwareActionListener<Response> searchListener, Response response) {
        if (Strings.hasLength(response.getScrollId()) && response.getHits().isEmpty()) {
            logger.debug("First response looks like a scan response. Jumping right to the second. scroll=[{}]", response.getScrollId());
            doStartNextScroll(response.getScrollId(), timeValueMillis(0), searchListener);
        } else {
            searchListener.onResponse(response);
        }
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        TimeValue keepAlive = timeValueNanos(searchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos());
        execute(RemoteRequestBuilders.scroll(scrollId, keepAlive, remoteVersion), RESPONSE_PARSER, searchListener);
    }

    @Override
    protected void clearScroll(String scrollId, Runnable onCompletion) {
        client.performRequestAsync(RemoteRequestBuilders.clearScroll(scrollId, remoteVersion), new ResponseListener() {
            @Override
            public void onSuccess(org.opensearch.client.Response response) {
                logger.debug("Successfully cleared [{}]", scrollId);
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logFailure(e);
                onCompletion.run();
            }

            private void logFailure(Exception e) {
                if (e instanceof ResponseException) {
                    ResponseException re = (ResponseException) e;
                    if (remoteVersion.before(Version.fromId(2000099)) && re.getResponse().getStatusLine().getStatusCode() == 404) {
                        logger.debug(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "Failed to clear scroll [{}] from pre-2.0 OpenSearch. This is normal if the request terminated "
                                    + "normally as the scroll has already been cleared automatically.",
                                scrollId
                            ),
                            e
                        );
                        return;
                    }
                }
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to clear scroll [{}]", scrollId), e);
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        /* This is called on the RestClient's thread pool and attempting to close the client on its
         * own threadpool causes it to fail to close. So we always shutdown the RestClient
         * asynchronously on a thread in Elasticsearch's generic thread pool. */
        threadPool.generic().submit(() -> {
            try {
                client.close();
                logger.debug("Shut down remote connection");
            } catch (IOException e) {
                logger.error("Failed to shutdown the remote connection", e);
            } finally {
                onCompletion.run();
            }
        });
    }

    private <T> void execute(
        Request request,
        BiFunction<XContentParser, XContentType, T> parser,
        RejectAwareActionListener<? super T> listener
    ) {
        // Preserve the thread context so headers survive after the call
        java.util.function.Supplier<ThreadContext.StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        try {
            client.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(org.opensearch.client.Response response) {
                    // Restore the thread context to get the precious headers
                    try (ThreadContext.StoredContext ctx = contextSupplier.get()) {
                        assert ctx != null; // eliminates compiler warning
                        T parsedResponse;
                        try {
                            HttpEntity responseEntity = response.getEntity();
                            InputStream content = responseEntity.getContent();
                            XContentType xContentType = null;
                            if (responseEntity.getContentType() != null) {
                                final String mimeType = ContentType.parse(responseEntity.getContentType()).getMimeType();
                                xContentType = XContentType.fromMediaType(mimeType);
                            }
                            if (xContentType == null) {
                                try {
                                    logger.debug("Response didn't include Content-Type: " + bodyMessage(response.getEntity()));
                                    throw new OpenSearchException(
                                        "Response didn't include supported Content-Type, remote is likely not an OpenSearch instance"
                                    );
                                } catch (IOException e) {
                                    OpenSearchException ee = new OpenSearchException("Error extracting body from response");
                                    ee.addSuppressed(e);
                                    throw ee;
                                }
                            }
                            // EMPTY is safe here because we don't call namedObject
                            try (
                                XContentParser xContentParser = xContentType.xContent()
                                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, content)
                            ) {
                                parsedResponse = parser.apply(xContentParser, xContentType);
                            } catch (XContentParseException e) {
                                /* Because we're streaming the response we can't get a copy of it here. The best we can do is hint that it
                                 * is totally wrong and we're probably not talking to Elasticsearch. */
                                throw new OpenSearchException("Error parsing the response, remote is likely not an OpenSearch instance", e);
                            }
                        } catch (IOException e) {
                            throw new OpenSearchException("Error deserializing response, remote is likely not an OpenSearch instance", e);
                        }
                        listener.onResponse(parsedResponse);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try (ThreadContext.StoredContext ctx = contextSupplier.get()) {
                        assert ctx != null; // eliminates compiler warning
                        if (e instanceof ResponseException) {
                            ResponseException re = (ResponseException) e;
                            int statusCode = re.getResponse().getStatusLine().getStatusCode();
                            e = wrapExceptionToPreserveStatus(statusCode, re.getResponse().getEntity(), re);
                            if (RestStatus.TOO_MANY_REQUESTS.getStatus() == statusCode) {
                                listener.onRejection(e);
                                return;
                            }
                        } else if (e instanceof ContentTooLongException) {
                            e = new IllegalArgumentException(
                                "Remote responded with a chunk that was too large. Use a smaller batch size.",
                                e
                            );
                        }
                        listener.onFailure(e);
                    }
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Wrap the ResponseException in an exception that'll preserve its status code if possible so we can send it back to the user. We might
     * not have a constant for the status code so in that case we just use 500 instead. We also extract make sure to include the response
     * body in the message so the user can figure out *why* the remote OpenSearch service threw the error back to us.
     */
    static OpenSearchStatusException wrapExceptionToPreserveStatus(int statusCode, @Nullable HttpEntity entity, Exception cause) {
        RestStatus status = RestStatus.fromCode(statusCode);
        String messagePrefix = "";
        if (status == null) {
            messagePrefix = "Couldn't extract status [" + statusCode + "]. ";
            status = RestStatus.INTERNAL_SERVER_ERROR;
        }
        try {
            return new OpenSearchStatusException(messagePrefix + bodyMessage(entity), status, cause);
        } catch (IOException ioe) {
            OpenSearchStatusException e = new OpenSearchStatusException(messagePrefix + "Failed to extract body.", status, cause);
            e.addSuppressed(ioe);
            return e;
        }
    }

    private static String bodyMessage(@Nullable HttpEntity entity) throws IOException {
        if (entity == null) {
            return "No error body.";
        } else {
            try {
                return "body=" + EntityUtils.toString(entity);
            } catch (final ParseException ex) {
                throw new IOException(ex);
            }
        }
    }
}
