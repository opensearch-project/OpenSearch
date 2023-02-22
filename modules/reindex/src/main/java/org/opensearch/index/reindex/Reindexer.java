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

package org.opensearch.index.reindex;

import java.util.Optional;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.client.ParentTaskAssigningClient;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.reindex.remote.RemoteScrollableHitSource;
import org.opensearch.index.reindex.spi.RemoteReindexExtension;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.requireNonNull;
import static org.opensearch.index.VersionType.INTERNAL;

public class Reindexer {

    private static final Logger logger = LogManager.getLogger(Reindexer.class);

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;
    private final ScriptService scriptService;
    private final ReindexSslConfig reindexSslConfig;
    private final Optional<RemoteReindexExtension> remoteExtension;

    Reindexer(
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool,
        ScriptService scriptService,
        ReindexSslConfig reindexSslConfig
    ) {
        this(clusterService, client, threadPool, scriptService, reindexSslConfig, Optional.empty());
    }

    Reindexer(
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool,
        ScriptService scriptService,
        ReindexSslConfig reindexSslConfig,
        Optional<RemoteReindexExtension> remoteExtension
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.reindexSslConfig = reindexSslConfig;
        this.remoteExtension = remoteExtension;
    }

    public void initTask(BulkByScrollTask task, ReindexRequest request, ActionListener<Void> listener) {
        BulkByScrollParallelizationHelper.initTaskState(task, request, client, listener);
    }

    public void execute(BulkByScrollTask task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        ActionListener<BulkByScrollResponse> remoteReindexActionListener = getRemoteReindexWrapperListener(listener, request);
        BulkByScrollParallelizationHelper.executeSlicedAction(
            task,
            request,
            ReindexAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(client, clusterService.localNode(), task);
                AsyncIndexBySearchAction searchAction = new AsyncIndexBySearchAction(
                    task,
                    logger,
                    assigningClient,
                    threadPool,
                    scriptService,
                    reindexSslConfig,
                    request,
                    remoteReindexActionListener,
                    getInterceptor(request)
                );
                searchAction.start();
            }
        );

    }

    private Optional<HttpRequestInterceptor> getInterceptor(ReindexRequest request) {
        if (request.getRemoteInfo() == null) {
            return Optional.empty();
        } else {
            return remoteExtension.map(x -> x.getInterceptorProvider())
                .flatMap(provider -> provider.getRestInterceptor(request, threadPool.getThreadContext()));
        }
    }

    private ActionListener<BulkByScrollResponse> getRemoteReindexWrapperListener(
        ActionListener<BulkByScrollResponse> listener,
        ReindexRequest reindexRequest
    ) {
        if (reindexRequest.getRemoteInfo() == null) {
            return listener;
        }
        if (remoteExtension.isPresent()) {
            return remoteExtension.get().getRemoteReindexActionListener(listener, reindexRequest);
        }
        logger.info("No extension found for remote reindex listener");
        return listener;
    }

    static RestClient buildRestClient(RemoteInfo remoteInfo, ReindexSslConfig sslConfig, long taskId, List<Thread> threadCollector) {
        return buildRestClient(remoteInfo, sslConfig, taskId, threadCollector, Optional.empty());
    }

    /**
     * Build the {@link RestClient} used for reindexing from remote clusters.
     *
     * @param remoteInfo connection information for the remote cluster
     * @param sslConfig configuration for potential outgoing HTTPS connections
     * @param taskId the id of the current task. This is added to the thread name for easier tracking
     * @param threadCollector a list in which we collect all the threads created by the client
     * @param restInterceptor an optional HttpRequestInterceptor
     */
    static RestClient buildRestClient(
        RemoteInfo remoteInfo,
        ReindexSslConfig sslConfig,
        long taskId,
        List<Thread> threadCollector,
        Optional<HttpRequestInterceptor> restInterceptor
    ) {
        Header[] clientHeaders = new Header[remoteInfo.getHeaders().size()];
        int i = 0;
        for (Map.Entry<String, String> header : remoteInfo.getHeaders().entrySet()) {
            clientHeaders[i++] = new BasicHeader(header.getKey(), header.getValue());
        }
        final HttpHost httpHost = new HttpHost(remoteInfo.getScheme(), remoteInfo.getHost(), remoteInfo.getPort());
        final RestClientBuilder builder = RestClient.builder(httpHost).setDefaultHeaders(clientHeaders).setRequestConfigCallback(c -> {
            c.setConnectTimeout(Timeout.ofMilliseconds(Math.toIntExact(remoteInfo.getConnectTimeout().millis())));
            c.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(remoteInfo.getSocketTimeout().millis())));
            return c;
        }).setHttpClientConfigCallback(c -> {
            // Enable basic auth if it is configured
            if (remoteInfo.getUsername() != null) {
                UsernamePasswordCredentials creds = new UsernamePasswordCredentials(
                    remoteInfo.getUsername(),
                    remoteInfo.getPassword().toCharArray()
                );
                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(new AuthScope(httpHost, null, "Basic"), creds);
                c.setDefaultCredentialsProvider(credentialsProvider);
            } else {
                restInterceptor.ifPresent(interceptor -> c.addRequestInterceptorLast(interceptor));
            }
            // Stick the task id in the thread name so we can track down tasks from stack traces
            AtomicInteger threads = new AtomicInteger();
            c.setThreadFactory(r -> {
                String name = "es-client-" + taskId + "-" + threads.getAndIncrement();
                Thread t = new Thread(r, name);
                threadCollector.add(t);
                return t;
            });
            // Limit ourselves to one reactor thread because for now the search process is single threaded.
            c.setIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());

            final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(sslConfig.getStrategy())
                .build();

            c.setConnectionManager(connectionManager);
            return c;
        });
        if (Strings.hasLength(remoteInfo.getPathPrefix()) && "/".equals(remoteInfo.getPathPrefix()) == false) {
            builder.setPathPrefix(remoteInfo.getPathPrefix());
        }
        return builder.build();
    }

    /**
     * Simple implementation of reindex using scrolling and bulk. There are tons
     * of optimizations that can be done on certain types of reindex requests
     * but this makes no attempt to do any of them so it can be as simple
     * possible.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<ReindexRequest, TransportReindexAction> {

        /**
         * List of threads created by this process. Usually actions don't create threads in OpenSearch. Instead they use the builtin
         * {@link ThreadPool}s. But reindex-from-remote uses OpenSearch's {@link RestClient} which doesn't use the
         * {@linkplain ThreadPool}s because it uses httpasyncclient. It'd be a ton of trouble to work around creating those threads. So
         * instead we let it create threads but we watch them carefully and assert that they are dead when the process is over.
         */
        private List<Thread> createdThreads = emptyList();

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient client,
            ThreadPool threadPool,
            ScriptService scriptService,
            ReindexSslConfig sslConfig,
            ReindexRequest request,
            ActionListener<BulkByScrollResponse> listener
        ) {
            this(task, logger, client, threadPool, scriptService, sslConfig, request, listener, Optional.empty());
        }

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient client,
            ThreadPool threadPool,
            ScriptService scriptService,
            ReindexSslConfig sslConfig,
            ReindexRequest request,
            ActionListener<BulkByScrollResponse> listener,
            Optional<HttpRequestInterceptor> interceptor
        ) {
            super(
                task,
                /*
                 * We only need the source version if we're going to use it when write and we only do that when the destination request uses
                 * external versioning.
                 */
                request.getDestination().versionType() != VersionType.INTERNAL,
                false,
                logger,
                client,
                threadPool,
                request,
                listener,
                scriptService,
                sslConfig,
                interceptor
            );
        }

        @Override
        protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy) {
            if (mainRequest.getRemoteInfo() != null) {
                RemoteInfo remoteInfo = mainRequest.getRemoteInfo();
                createdThreads = synchronizedList(new ArrayList<>());
                assert sslConfig != null : "Reindex ssl config must be set";
                RestClient restClient = buildRestClient(remoteInfo, sslConfig, task.getId(), createdThreads, this.interceptor);
                return new RemoteScrollableHitSource(
                    logger,
                    backoffPolicy,
                    threadPool,
                    worker::countSearchRetry,
                    this::onScrollResponse,
                    this::finishHim,
                    restClient,
                    remoteInfo.getQuery(),
                    mainRequest.getSearchRequest()
                );
            }
            return super.buildScrollableResultSource(backoffPolicy);
        }

        @Override
        protected void finishHim(
            Exception failure,
            List<BulkItemResponse.Failure> indexingFailures,
            List<ScrollableHitSource.SearchFailure> searchFailures,
            boolean timedOut
        ) {
            super.finishHim(failure, indexingFailures, searchFailures, timedOut);
            // A little extra paranoia so we log something if we leave any threads running
            for (Thread thread : createdThreads) {
                if (thread.isAlive()) {
                    assert false : "Failed to properly stop client thread [" + thread.getName() + "]";
                    logger.error("Failed to properly stop client thread [{}]", thread.getName());
                }
            }
        }

        @Override
        public BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                assert scriptService != null : "Script service must be set";
                return new Reindexer.AsyncIndexBySearchAction.ReindexScriptApplier(worker, scriptService, script, script.getParams());
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(ScrollableHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();

            // Copy the index from the request so we always write where it asked to write
            index.index(mainRequest.getDestination().index());

            /*
             * Internal versioning can just use what we copied from the destination request. Otherwise we assume we're using external
             * versioning and use the doc's version.
             */
            index.versionType(mainRequest.getDestination().versionType());
            if (index.versionType() == INTERNAL) {
                assert doc.getVersion() == -1 : "fetched version when we didn't have to";
                index.version(mainRequest.getDestination().version());
            } else {
                index.version(doc.getVersion());
            }

            // id and source always come from the found doc. Scripts can change them but they operate on the index request.
            index.id(doc.getId());

            // the source xcontent type and destination could be different
            final XContentType sourceXContentType = doc.getXContentType();
            final XContentType mainRequestXContentType = mainRequest.getDestination().getContentType();
            if (mainRequestXContentType != null && doc.getXContentType() != mainRequestXContentType) {
                // we need to convert
                try (
                    InputStream stream = doc.getSource().streamInput();
                    XContentParser parser = sourceXContentType.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream);
                    XContentBuilder builder = XContentBuilder.builder(mainRequestXContentType.xContent())
                ) {
                    parser.nextToken();
                    builder.copyCurrentStructure(parser);
                    index.source(BytesReference.bytes(builder), builder.contentType());
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "failed to convert hit from " + sourceXContentType + " to " + mainRequestXContentType,
                        e
                    );
                }
            } else {
                index.source(doc.getSource(), doc.getXContentType());
            }

            /*
             * The rest of the index request just has to be copied from the template. It may be changed later from scripts or the superclass
             * here on out operates on the index request rather than the template.
             */
            index.routing(mainRequest.getDestination().routing());
            index.setPipeline(mainRequest.getDestination().getPipeline());
            if (mainRequest.getDestination().opType() == DocWriteRequest.OpType.CREATE) {
                index.opType(mainRequest.getDestination().opType());
            }

            return wrap(index);
        }

        /**
         * Override the simple copy behavior to allow more fine grained control.
         */
        @Override
        protected void copyRouting(RequestWrapper<?> request, String routing) {
            String routingSpec = mainRequest.getDestination().routing();
            if (routingSpec == null) {
                super.copyRouting(request, routing);
                return;
            }
            if (routingSpec.startsWith("=")) {
                super.copyRouting(request, mainRequest.getDestination().routing().substring(1));
                return;
            }
            switch (routingSpec) {
                case "keep":
                    super.copyRouting(request, routing);
                    break;
                case "discard":
                    super.copyRouting(request, null);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported routing command");
            }
        }

        class ReindexScriptApplier extends ScriptApplier {

            ReindexScriptApplier(
                WorkerBulkByScrollTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params
            ) {
                super(taskWorker, scriptService, script, params);
            }

            /*
             * Methods below here handle script updating the index request. They try
             * to be pretty liberal with regards to types because script are often
             * dynamically typed.
             */

            @Override
            protected void scriptChangedIndex(RequestWrapper<?> request, Object to) {
                requireNonNull(to, "Can't reindex without a destination index!");
                request.setIndex(to.toString());
            }

            @Override
            protected void scriptChangedId(RequestWrapper<?> request, Object to) {
                request.setId(Objects.toString(to, null));
            }

            @Override
            protected void scriptChangedVersion(RequestWrapper<?> request, Object to) {
                if (to == null) {
                    request.setVersion(Versions.MATCH_ANY);
                    request.setVersionType(INTERNAL);
                } else {
                    request.setVersion(asLong(to, VersionFieldMapper.NAME));
                }
            }

            @Override
            protected void scriptChangedRouting(RequestWrapper<?> request, Object to) {
                request.setRouting(Objects.toString(to, null));
            }

            private long asLong(Object from, String name) {
                /*
                 * Stuffing a number into the map will have converted it to
                 * some Number.
                 * */
                Number fromNumber;
                try {
                    fromNumber = (Number) from;
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException(name + " may only be set to an int or a long but was [" + from + "]", e);
                }
                long l = fromNumber.longValue();
                // Check that we didn't round when we fetched the value.
                if (fromNumber.doubleValue() != l) {
                    throw new IllegalArgumentException(name + " may only be set to an int or a long but was [" + from + "]");
                }
                return l;
            }
        }
    }
}
