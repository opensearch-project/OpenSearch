/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class IdentityPlugin extends Plugin implements ActionPlugin {
    private volatile Logger log = LogManager.getLogger(this.getClass());
    private volatile Settings settings;

    private volatile Path configPath;
    private volatile SecurityFilter sf;
    private volatile ThreadPool threadPool;
    private volatile ClusterService cs;
    private volatile Client localClient;
    private volatile NamedXContentRegistry namedXContentRegistry = null;

    @SuppressWarnings("removal")
    public IdentityPlugin(final Settings settings, final Path configPath) {
        this.configPath = configPath;

        if(this.configPath != null) {
            log.info("OpenSearch Config path is {}", this.configPath.toAbsolutePath());
        } else {
            log.info("OpenSearch Config path is not set");
        }

        this.settings = settings;
    }

//    @Override
//    public UnaryOperator<RestHandler> getRestHandlerWrapper(final ThreadContext threadContext) {
//
//        if(client || disabled || SSLConfig.isSslOnlyMode()) {
//            return (rh) -> rh;
//        }
//
//        return (rh) -> securityRestHandler.wrap(rh, adminDns);
//    }

    @Override
    public List<ActionFilter> getActionFilters() {
        List<ActionFilter> filters = new ArrayList<>(1);
        filters.add(Objects.requireNonNull(sf));
        return filters;
    }

//    @Override
//    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
//        List<TransportInterceptor> interceptors = new ArrayList<TransportInterceptor>(1);
//
//        if (!client && !disabled && !SSLConfig.isSslOnlyMode()) {
//            interceptors.add(new TransportInterceptor() {
//
//                @Override
//                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
//                                                                                                boolean forceExecution, TransportRequestHandler<T> actualHandler) {
//
//                    return new TransportRequestHandler<T>() {
//
//                        @Override
//                        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
//                            si.getHandler(action, actualHandler).messageReceived(request, channel, task);
//                        }
//                    };
//
//                }
//
//                @Override
//                public AsyncSender interceptSender(AsyncSender sender) {
//
//                    return new AsyncSender() {
//
//                        @Override
//                        public <T extends TransportResponse> void sendRequest(Connection connection, String action,
//                                                                              TransportRequest request, TransportRequestOptions options, TransportResponseHandler<T> handler) {
//                            si.sendRequestDecorate(sender, connection, action, request, options, handler);
//                        }
//                    };
//                }
//            });
//        }
//
//        return interceptors;
//    }


    @Override
    public Collection<Object> createComponents(Client localClient, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
                                               Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {


        this.threadPool = threadPool;
        this.cs = clusterService;
        this.localClient = localClient;

        final List<Object> components = new ArrayList<Object>();

        sf = new SecurityFilter(localClient, settings, threadPool, cs);

        return components;

    }
}
