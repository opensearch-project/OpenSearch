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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.network;

import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocationCommand;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.RawTaskStatus;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A module to handle registering and binding all network related classes.
 *
 * @opensearch.internal
 */
public final class NetworkModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String HTTP_TYPE_KEY = "http.type";
    public static final String HTTP_TYPE_DEFAULT_KEY = "http.type.default";
    public static final String TRANSPORT_TYPE_DEFAULT_KEY = "transport.type.default";
    public static final String TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_KEY = "transport.ssl.enforce_hostname_verification";
    public static final String TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME_KEY = "transport.ssl.resolve_hostname";
    public static final String TRANSPORT_SSL_DUAL_MODE_ENABLED_KEY = "transport.ssl.dual_mode.enabled";

    public static final Setting<String> TRANSPORT_DEFAULT_TYPE_SETTING = Setting.simpleString(
        TRANSPORT_TYPE_DEFAULT_KEY,
        Property.NodeScope
    );
    public static final Setting<String> HTTP_DEFAULT_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_DEFAULT_KEY, Property.NodeScope);
    public static final Setting<String> HTTP_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_KEY, Property.NodeScope);
    public static final Setting<String> TRANSPORT_TYPE_SETTING = Setting.simpleString(TRANSPORT_TYPE_KEY, Property.NodeScope);

    public static final Setting<Boolean> TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION = Setting.boolSetting(
        TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_KEY,
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME = Setting.boolSetting(
        TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME_KEY,
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> TRANSPORT_SSL_DUAL_MODE_ENABLED = Setting.boolSetting(
        TRANSPORT_SSL_DUAL_MODE_ENABLED_KEY,
        false,
        Property.NodeScope
    );

    private final Settings settings;

    private static final List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
    private static final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

    static {
        registerAllocationCommand(
            CancelAllocationCommand::new,
            CancelAllocationCommand::fromXContent,
            CancelAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            MoveAllocationCommand::new,
            MoveAllocationCommand::fromXContent,
            MoveAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            AllocateReplicaAllocationCommand::new,
            AllocateReplicaAllocationCommand::fromXContent,
            AllocateReplicaAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            AllocateEmptyPrimaryAllocationCommand::new,
            AllocateEmptyPrimaryAllocationCommand::fromXContent,
            AllocateEmptyPrimaryAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            AllocateStalePrimaryAllocationCommand::new,
            AllocateStalePrimaryAllocationCommand::fromXContent,
            AllocateStalePrimaryAllocationCommand.COMMAND_NAME_FIELD
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, ReplicationTask.Status.NAME, ReplicationTask.Status::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, ResyncTask.Status.NAME, ResyncTask.Status::new));
    }

    private final Map<String, Supplier<Transport>> transportFactories = new HashMap<>();
    private final Map<String, Supplier<HttpServerTransport>> transportHttpFactories = new HashMap<>();
    private final List<TransportInterceptor> transportInterceptors = new ArrayList<>();

    /**
     * Creates a network module that custom networking classes can be plugged into.
     * @param settings The settings for the node
     */
    public NetworkModule(
        Settings settings,
        List<NetworkPlugin> plugins,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        Tracer tracer,
        List<TransportInterceptor> transportInterceptors,
        Collection<SecureSettingsFactory> secureSettingsFactories
    ) {
        this.settings = settings;

        final Collection<SecureTransportSettingsProvider> secureTransportSettingsProviders = secureSettingsFactories.stream()
            .map(p -> p.getSecureTransportSettingsProvider(settings))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        if (secureTransportSettingsProviders.size() > 1) {
            throw new IllegalArgumentException(
                "there is more than one secure transport settings provider: " + secureTransportSettingsProviders
            );
        }

        final Collection<SecureHttpTransportSettingsProvider> secureHttpTransportSettingsProviders = secureSettingsFactories.stream()
            .map(p -> p.getSecureHttpTransportSettingsProvider(settings))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        if (secureHttpTransportSettingsProviders.size() > 1) {
            throw new IllegalArgumentException(
                "there is more than one secure HTTP transport settings provider: " + secureHttpTransportSettingsProviders
            );
        }

        for (NetworkPlugin plugin : plugins) {
            Map<String, Supplier<HttpServerTransport>> httpTransportFactory = plugin.getHttpTransports(
                settings,
                threadPool,
                bigArrays,
                pageCacheRecycler,
                circuitBreakerService,
                xContentRegistry,
                networkService,
                dispatcher,
                clusterSettings,
                tracer
            );
            for (Map.Entry<String, Supplier<HttpServerTransport>> entry : httpTransportFactory.entrySet()) {
                registerHttpTransport(entry.getKey(), entry.getValue());
            }

            Map<String, Supplier<Transport>> transportFactory = plugin.getTransports(
                settings,
                threadPool,
                pageCacheRecycler,
                circuitBreakerService,
                namedWriteableRegistry,
                networkService,
                tracer
            );
            for (Map.Entry<String, Supplier<Transport>> entry : transportFactory.entrySet()) {
                registerTransport(entry.getKey(), entry.getValue());
            }

            // Register any HTTP secure transports if available
            if (secureHttpTransportSettingsProviders.isEmpty() == false) {
                final SecureHttpTransportSettingsProvider secureSettingProvider = secureHttpTransportSettingsProviders.iterator().next();

                final Map<String, Supplier<HttpServerTransport>> secureHttpTransportFactory = plugin.getSecureHttpTransports(
                    settings,
                    threadPool,
                    bigArrays,
                    pageCacheRecycler,
                    circuitBreakerService,
                    xContentRegistry,
                    networkService,
                    dispatcher,
                    clusterSettings,
                    secureSettingProvider,
                    tracer
                );
                for (Map.Entry<String, Supplier<HttpServerTransport>> entry : secureHttpTransportFactory.entrySet()) {
                    registerHttpTransport(entry.getKey(), entry.getValue());
                }
            }

            // Register any secure transports if available
            if (secureTransportSettingsProviders.isEmpty() == false) {
                final SecureTransportSettingsProvider secureSettingProvider = secureTransportSettingsProviders.iterator().next();

                final Map<String, Supplier<Transport>> secureTransportFactory = plugin.getSecureTransports(
                    settings,
                    threadPool,
                    pageCacheRecycler,
                    circuitBreakerService,
                    namedWriteableRegistry,
                    networkService,
                    secureSettingProvider,
                    tracer
                );
                for (Map.Entry<String, Supplier<Transport>> entry : secureTransportFactory.entrySet()) {
                    registerTransport(entry.getKey(), entry.getValue());
                }
            }

            List<TransportInterceptor> pluginTransportInterceptors = plugin.getTransportInterceptors(
                namedWriteableRegistry,
                threadPool.getThreadContext()
            );
            for (TransportInterceptor interceptor : pluginTransportInterceptors) {
                registerTransportInterceptor(interceptor);
            }
        }
        // Adding last because interceptors are triggered from last to first order from the list
        if (transportInterceptors != null) {
            transportInterceptors.forEach(this::registerTransportInterceptor);
        }
    }

    /** Adds a transport implementation that can be selected by setting {@link #TRANSPORT_TYPE_KEY}. */
    private void registerTransport(String key, Supplier<Transport> factory) {
        if (transportFactories.putIfAbsent(key, factory) != null) {
            throw new IllegalArgumentException("transport for name: " + key + " is already registered");
        }
    }

    /** Adds an http transport implementation that can be selected by setting {@link #HTTP_TYPE_KEY}. */
    // TODO: we need another name than "http transport"....so confusing with transportClient...
    private void registerHttpTransport(String key, Supplier<HttpServerTransport> factory) {
        if (transportHttpFactories.putIfAbsent(key, factory) != null) {
            throw new IllegalArgumentException("transport for name: " + key + " is already registered");
        }
    }

    /**
     * Register an allocation command.
     * <p>
     * This lives here instead of the more aptly named ClusterModule because the Transport client needed these to be registered.
     * </p>
     * @param reader the reader to read it from a stream
     * @param parser the parser to read it from XContent
     * @param commandName the names under which the command should be parsed. The {@link ParseField#getPreferredName()} is special because
     *        it is the name under which the command's reader is registered.
     */
    private static <T extends AllocationCommand> void registerAllocationCommand(
        Writeable.Reader<T> reader,
        CheckedFunction<XContentParser, T, IOException> parser,
        ParseField commandName
    ) {
        namedXContents.add(new NamedXContentRegistry.Entry(AllocationCommand.class, commandName, parser));
        namedWriteables.add(new NamedWriteableRegistry.Entry(AllocationCommand.class, commandName.getPreferredName(), reader));
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.unmodifiableList(namedWriteables);
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Collections.unmodifiableList(namedXContents);
    }

    public Supplier<HttpServerTransport> getHttpServerTransportSupplier() {
        final String name;
        if (HTTP_TYPE_SETTING.exists(settings)) {
            name = HTTP_TYPE_SETTING.get(settings);
        } else {
            name = HTTP_DEFAULT_TYPE_SETTING.get(settings);
        }
        final Supplier<HttpServerTransport> factory = transportHttpFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("Unsupported http.type [" + name + "]");
        }
        return factory;
    }

    public Supplier<Transport> getTransportSupplier() {
        final String name;
        if (TRANSPORT_TYPE_SETTING.exists(settings)) {
            name = TRANSPORT_TYPE_SETTING.get(settings);
        } else {
            name = TRANSPORT_DEFAULT_TYPE_SETTING.get(settings);
        }
        final Supplier<Transport> factory = transportFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("Unsupported transport.type [" + name + "]");
        }
        return factory;
    }

    /**
     * Registers a new {@link TransportInterceptor}
     */
    private void registerTransportInterceptor(TransportInterceptor interceptor) {
        this.transportInterceptors.add(Objects.requireNonNull(interceptor, "interceptor must not be null"));
    }

    /**
     * Returns a composite {@link TransportInterceptor} containing all registered interceptors
     * @see #registerTransportInterceptor(TransportInterceptor)
     */
    public TransportInterceptor getTransportInterceptor() {
        return new CompositeTransportInterceptor(this.transportInterceptors);
    }

    static final class CompositeTransportInterceptor implements TransportInterceptor {
        final List<TransportInterceptor> transportInterceptors;

        private CompositeTransportInterceptor(List<TransportInterceptor> transportInterceptors) {
            this.transportInterceptors = new ArrayList<>(transportInterceptors);
        }

        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
            String action,
            String executor,
            boolean forceExecution,
            TransportRequestHandler<T> actualHandler
        ) {
            for (TransportInterceptor interceptor : this.transportInterceptors) {
                actualHandler = interceptor.interceptHandler(action, executor, forceExecution, actualHandler);
            }
            return actualHandler;
        }

        /**
         * Intercept the transport action and perform admission control if applicable
         * @param action The action the request handler is associated with
         * @param executor The executor the request handling will be executed on
         * @param forceExecution Force execution on the executor queue and never reject it
         * @param actualHandler The handler itself that implements the request handling
         * @param admissionControlActionType Admission control based on resource usage limits of provided action type
         * @return returns the actual TransportRequestHandler after intercepting all previous handlers
         * @param <T> transport request type
         */
        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
            String action,
            String executor,
            boolean forceExecution,
            TransportRequestHandler<T> actualHandler,
            AdmissionControlActionType admissionControlActionType
        ) {
            for (TransportInterceptor interceptor : this.transportInterceptors) {
                actualHandler = interceptor.interceptHandler(action, executor, forceExecution, actualHandler, admissionControlActionType);
            }
            return actualHandler;
        }

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            for (TransportInterceptor interceptor : this.transportInterceptors) {
                sender = interceptor.interceptSender(sender);
            }
            return sender;
        }
    }

}
