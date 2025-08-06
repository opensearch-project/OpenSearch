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

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Strategy to connect to remote nodes
 *
 * @opensearch.internal
 */
public abstract class RemoteConnectionStrategy implements TransportConnectionListener, Closeable {

    /**
     * Strategy to connect to remote nodes
     *
    * @opensearch.api
    */
    @PublicApi(since = "1.0.0")
    public enum ConnectionStrategy {
        SNIFF(
            SniffConnectionStrategy.CHANNELS_PER_CONNECTION,
            SniffConnectionStrategy::enablementSettings,
            SniffConnectionStrategy::infoReader
        ) {
            @Override
            public String toString() {
                return "sniff";
            }
        },
        PROXY(
            ProxyConnectionStrategy.CHANNELS_PER_CONNECTION,
            ProxyConnectionStrategy::enablementSettings,
            ProxyConnectionStrategy::infoReader
        ) {
            @Override
            public String toString() {
                return "proxy";
            }
        };

        private final int numberOfChannels;
        private final Supplier<Stream<Setting.AffixSetting<?>>> enablementSettings;
        private final Supplier<Writeable.Reader<RemoteConnectionInfo.ModeInfo>> reader;

        ConnectionStrategy(
            int numberOfChannels,
            Supplier<Stream<Setting.AffixSetting<?>>> enablementSettings,
            Supplier<Writeable.Reader<RemoteConnectionInfo.ModeInfo>> reader
        ) {
            this.numberOfChannels = numberOfChannels;
            this.enablementSettings = enablementSettings;
            this.reader = reader;
        }

        public int getNumberOfChannels() {
            return numberOfChannels;
        }

        public Supplier<Stream<Setting.AffixSetting<?>>> getEnablementSettings() {
            return enablementSettings;
        }

        public Writeable.Reader<RemoteConnectionInfo.ModeInfo> getReader() {
            return reader.get();
        }
    }

    public static final Setting.AffixSetting<ConnectionStrategy> REMOTE_CONNECTION_MODE = Setting.affixKeySetting(
        "cluster.remote.",
        "mode",
        key -> new Setting<>(
            key,
            ConnectionStrategy.SNIFF.name(),
            value -> ConnectionStrategy.valueOf(value.toUpperCase(Locale.ROOT)),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    );

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<Integer> REMOTE_MAX_PENDING_CONNECTION_LISTENERS = Setting.intSetting(
        "cluster.remote.max_pending_connection_listeners",
        1000,
        Setting.Property.NodeScope
    );

    private final int maxPendingConnectionListeners;

    protected final Logger logger = LogManager.getLogger(getClass());

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object mutex = new Object();
    private List<ActionListener<Void>> listeners = new ArrayList<>();

    protected final TransportService transportService;
    protected final RemoteConnectionManager connectionManager;
    protected final String clusterAlias;

    RemoteConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings
    ) {
        this.clusterAlias = clusterAlias;
        this.transportService = transportService;
        this.connectionManager = connectionManager;
        this.maxPendingConnectionListeners = REMOTE_MAX_PENDING_CONNECTION_LISTENERS.get(settings);
        connectionManager.addListener(this);
    }

    static ConnectionProfile buildConnectionProfile(String clusterAlias, Settings settings) {
        ConnectionStrategy mode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder().setConnectTimeout(
            TransportSettings.CONNECT_TIMEOUT.get(settings)
        )
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setCompressionEnabled(RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .setPingInterval(RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .addConnections(
                0,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.STATE,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.STREAM
            )
            .addConnections(mode.numberOfChannels, TransportRequestOptions.Type.REG);
        return builder.build();
    }

    static RemoteConnectionStrategy buildStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings
    ) {
        ConnectionStrategy mode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        switch (mode) {
            case SNIFF:
                return new SniffConnectionStrategy(clusterAlias, transportService, connectionManager, settings);
            case PROXY:
                return new ProxyConnectionStrategy(clusterAlias, transportService, connectionManager, settings);
            default:
                throw new AssertionError("Invalid connection strategy" + mode);
        }
    }

    static Set<String> getRemoteClusters(Settings settings) {
        final Stream<Setting.AffixSetting<?>> enablementSettings = Arrays.stream(ConnectionStrategy.values())
            .flatMap(strategy -> strategy.getEnablementSettings().get());
        return enablementSettings.flatMap(s -> getClusterAlias(settings, s)).collect(Collectors.toSet());
    }

    public static boolean isConnectionEnabled(String clusterAlias, Settings settings) {
        ConnectionStrategy mode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        if (mode.equals(ConnectionStrategy.SNIFF)) {
            List<String> seeds = SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings);
            return seeds.isEmpty() == false;
        } else {
            String address = ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(settings);
            return Strings.isEmpty(address) == false;
        }
    }

    @SuppressWarnings("unchecked")
    public static boolean isConnectionEnabled(String clusterAlias, Map<Setting<?>, Object> settings) {
        ConnectionStrategy mode = (ConnectionStrategy) settings.get(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias));
        if (mode.equals(ConnectionStrategy.SNIFF)) {
            List<String> seeds = (List<String>) settings.get(
                SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias)
            );
            return seeds.isEmpty() == false;
        } else {
            String address = (String) settings.get(ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias));
            return Strings.isEmpty(address) == false;
        }
    }

    private static <T> Stream<String> getClusterAlias(Settings settings, Setting.AffixSetting<T> affixSetting) {
        Stream<Setting<T>> allConcreteSettings = affixSetting.getAllConcreteSettings(settings);
        return allConcreteSettings.map(affixSetting::getNamespace);
    }

    static InetSocketAddress parseConfiguredAddress(String configuredAddress) {
        final String host = parseHost(configuredAddress);
        final int port = parsePort(configuredAddress);
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("unknown host [" + host + "]", e);
        }
        return new InetSocketAddress(hostAddress, port);
    }

    static String parseHost(final String configuredAddress) {
        return configuredAddress.substring(0, indexOfPortSeparator(configuredAddress));
    }

    static int parsePort(String remoteHost) {
        try {
            int port = Integer.parseInt(remoteHost.substring(indexOfPortSeparator(remoteHost) + 1));
            if (port <= 0) {
                throw new IllegalArgumentException("port number must be > 0 but was: [" + port + "]");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse port", e);
        }
    }

    private static int indexOfPortSeparator(String remoteHost) {
        int portSeparator = remoteHost.lastIndexOf(':'); // in case we have a IPv6 address ie. [::1]:9300
        if (portSeparator == -1 || portSeparator == remoteHost.length()) {
            throw new IllegalArgumentException("remote hosts need to be configured as [host:port], found [" + remoteHost + "] instead");
        }
        return portSeparator;
    }

    /**
     * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
     * be queued or rejected and failed.
     */
    void connect(ActionListener<Void> connectListener) {
        boolean runConnect = false;
        final ActionListener<Void> listener = ContextPreservingActionListener.wrapPreservingContext(
            connectListener,
            transportService.getThreadPool().getThreadContext()
        );
        boolean closed;
        synchronized (mutex) {
            closed = this.closed.get();
            if (closed) {
                assert listeners.isEmpty();
            } else {
                if (listeners.size() >= maxPendingConnectionListeners) {
                    assert listeners.size() == maxPendingConnectionListeners;
                    listener.onFailure(new OpenSearchRejectedExecutionException("connect listener queue is full"));
                    return;
                } else {
                    listeners.add(listener);
                }
                runConnect = listeners.size() == 1;
            }
        }
        if (closed) {
            connectListener.onFailure(new AlreadyClosedException("connect handler is already closed"));
            return;
        }
        if (runConnect) {
            ExecutorService executor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
            executor.submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    ActionListener.onFailure(getAndClearListeners(), e);
                }

                @Override
                protected void doRun() {
                    connectImpl(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            ActionListener.onResponse(getAndClearListeners(), aVoid);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            ActionListener.onFailure(getAndClearListeners(), e);
                        }
                    });
                }
            });
        }
    }

    boolean shouldRebuildConnection(Settings newSettings) {
        ConnectionStrategy newMode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        if (newMode.equals(strategyType()) == false) {
            return true;
        } else {
            Boolean compressionEnabled = RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias)
                .get(newSettings);
            TimeValue pingSchedule = RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace(clusterAlias)
                .get(newSettings);

            ConnectionProfile oldProfile = connectionManager.getConnectionProfile();
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder(oldProfile);
            builder.setCompressionEnabled(compressionEnabled);
            builder.setPingInterval(pingSchedule);
            ConnectionProfile newProfile = builder.build();
            return connectionProfileChanged(oldProfile, newProfile) || strategyMustBeRebuilt(newSettings);
        }
    }

    protected abstract boolean strategyMustBeRebuilt(Settings newSettings);

    protected abstract ConnectionStrategy strategyType();

    @Override
    public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
        if (shouldOpenMoreConnections()) {
            // try to reconnect and fill up the slot of the disconnected node
            connect(
                ActionListener.wrap(
                    ignore -> logger.trace("[{}] successfully connected after disconnect of {}", clusterAlias, node),
                    e -> logger.debug(
                        () -> new ParameterizedMessage("[{}] failed to connect after disconnect of {}", clusterAlias, node),
                        e
                    )
                )
            );
        }
    }

    @Override
    public void close() {
        final List<ActionListener<Void>> toNotify;
        synchronized (mutex) {
            if (closed.compareAndSet(false, true)) {
                connectionManager.removeListener(this);
                toNotify = listeners;
                listeners = Collections.emptyList();
            } else {
                toNotify = Collections.emptyList();
            }
        }
        ActionListener.onFailure(toNotify, new AlreadyClosedException("connect handler is already closed"));
    }

    public boolean isClosed() {
        return closed.get();
    }

    // for testing only
    boolean assertNoRunningConnections() {
        synchronized (mutex) {
            assert listeners.isEmpty();
        }
        return true;
    }

    protected abstract boolean shouldOpenMoreConnections();

    protected abstract void connectImpl(ActionListener<Void> listener);

    protected abstract RemoteConnectionInfo.ModeInfo getModeInfo();

    private List<ActionListener<Void>> getAndClearListeners() {
        final List<ActionListener<Void>> result;
        synchronized (mutex) {
            if (listeners.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = listeners;
                listeners = new ArrayList<>();
            }
        }
        return result;
    }

    private boolean connectionProfileChanged(ConnectionProfile oldProfile, ConnectionProfile newProfile) {
        return Objects.equals(oldProfile.getCompressionEnabled(), newProfile.getCompressionEnabled()) == false
            || Objects.equals(oldProfile.getPingInterval(), newProfile.getPingInterval()) == false;
    }

    /**
     * Internal strategy validation object
     *
     * @opensearch.internal
     */
    static class StrategyValidator<T> implements Setting.Validator<T> {

        private final String key;
        private final ConnectionStrategy expectedStrategy;
        private final String namespace;
        private final Consumer<T> valueChecker;

        StrategyValidator(String namespace, String key, ConnectionStrategy expectedStrategy) {
            this(namespace, key, expectedStrategy, (v) -> {});
        }

        StrategyValidator(String namespace, String key, ConnectionStrategy expectedStrategy, Consumer<T> valueChecker) {
            this.namespace = namespace;
            this.key = key;
            this.expectedStrategy = expectedStrategy;
            this.valueChecker = valueChecker;
        }

        @Override
        public void validate(T value) {
            valueChecker.accept(value);
        }

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            Setting<ConnectionStrategy> concrete = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(namespace);
            ConnectionStrategy modeType = (ConnectionStrategy) settings.get(concrete);
            if (isPresent && modeType.equals(expectedStrategy) == false) {
                throw new IllegalArgumentException(
                    "Setting \""
                        + key
                        + "\" cannot be used with the configured \""
                        + concrete.getKey()
                        + "\" [required="
                        + expectedStrategy.name()
                        + ", configured="
                        + modeType.name()
                        + "]"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            Setting<ConnectionStrategy> concrete = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(namespace);
            Stream<Setting<?>> settingStream = Stream.of(concrete);
            return settingStream.iterator();
        }
    }
}
