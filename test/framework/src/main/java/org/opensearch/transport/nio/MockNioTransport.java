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

package org.opensearch.transport.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.recycler.Recycler;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.nio.BytesChannelContext;
import org.opensearch.nio.BytesWriteHandler;
import org.opensearch.nio.ChannelFactory;
import org.opensearch.nio.Config;
import org.opensearch.nio.InboundChannelBuffer;
import org.opensearch.nio.NioSelector;
import org.opensearch.nio.NioSelectorGroup;
import org.opensearch.nio.NioServerSocketChannel;
import org.opensearch.nio.NioSocketChannel;
import org.opensearch.nio.Page;
import org.opensearch.nio.ServerChannelContext;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.InboundPipeline;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpServerChannel;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;

public class MockNioTransport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(MockNioTransport.class);

    private final ConcurrentMap<String, MockTcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final TransportThreadWatchdog transportThreadWatchdog;
    private volatile NioSelectorGroup nioGroup;
    private volatile MockTcpChannelFactory clientChannelFactory;

    public MockNioTransport(
        Settings settings,
        Version version,
        ThreadPool threadPool,
        NetworkService networkService,
        PageCacheRecycler pageCacheRecycler,
        NamedWriteableRegistry namedWriteableRegistry,
        CircuitBreakerService circuitBreakerService,
        Tracer tracer
    ) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService, tracer);
        this.transportThreadWatchdog = new TransportThreadWatchdog(threadPool, settings);
    }

    @Override
    protected MockServerChannel bind(String name, InetSocketAddress address) throws IOException {
        MockTcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        MockServerChannel serverChannel = nioGroup.bindServerChannel(address, channelFactory);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        serverChannel.addBindListener(ActionListener.toBiConsumer(future));
        future.actionGet();
        return serverChannel;
    }

    @Override
    protected MockSocketChannel initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        return nioGroup.openChannel(address, clientChannelFactory);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            nioGroup = new NioSelectorGroup(
                daemonThreadFactory(this.settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                2,
                (s) -> new TestEventHandler(this::onNonChannelException, s, transportThreadWatchdog)
            );

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, "default");
            clientChannelFactory = new MockTcpChannelFactory(true, clientProfileSettings, "client");

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    MockTcpChannelFactory factory = new MockTcpChannelFactory(false, profileSettings, profileName);
                    profileToChannelFactory.putIfAbsent(profileName, factory);
                    bindServer(profileSettings);
                }
            }

            super.doStart();
            success = true;
        } catch (IOException e) {
            throw new OpenSearchException(e);
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    @Override
    protected void stopInternal() {
        try {
            transportThreadWatchdog.stop();
            nioGroup.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping nio group", e);
        }
        profileToChannelFactory.clear();
    }

    @Override
    protected ConnectionProfile maybeOverrideConnectionProfile(ConnectionProfile connectionProfile) {
        if (connectionProfile.getNumConnections() <= 3) {
            return connectionProfile;
        }
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        Set<TransportRequestOptions.Type> allTypesWithConnection = new HashSet<>();
        Set<TransportRequestOptions.Type> allTypesWithoutConnection = new HashSet<>();
        for (TransportRequestOptions.Type type : TransportRequestOptions.Type.values()) {
            int numConnections = connectionProfile.getNumConnectionsPerType(type);
            if (numConnections > 0) {
                allTypesWithConnection.add(type);
            } else {
                allTypesWithoutConnection.add(type);
            }
        }

        // make sure we maintain at least the types that are supported by this profile even if we only use a single channel for them.
        builder.addConnections(3, allTypesWithConnection.toArray(new TransportRequestOptions.Type[0]));
        if (allTypesWithoutConnection.isEmpty() == false) {
            builder.addConnections(0, allTypesWithoutConnection.toArray(new TransportRequestOptions.Type[0]));
        }
        builder.setHandshakeTimeout(connectionProfile.getHandshakeTimeout());
        builder.setConnectTimeout(connectionProfile.getConnectTimeout());
        builder.setPingInterval(connectionProfile.getPingInterval());
        builder.setCompressionEnabled(connectionProfile.getCompressionEnabled());
        return builder.build();
    }

    private void onNonChannelException(Exception exception) {
        logger.warn(
            new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()),
            exception
        );
    }

    private void exceptionCaught(NioSocketChannel channel, Exception exception) {
        onException((TcpChannel) channel, exception);
    }

    private void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((TcpChannel) channel);
    }

    private class MockTcpChannelFactory extends ChannelFactory<MockServerChannel, MockSocketChannel> {

        private final boolean isClient;
        private final String profileName;

        private MockTcpChannelFactory(boolean isClient, ProfileSettings profileSettings, String profileName) {
            super(
                profileSettings.tcpNoDelay,
                profileSettings.tcpKeepAlive,
                profileSettings.tcpKeepIdle,
                profileSettings.tcpKeepInterval,
                profileSettings.tcpKeepCount,
                profileSettings.reuseAddress,
                Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes())
            );
            this.isClient = isClient;
            this.profileName = profileName;
        }

        @Override
        public MockSocketChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) {
            MockSocketChannel nioChannel = new MockSocketChannel(isClient == false, profileName, channel);
            IntFunction<Page> pageSupplier = (length) -> {
                if (length > PageCacheRecycler.BYTE_PAGE_SIZE) {
                    return new Page(ByteBuffer.allocate(length), () -> {});
                } else {
                    Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                    return new Page(ByteBuffer.wrap(bytes.v(), 0, length), bytes::close);
                }
            };
            MockTcpReadWriteHandler readWriteHandler = new MockTcpReadWriteHandler(nioChannel, pageCacheRecycler, MockNioTransport.this);
            BytesChannelContext context = new BytesChannelContext(
                nioChannel,
                selector,
                socketConfig,
                e -> exceptionCaught(nioChannel, e),
                readWriteHandler,
                new InboundChannelBuffer(pageSupplier)
            );
            nioChannel.setContext(context);
            nioChannel.addConnectListener((v, e) -> {
                if (e == null) {
                    if (channel.isConnected()) {
                        try {
                            channel.setOption(StandardSocketOptions.SO_LINGER, 0);
                        } catch (IOException ex) {
                            throw new UncheckedIOException(new IOException());
                        }
                    }
                }
            });
            return nioChannel;
        }

        @Override
        public MockServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel, Config.ServerSocket socketConfig) {
            MockServerChannel nioServerChannel = new MockServerChannel(channel);
            ServerChannelContext context = new ServerChannelContext(
                nioServerChannel,
                this,
                selector,
                socketConfig,
                MockNioTransport.this::acceptChannel,
                e -> onServerException(nioServerChannel, e)
            ) {
                @Override
                public void acceptChannels(Supplier<NioSelector> selectorSupplier) throws IOException {
                    int acceptCount = 0;
                    SocketChannel acceptedChannel;
                    while ((acceptedChannel = accept(rawChannel)) != null) {
                        NioSocketChannel nioChannel = MockTcpChannelFactory.this.acceptNioChannel(acceptedChannel, selectorSupplier);
                        acceptChannel(nioChannel);
                        ++acceptCount;
                        if (acceptCount % 100 == 0) {
                            logger.warn("Accepted [{}] connections in a single select loop iteration on [{}]", acceptCount, channel);
                        }
                    }
                }
            };
            nioServerChannel.setContext(context);
            return nioServerChannel;
        }
    }

    private static class MockTcpReadWriteHandler extends BytesWriteHandler {

        private final MockSocketChannel channel;
        private final InboundPipeline pipeline;

        private MockTcpReadWriteHandler(MockSocketChannel channel, PageCacheRecycler recycler, TcpTransport transport) {
            this.channel = channel;
            final ThreadPool threadPool = transport.getThreadPool();
            final Supplier<CircuitBreaker> breaker = transport.getInflightBreaker();
            final RequestHandlers requestHandlers = transport.getRequestHandlers();
            final Version version = transport.getVersion();
            final StatsTracker statsTracker = transport.getStatsTracker();
            this.pipeline = new InboundPipeline(
                version,
                statsTracker,
                recycler,
                threadPool::relativeTimeInMillis,
                breaker,
                requestHandlers::getHandler,
                transport::inboundMessage
            );
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            Page[] pages = channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex());
            BytesReference[] references = new BytesReference[pages.length];
            for (int i = 0; i < pages.length; ++i) {
                references[i] = BytesReference.fromByteBuffer(pages[i].byteBuffer());
            }
            Releasable releasable = () -> IOUtils.closeWhileHandlingException(pages);
            try (ReleasableBytesReference reference = new ReleasableBytesReference(CompositeBytesReference.of(references), releasable)) {
                pipeline.handleBytes(channel, reference);
                return reference.length();
            }
        }

        @Override
        public void close() {
            Releasables.closeWhileHandlingException(pipeline);
            super.close();
        }
    }

    private static class MockServerChannel extends NioServerSocketChannel implements TcpServerChannel {

        MockServerChannel(ServerSocketChannel channel) {
            super(channel);
        }

        @Override
        public void close() {
            getContext().closeChannel();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            addCloseListener(ActionListener.toBiConsumer(listener));
        }
    }

    private static class MockSocketChannel extends NioSocketChannel implements TcpChannel {

        private final boolean isServer;
        private final String profile;
        private final ChannelStats stats = new ChannelStats();

        private MockSocketChannel(boolean isServer, String profile, SocketChannel socketChannel) {
            super(socketChannel);
            this.isServer = isServer;
            this.profile = profile;
        }

        @Override
        public void close() {
            getContext().closeChannel();
        }

        @Override
        public String getProfile() {
            return profile;
        }

        @Override
        public boolean isServerChannel() {
            return isServer;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            addCloseListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public void addConnectListener(ActionListener<Void> listener) {
            addConnectListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public ChannelStats getChannelStats() {
            return stats;
        }

        @Override
        public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
            getContext().sendMessage(BytesReference.toByteBuffers(reference), ActionListener.toBiConsumer(listener));
        }
    }

    static final class TransportThreadWatchdog {
        // Only check every 2s to not flood the logs on a blocked thread.
        // We mostly care about long blocks and not random slowness anyway and in tests would randomly catch slow operations that block for
        // less than 2s eventually.
        private static final TimeValue CHECK_INTERVAL = TimeValue.timeValueSeconds(2);

        private final long warnThreshold;
        private final ThreadPool threadPool;
        private final ConcurrentHashMap<Thread, Long> registry = new ConcurrentHashMap<>();

        private volatile boolean stopped;

        TransportThreadWatchdog(ThreadPool threadPool, Settings settings) {
            this.threadPool = threadPool;
            warnThreshold = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(settings).nanos() + TimeValue.timeValueMillis(100L).nanos();
            threadPool.schedule(this::logLongRunningExecutions, CHECK_INTERVAL, ThreadPool.Names.GENERIC);
        }

        public boolean register() {
            Long previousValue = registry.put(Thread.currentThread(), threadPool.relativeTimeInNanos());
            return previousValue == null;
        }

        public void unregister() {
            Long previousValue = registry.remove(Thread.currentThread());
            assert previousValue != null;
            maybeLogElapsedTime(previousValue);
        }

        private void maybeLogElapsedTime(long startTime) {
            long elapsedTime = threadPool.relativeTimeInNanos() - startTime;
            if (elapsedTime > warnThreshold) {
                logger.warn(
                    new ParameterizedMessage(
                        "Slow execution on network thread [{} milliseconds]",
                        TimeUnit.NANOSECONDS.toMillis(elapsedTime)
                    ),
                    new RuntimeException("Slow exception on network thread")
                );
            }
        }

        private void logLongRunningExecutions() {
            for (Map.Entry<Thread, Long> entry : registry.entrySet()) {
                final Long blockedSinceInNanos = entry.getValue();
                final long elapsedTimeInNanos = threadPool.relativeTimeInNanos() - blockedSinceInNanos;
                if (elapsedTimeInNanos > warnThreshold) {
                    final Thread thread = entry.getKey();
                    final String stackTrace = Arrays.stream(thread.getStackTrace()).map(Object::toString).collect(Collectors.joining("\n"));
                    final Thread.State threadState = thread.getState();
                    if (blockedSinceInNanos.equals(registry.get(thread))) {
                        logger.warn(
                            "Potentially blocked execution on network thread [{}] [{}] [{} milliseconds]: \n{}",
                            thread.getName(),
                            threadState,
                            TimeUnit.NANOSECONDS.toMillis(elapsedTimeInNanos),
                            stackTrace
                        );
                    }
                }
            }
            if (stopped == false) {
                threadPool.scheduleUnlessShuttingDown(CHECK_INTERVAL, ThreadPool.Names.GENERIC, this::logLongRunningExecutions);
            }
        }

        public void stop() {
            stopped = true;
        }
    }
}
