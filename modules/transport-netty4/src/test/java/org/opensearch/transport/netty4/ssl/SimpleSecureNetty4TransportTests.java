/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.netty4.ssl;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.util.net.NetUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.fips.FipsAwareSslProvider;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.TransportExceptionHandler;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.BouncyCastleThreadFilter;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.AbstractSimpleTransportTestCase;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.Netty4NioSocketChannel;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.SharedGroupFactory;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.TestProfiles;
import org.opensearch.transport.Transport;
import org.opensearch.transport.netty4.Netty4TcpChannel;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Optional;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ThreadLeakFilters(filters = { BouncyCastleThreadFilter.class })
public class SimpleSecureNetty4TransportTests extends AbstractSimpleTransportTestCase {

    private static final char[] PASSWORD = "password".toCharArray();

    // Cached contexts to avoid repeated keystore loading and SSL context creation
    // (especially slow in FIPS mode with BCFKS).
    private static volatile SslContext cachedClientSslContext;
    private static volatile SslContext cachedServerSslContext;

    private static final FipsAwareSslProvider<SslContext> serverSslContextProvider = (
        String keyStoreType,
        String fileExtension,
        String jcaProvider,
        String jsseProvider) -> {
        if (cachedServerSslContext == null) {
            synchronized (SimpleSecureNetty4TransportTests.class) {
                if (cachedServerSslContext == null) {
                    try {
                        var keyStore = KeyStore.getInstance(keyStoreType, jcaProvider);
                        keyStore.load(
                            SimpleSecureNetty4TransportTests.class.getResourceAsStream("/netty4-server-keystore" + fileExtension),
                            PASSWORD
                        );
                        var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm(), jsseProvider);
                        keyManagerFactory.init(keyStore, PASSWORD);

                        cachedServerSslContext = SslContextBuilder.forServer(keyManagerFactory).clientAuth(ClientAuth.NONE).build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return cachedServerSslContext;
    };

    private static final FipsAwareSslProvider<SslContext> clientSslContextProvider = (
        String keyStoreType,
        String fileExtension,
        String jcaProvider,
        String jsseProvider) -> {
        if (cachedClientSslContext == null) {
            synchronized (SimpleSecureNetty4TransportTests.class) {
                if (cachedClientSslContext == null) {
                    try {
                        final KeyStore trustStore = KeyStore.getInstance(keyStoreType, jcaProvider);
                        trustStore.load(
                            SimpleSecureNetty4TransportTests.class.getResourceAsStream("/netty4-client-truststore" + fileExtension),
                            PASSWORD
                        );
                        var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm(), jsseProvider);
                        trustManagerFactory.init(trustStore);

                        cachedClientSslContext = SslContextBuilder.forClient()
                            .clientAuth(ClientAuth.NONE)
                            .trustManager(trustManagerFactory)
                            .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return cachedClientSslContext;
    };

    @Override
    protected Transport build(Settings settings, final Version version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        final SecureTransportSettingsProvider secureTransportSettingsProvider = new SecureTransportSettingsProvider() {
            @Override
            public Optional<TransportExceptionHandler> buildServerTransportExceptionHandler(Settings settings, Transport transport) {
                return Optional.empty();
            }

            @Override
            public Optional<SSLEngine> buildSecureServerTransportEngine(Settings settings, Transport transport) throws SSLException {
                return Optional.of(serverSslContextProvider.create().newEngine(NettyAllocator.getAllocator()));
            }

            @Override
            public Optional<SSLEngine> buildSecureClientTransportEngine(Settings settings, String hostname, int port) throws SSLException {
                return Optional.of(clientSslContextProvider.create().newEngine(NettyAllocator.getAllocator(), hostname, port));
            }
        };

        return new SecureNetty4Transport(
            settings,
            version,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings),
            secureTransportSettingsProvider,
            NoopTracer.INSTANCE
        ) {

            @Override
            public void executeHandshake(
                DiscoveryNode node,
                TcpChannel channel,
                ConnectionProfile profile,
                ActionListener<Version> listener
            ) {
                if (doHandshake) {
                    super.executeHandshake(node, channel, profile, listener);
                } else {
                    listener.onResponse(version.minimumCompatibilityVersion());
                }
            }
        };
    }

    public void testConnectException() throws UnknownHostException {
        try {
            serviceA.connectToNode(
                new DiscoveryNode(
                    "C",
                    new TransportAddress(InetAddress.getByName("localhost"), 9876),
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT
                )
            );
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
        }
    }

    public void testDefaultKeepAliveSettings() throws IOException {
        assumeTrue("setting default keepalive options not supported on this platform", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        try (
            MockTransportService serviceC = buildService("TS_C", Version.CURRENT, Settings.EMPTY);
            MockTransportService serviceD = buildService("TS_D", Version.CURRENT, Settings.EMPTY)
        ) {
            serviceC.start();
            serviceC.acceptIncomingRequests();
            serviceD.start();
            serviceD.acceptIncomingRequests();

            try (Transport.Connection connection = serviceC.openConnection(serviceD.getLocalDiscoNode(), TestProfiles.LIGHT_PROFILE)) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    checkDefaultKeepAliveOptions(channel);
                }

                assertThat(serviceD.getOriginalTransport(), instanceOf(TcpTransport.class));
                for (TcpChannel channel : getAcceptedChannels((TcpTransport) serviceD.getOriginalTransport())) {
                    assertTrue(channel.isServerChannel());
                    checkDefaultKeepAliveOptions(channel);
                }
            }
        }
    }

    private void checkDefaultKeepAliveOptions(TcpChannel channel) throws IOException {
        assertThat(channel, instanceOf(Netty4TcpChannel.class));
        Netty4TcpChannel nettyChannel = (Netty4TcpChannel) channel;
        assertThat(nettyChannel.getNettyChannel(), instanceOf(Netty4NioSocketChannel.class));
        Netty4NioSocketChannel netty4NioSocketChannel = (Netty4NioSocketChannel) nettyChannel.getNettyChannel();
        SocketChannel socketChannel = netty4NioSocketChannel.javaChannel();
        assertThat(socketChannel.supportedOptions(), hasItem(NetUtils.getTcpKeepIdleSocketOptionOrNull()));
        Integer keepIdle = socketChannel.getOption(NetUtils.getTcpKeepIdleSocketOptionOrNull());
        assertNotNull(keepIdle);
        assertThat(keepIdle, lessThanOrEqualTo(500));
        assertThat(socketChannel.supportedOptions(), hasItem(NetUtils.getTcpKeepIntervalSocketOptionOrNull()));
        Integer keepInterval = socketChannel.getOption(NetUtils.getTcpKeepIntervalSocketOptionOrNull());
        assertNotNull(keepInterval);
        assertThat(keepInterval, lessThanOrEqualTo(500));
    }

}
