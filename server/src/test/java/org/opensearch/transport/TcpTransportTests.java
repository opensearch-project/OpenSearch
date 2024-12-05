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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.network.NetworkUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/** Unit tests for {@link TcpTransport} */
public class TcpTransportTests extends OpenSearchTestCase {

    /** Test ipv4 host with a default port works */
    public void testParseV4DefaultPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1", 1234);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv4 host with port works */
    public void testParseV4WithPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1:2345", 1234);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    /** Test unbracketed ipv6 hosts in configuration fail. Leave no ambiguity */
    public void testParseV6UnBracketed() throws Exception {
        try {
            TcpTransport.parse("::1", 1234);
            fail("should have gotten exception");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("must be bracketed"));
        }
    }

    /** Test ipv6 host with a default port works */
    public void testParseV6DefaultPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]", 1234);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv6 host with port works */
    public void testParseV6WithPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]:2345", 1234);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    public void testRejectsPortRanges() {
        expectThrows(NumberFormatException.class, () -> TcpTransport.parse("[::1]:100-200", 1000));
    }

    public void testDefaultSeedAddressesWithDefaultPort() {
        final Matcher<Iterable<? extends String>> seedAddressMatcher = NetworkUtils.SUPPORTS_V6
            ? containsInAnyOrder(
                "[::1]:9300",
                "[::1]:9301",
                "[::1]:9302",
                "[::1]:9303",
                "[::1]:9304",
                "[::1]:9305",
                "127.0.0.1:9300",
                "127.0.0.1:9301",
                "127.0.0.1:9302",
                "127.0.0.1:9303",
                "127.0.0.1:9304",
                "127.0.0.1:9305"
            )
            : containsInAnyOrder(
                "127.0.0.1:9300",
                "127.0.0.1:9301",
                "127.0.0.1:9302",
                "127.0.0.1:9303",
                "127.0.0.1:9304",
                "127.0.0.1:9305"
            );
        testDefaultSeedAddresses(Settings.EMPTY, seedAddressMatcher);
    }

    public void testDefaultSeedAddressesWithNonstandardGlobalPortRange() {
        final Matcher<Iterable<? extends String>> seedAddressMatcher = NetworkUtils.SUPPORTS_V6
            ? containsInAnyOrder(
                "[::1]:9500",
                "[::1]:9501",
                "[::1]:9502",
                "[::1]:9503",
                "[::1]:9504",
                "[::1]:9505",
                "127.0.0.1:9500",
                "127.0.0.1:9501",
                "127.0.0.1:9502",
                "127.0.0.1:9503",
                "127.0.0.1:9504",
                "127.0.0.1:9505"
            )
            : containsInAnyOrder(
                "127.0.0.1:9500",
                "127.0.0.1:9501",
                "127.0.0.1:9502",
                "127.0.0.1:9503",
                "127.0.0.1:9504",
                "127.0.0.1:9505"
            );
        testDefaultSeedAddresses(Settings.builder().put(TransportSettings.PORT.getKey(), "9500-9600").build(), seedAddressMatcher);
    }

    public void testDefaultSeedAddressesWithSmallGlobalPortRange() {
        final Matcher<Iterable<? extends String>> seedAddressMatcher = NetworkUtils.SUPPORTS_V6
            ? containsInAnyOrder("[::1]:9300", "[::1]:9301", "[::1]:9302", "127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302")
            : containsInAnyOrder("127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302");
        testDefaultSeedAddresses(Settings.builder().put(TransportSettings.PORT.getKey(), "9300-9302").build(), seedAddressMatcher);
    }

    public void testDefaultSeedAddressesWithNonstandardProfilePortRange() {
        final Matcher<Iterable<? extends String>> seedAddressMatcher = NetworkUtils.SUPPORTS_V6
            ? containsInAnyOrder(
                "[::1]:9500",
                "[::1]:9501",
                "[::1]:9502",
                "[::1]:9503",
                "[::1]:9504",
                "[::1]:9505",
                "127.0.0.1:9500",
                "127.0.0.1:9501",
                "127.0.0.1:9502",
                "127.0.0.1:9503",
                "127.0.0.1:9504",
                "127.0.0.1:9505"
            )
            : containsInAnyOrder(
                "127.0.0.1:9500",
                "127.0.0.1:9501",
                "127.0.0.1:9502",
                "127.0.0.1:9503",
                "127.0.0.1:9504",
                "127.0.0.1:9505"
            );
        testDefaultSeedAddresses(
            Settings.builder()
                .put(TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(TransportSettings.DEFAULT_PROFILE).getKey(), "9500-9600")
                .build(),
            seedAddressMatcher
        );
    }

    public void testDefaultSeedAddressesWithSmallProfilePortRange() {
        final Matcher<Iterable<? extends String>> seedAddressMatcher = NetworkUtils.SUPPORTS_V6
            ? containsInAnyOrder("[::1]:9300", "[::1]:9301", "[::1]:9302", "127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302")
            : containsInAnyOrder("127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302");
        testDefaultSeedAddresses(
            Settings.builder()
                .put(TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(TransportSettings.DEFAULT_PROFILE).getKey(), "9300-9302")
                .build(),
            seedAddressMatcher
        );
    }

    public void testDefaultSeedAddressesPrefersProfileSettingToGlobalSetting() {
        final Matcher<Iterable<? extends String>> seedAddressMatcher = NetworkUtils.SUPPORTS_V6
            ? containsInAnyOrder("[::1]:9300", "[::1]:9301", "[::1]:9302", "127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302")
            : containsInAnyOrder("127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302");
        testDefaultSeedAddresses(
            Settings.builder()
                .put(TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(TransportSettings.DEFAULT_PROFILE).getKey(), "9300-9302")
                .put(TransportSettings.PORT.getKey(), "9500-9600")
                .build(),
            seedAddressMatcher
        );
    }

    public void testDefaultSeedAddressesWithNonstandardSinglePort() {
        testDefaultSeedAddresses(
            Settings.builder().put(TransportSettings.PORT.getKey(), "9500").build(),
            NetworkUtils.SUPPORTS_V6 ? containsInAnyOrder("[::1]:9500", "127.0.0.1:9500") : containsInAnyOrder("127.0.0.1:9500")
        );
    }

    private void testDefaultSeedAddresses(final Settings settings, Matcher<Iterable<? extends String>> seedAddressesMatcher) {
        final TestThreadPool testThreadPool = new TestThreadPool("test");
        try {
            final TcpTransport tcpTransport = new TcpTransport(
                settings,
                Version.CURRENT,
                testThreadPool,
                new MockPageCacheRecycler(settings),
                new NoneCircuitBreakerService(),
                writableRegistry(),
                new NetworkService(Collections.emptyList()),
                NoopTracer.INSTANCE
            ) {

                @Override
                protected TcpServerChannel bind(String name, InetSocketAddress address) {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected TcpChannel initiateChannel(DiscoveryNode node) {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected void stopInternal() {
                    throw new UnsupportedOperationException();
                }
            };

            assertThat(tcpTransport.getDefaultSeedAddresses(), seedAddressesMatcher);
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testReadMessageLengthWithIncompleteHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.write(1);
        streamOutput.write(1);

        assertEquals(-1, TcpTransport.readMessageLength(streamOutput.bytes()));
    }

    public void testReadPingMessageLength() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-1);

        assertEquals(0, TcpTransport.readMessageLength(streamOutput.bytes()));
    }

    public void testReadPingMessageLengthWithStartOfSecondMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-1);
        streamOutput.write('E');
        streamOutput.write('S');

        assertEquals(0, TcpTransport.readMessageLength(streamOutput.bytes()));
    }

    public void testReadMessageLength() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(2);
        streamOutput.write('M');
        streamOutput.write('A');

        assertEquals(2, TcpTransport.readMessageLength(streamOutput.bytes()));

    }

    public void testInvalidLength() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-2);
        streamOutput.write('M');
        streamOutput.write('A');

        try {
            TcpTransport.readMessageLength(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(StreamCorruptedException.class));
            assertEquals("invalid data length: -2", ex.getMessage());
        }
    }

    public void testInvalidHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('C');
        byte byte1 = randomByte();
        byte byte2 = randomByte();
        streamOutput.write(byte1);
        streamOutput.write(byte2);
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());

        try {
            TcpTransport.readMessageLength(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(StreamCorruptedException.class));
            String expected = "invalid internal transport message format, got (45,43,"
                + Integer.toHexString(byte1 & 0xFF)
                + ","
                + Integer.toHexString(byte2 & 0xFF)
                + ")";
            assertEquals(expected, ex.getMessage());
        }
    }

    public void testHTTPRequest() throws IOException {
        String[] httpHeaders = { "GET", "POST", "PUT", "HEAD", "DELETE", "OPTIONS", "PATCH", "TRACE" };

        for (String httpHeader : httpHeaders) {
            BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);

            for (char c : httpHeader.toCharArray()) {
                streamOutput.write((byte) c);
            }
            streamOutput.write(new byte[6]);

            try {
                TcpTransport.readMessageLength(streamOutput.bytes());
                fail("Expected exception");
            } catch (Exception ex) {
                assertThat(ex, instanceOf(TcpTransport.HttpRequestOnTransportException.class));
                assertEquals("This is not an HTTP port", ex.getMessage());
            }
        }
    }

    public void testTLSHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);

        streamOutput.write(0x16);
        streamOutput.write(0x03);
        byte byte1 = randomByte();
        streamOutput.write(byte1);
        byte byte2 = randomByte();
        streamOutput.write(byte2);
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());

        try {
            TcpTransport.readMessageLength(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(StreamCorruptedException.class));
            String expected = "SSL/TLS request received but SSL/TLS is not enabled on this node, got (16,3,"
                + Integer.toHexString(byte1 & 0xFF)
                + ","
                + Integer.toHexString(byte2 & 0xFF)
                + ")";
            assertEquals(expected, ex.getMessage());
        }
    }

    public void testHTTPResponse() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('H');
        streamOutput.write('T');
        streamOutput.write('T');
        streamOutput.write('P');
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());

        try {
            TcpTransport.readMessageLength(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(StreamCorruptedException.class));
            assertEquals(
                "received HTTP response on transport port, ensure that transport port "
                    + "(not HTTP port) of a remote node is specified in the configuration",
                ex.getMessage()
            );
        }
    }

    @TestLogging(reason = "testing logging", value = "org.opensearch.transport.TcpTransport:DEBUG")
    public void testExceptionHandling() throws IllegalAccessException {
        testExceptionHandling(
            false,
            new OpenSearchException("simulated"),
            true,
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.ERROR, "*"),
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.WARN, "*"),
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.INFO, "*"),
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.DEBUG, "*")
        );
        testExceptionHandling(
            new OpenSearchException("simulated"),
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.opensearch.transport.TcpTransport",
                Level.WARN,
                "exception caught on transport layer [*], closing connection"
            )
        );
        testExceptionHandling(
            new ClosedChannelException(),
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.opensearch.transport.TcpTransport",
                Level.DEBUG,
                "close connection exception caught on transport layer [*], disconnecting from relevant node"
            )
        );
        testExceptionHandling(
            new OpenSearchException("Connection reset"),
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.opensearch.transport.TcpTransport",
                Level.DEBUG,
                "close connection exception caught on transport layer [*], disconnecting from relevant node"
            )
        );
        testExceptionHandling(
            new BindException(),
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.opensearch.transport.TcpTransport",
                Level.DEBUG,
                "bind exception caught on transport layer [*]"
            )
        );
        testExceptionHandling(
            new CancelledKeyException(),
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.opensearch.transport.TcpTransport",
                Level.DEBUG,
                "cancelled key exception caught on transport layer [*], disconnecting from relevant node"
            )
        );
        testExceptionHandling(
            true,
            new TcpTransport.HttpRequestOnTransportException("test"),
            false,
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.ERROR, "*"),
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.WARN, "*"),
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.INFO, "*"),
            new MockLogAppender.UnseenEventExpectation("message", "org.opensearch.transport.TcpTransport", Level.DEBUG, "*")
        );
        testExceptionHandling(
            new StreamCorruptedException("simulated"),
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.opensearch.transport.TcpTransport",
                Level.WARN,
                "simulated, [*], closing connection"
            )
        );
    }

    private void testExceptionHandling(Exception exception, MockLogAppender.LoggingExpectation... expectations)
        throws IllegalAccessException {
        testExceptionHandling(true, exception, true, expectations);
    }

    private void testExceptionHandling(
        boolean startTransport,
        Exception exception,
        boolean expectClosed,
        MockLogAppender.LoggingExpectation... expectations
    ) throws IllegalAccessException {
        final TestThreadPool testThreadPool = new TestThreadPool("test");
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TcpTransport.class))) {
            for (MockLogAppender.LoggingExpectation expectation : expectations) {
                appender.addExpectation(expectation);
            }

            final Lifecycle lifecycle = new Lifecycle();

            if (startTransport) {
                lifecycle.moveToStarted();
            }

            final FakeTcpChannel channel = new FakeTcpChannel();
            final PlainActionFuture<Void> listener = new PlainActionFuture<>();
            channel.addCloseListener(listener);

            TcpTransport.handleException(channel, exception, lifecycle, new OutboundHandler(new StatsTracker(), testThreadPool));

            if (expectClosed) {
                assertTrue(listener.isDone());
                assertThat(listener.actionGet(), nullValue());
            } else {
                assertFalse(listener.isDone());
            }

            appender.assertAllExpectationsMatched();

        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }
}
