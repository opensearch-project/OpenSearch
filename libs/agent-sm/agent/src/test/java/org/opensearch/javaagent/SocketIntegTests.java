/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;

@SuppressForbidden(reason = "Test class needs to use socket connections and DNS resolution for testing network permissions")
@AwaitsFix(bugUrl = "Awaiting fix for: https://github.com/opensearch-project/OpenSearch/issues/17662")
public class SocketIntegTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testLocalSocketConnection() throws IOException {
        try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
            serverChannel.bind(new InetSocketAddress("localhost", 0));
            int port = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();

            new Thread(() -> {
                try {
                    serverChannel.accept();
                } catch (IOException ignored) {}
            }).start();

            try (SocketChannel clientChannel = SocketChannel.open(new InetSocketAddress("localhost", port))) {
                assertTrue("Client should be connected", clientChannel.isConnected());
            }
        }
    }

    public void testBasicUnixDomainSocket() throws IOException {
        // Skip test if not running on Unix-like system
        assumeTrue("Test requires Unix-like system", System.getProperty("os.name").toLowerCase().matches(".*(unix|linux|mac).*"));

        Path socketPath = Path.of("/tmp/test-" + System.nanoTime() + ".sock");

        ServerSocketChannel serverChannel = null;
        SocketChannel clientChannel = null;
        SocketChannel acceptedChannel = null;

        try {
            // Create Unix Domain Socket server
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
            serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            serverChannel.bind(address);

            // Configure non-blocking mode
            serverChannel.configureBlocking(false);

            // Connect client
            clientChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
            clientChannel.connect(address);

            // Accept the connection
            acceptedChannel = serverChannel.accept();
            assertNotNull("Server should accept the connection", acceptedChannel);

            // Verify connections
            assertTrue("Client should be connected", clientChannel.isConnected());
            assertTrue("Accepted channel should be connected", acceptedChannel.isConnected());

        } finally {
            // Cleanup
            if (acceptedChannel != null) {
                acceptedChannel.close();
            }
            if (clientChannel != null) {
                clientChannel.close();
            }
            if (serverChannel != null) {
                serverChannel.close();
            }
            Files.deleteIfExists(socketPath);
        }
    }

    public void testExternalHostConnection() throws IOException {
        try (SocketChannel clientChannel = SocketChannel.open()) {
            clientChannel.connect(new InetSocketAddress("opensearch.org", 80));
            assertTrue("Client should be connected to external host", clientChannel.isConnected());
        }
    }

    public void testHostnameResolution() throws UnknownHostException {
        InetAddress address = InetAddress.getByName("opensearch.org");
        assertNotNull("Host should be resolvable", address);
    }

}
