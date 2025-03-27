/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;

import static org.junit.Assert.fail;

@SuppressForbidden(reason = "Test class needs to use socket connections and DNS resolution for testing network permissions")
public class SocketChannelInterceptorAllowTests extends OpenSearchTestCase {

    private SocketChannelInterceptor interceptor;
    private Method testMethod;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        interceptor = new SocketChannelInterceptor();
        testMethod = SocketChannelInterceptor.class.getDeclaredMethod("intercept", Object[].class, Method.class);
    }

    public void testAllowedLocalhost() throws Exception {
        InetSocketAddress address = new InetSocketAddress("localhost", 9200);
        Object[] args = new Object[] { address };

        try {
            SocketChannelInterceptor.intercept(args, testMethod);
        } catch (SecurityException e) {
            fail("Should allow connection to localhost: " + e.getMessage());
        }
    }

    public void testAllowedLocalhostIPv6() throws Exception {
        InetSocketAddress address = new InetSocketAddress("::1", 9200);
        Object[] args = new Object[] { address };

        try {
            SocketChannelInterceptor.intercept(args, testMethod);
        } catch (SecurityException e) {
            fail("Should allow connection to localhost IPv6 (::1): " + e.getMessage());
        }
    }

    public void testAllowedExternalHost() throws Exception {
        InetSocketAddress address = new InetSocketAddress("google.com", 443);
        Object[] args = new Object[] { address };

        try {
            SocketChannelInterceptor.intercept(args, testMethod);
        } catch (SecurityException e) {
            fail("Should allow connection to external host: " + e.getMessage());
        }
    }

    public void testAllowedUnixDomainSocket() throws Exception {
        Path socketPath = Path.of("/tmp/test.sock");
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
        Object[] args = new Object[] { address };

        try {
            SocketChannelInterceptor.intercept(args, testMethod);
        } catch (SecurityException e) {
            fail("Should allow Unix domain socket connection: " + e.getMessage());
        }
    }

    public void testAllowedResolveLocalHost() throws Exception {
        InetSocketAddress address = new InetSocketAddress("localhost", 9200);
        Object[] args = new Object[] { address };

        try {
            InetAddress inetAddress = address.getAddress();
            SocketChannelInterceptor.intercept(args, testMethod);
            assertNotNull("InetAddress should not be null", inetAddress);
            assertFalse("Address should not be unresolved", address.isUnresolved());
        } catch (SecurityException e) {
            fail("Should allow DNS resolution for localhost: " + e.getMessage());
        }
    }

    public void testAllowedResolveExternalHost() throws Exception {
        String host = "opensearch.org";
        int port = 443;

        try {
            InetSocketAddress address = new InetSocketAddress(host, port);
            Object[] args = new Object[] { address };

            SocketChannelInterceptor.intercept(args, testMethod);

            assertFalse("Address should be resolved", address.isUnresolved());
            assertNotNull("Should have IP address", address.getAddress());

        } catch (SecurityException e) {
            fail("Should allow DNS resolution for external host: " + e.getMessage());
        }
    }

    public void testAllowedListenPermission() throws Exception {
        InetSocketAddress address = new InetSocketAddress("localhost", 0);
        Object[] args = new Object[] { address };

        try {
            try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
                serverChannel.bind(address);
                int boundPort = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
                assertTrue("Should bind to an ephemeral port", boundPort > 0);
            }
        } catch (SecurityException e) {
            fail("Should allow binding to localhost: " + e.getMessage());
        }
    }

    public void testCombinedPermissions() throws Exception {
        // 1. Create a server socket
        try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
            InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
            serverChannel.bind(bindAddress);

            int port = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();

            // 2. Resolve hostname
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", port);
            Object[] args = new Object[] { clientAddress };

            // Test the interceptor
            SocketChannelInterceptor.intercept(args, testMethod);

            // Verify resolution
            assertFalse("Address should be resolved", clientAddress.isUnresolved());

            // 3. Connect to server
            try (SocketChannel clientChannel = SocketChannel.open()) {
                clientChannel.connect(clientAddress);
                assertTrue("Should connect successfully", clientChannel.isConnected());
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

}
