/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertThrows;

public class SocketChannelInterceptorTests extends AgentTestCase {
    @Test
    public void testConnections() throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            assertThrows(SecurityException.class, () -> channel.connect(new InetSocketAddress("localhost", 9200)));

            assertThrows(SecurityException.class, () -> channel.connect(new InetSocketAddress("opensearch.org", 80)));
        }
    }

    @Test
    public void testHostnameResolution() throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            InetAddress[] addresses = InetAddress.getAllByName("localhost");
            for (InetAddress address : addresses) {
                assertThrows(SecurityException.class, () -> channel.connect(new InetSocketAddress(address, 9200)));
            }
        }
    }
}
