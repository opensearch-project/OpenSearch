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
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertThrows;

public class SocketChannelInterceptorTests extends AgentTestCase {
    @Test
    public void test() throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            assertThrows(SecurityException.class, () -> channel.connect(new InetSocketAddress("localhost", 9200)));

            assertThrows(SecurityException.class, () -> channel.connect(UnixDomainSocketAddress.of("fake-path")));
        }
    }
}
