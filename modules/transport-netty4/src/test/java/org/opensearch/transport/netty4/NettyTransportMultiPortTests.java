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

package org.opensearch.transport.netty4;

import org.opensearch.Version;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.network.NetworkUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.TransportSettings;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class NettyTransportMultiPortTests extends OpenSearchTestCase {

    private String host;

    @Before
    public void setup() {
        if (NetworkUtils.SUPPORTS_V6 && randomBoolean()) {
            host = "::1";
        } else {
            host = "127.0.0.1";
        }
    }

    public void testThatNettyCanBindToMultiplePorts() throws Exception {
        Settings settings = Settings.builder()
            .put("network.host", host)
            .put(TransportSettings.PORT.getKey(), 22) // will not actually bind to this
            .put("transport.profiles.default.port", 0)
            .put("transport.profiles.client1.port", 0)
            .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport transport = startTransport(settings, threadPool)) {
            assertEquals(1, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatDefaultProfileInheritsFromStandardSettings() throws Exception {
        Settings settings = Settings.builder()
            .put("network.host", host)
            .put(TransportSettings.PORT.getKey(), 0)
            .put("transport.profiles.client1.port", 0)
            .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport transport = startTransport(settings, threadPool)) {
            assertEquals(1, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatProfileWithoutPortSettingsFails() throws Exception {

        Settings settings = Settings.builder()
            .put("network.host", host)
            .put(TransportSettings.PORT.getKey(), 0)
            .put("transport.profiles.client1.whatever", "foo")
            .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try {
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> startTransport(settings, threadPool));
            assertEquals("profile [client1] has no port configured", ex.getMessage());
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatDefaultProfilePortOverridesGeneralConfiguration() throws Exception {
        Settings settings = Settings.builder()
            .put("network.host", host)
            .put(TransportSettings.PORT.getKey(), 22) // will not actually bind to this
            .put("transport.profiles.default.port", 0)
            .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport transport = startTransport(settings, threadPool)) {
            assertEquals(0, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    private TcpTransport startTransport(Settings settings, ThreadPool threadPool) {
        PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);
        TcpTransport transport = new Netty4Transport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            recycler,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings)
        );
        transport.start();

        assertThat(transport.lifecycleState(), is(Lifecycle.State.STARTED));
        return transport;
    }
}
