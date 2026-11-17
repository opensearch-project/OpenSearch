/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.http.AbstractHttpServerTransport;
import org.opensearch.http.HttpServerChannel;
import org.opensearch.http.HttpStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.EventLoopStats;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class Netty4CompositeHttpServerTransportTests extends OpenSearchTestCase {

    public void testStatsSumsConnectionsWithoutDoubleCountingWorkers() {
        EventLoopStats first = new EventLoopStats(
            true,
            List.of(new EventLoopStats.Worker("transport_worker[T#1]", 4), new EventLoopStats.Worker("transport_worker[T#2]", 6))
        );
        EventLoopStats second = new EventLoopStats(true, List.of(new EventLoopStats.Worker("transport_worker[T#1]", 9)));

        HttpStats combined = new Netty4CompositeHttpServerTransport(
            transport(new HttpStats.Builder().serverOpen(1).totalOpen(2).eventLoopStats(first).build()),
            transport(new HttpStats.Builder().serverOpen(3).totalOpen(4).eventLoopStats(second).build())
        ).stats();

        assertThat(combined.getServerOpen(), equalTo(4L));
        assertThat(combined.getTotalOpen(), equalTo(6L));
        assertThat(combined.getEventLoopStats(), equalTo(first));
        assertThat(combined.getEventLoopStats().getPendingTasks(), equalTo(10L));
    }

    public void testStatsUsesFirstAvailableEventLoop() {
        EventLoopStats eventLoopStats = new EventLoopStats(false, List.of(new EventLoopStats.Worker("http_server_worker[T#1]", 2)));

        HttpStats combined = new Netty4CompositeHttpServerTransport(
            transport(new HttpStats.Builder().serverOpen(1).totalOpen(1).build()),
            transport(new HttpStats.Builder().serverOpen(2).totalOpen(3).eventLoopStats(eventLoopStats).build())
        ).stats();

        assertThat(combined.getServerOpen(), equalTo(3L));
        assertThat(combined.getTotalOpen(), equalTo(4L));
        assertThat(combined.getEventLoopStats(), equalTo(eventLoopStats));
    }

    public void testStatsWithoutEventLoops() {
        HttpStats combined = new Netty4CompositeHttpServerTransport(
            transport(new HttpStats.Builder().serverOpen(1).totalOpen(1).build()),
            transport(new HttpStats.Builder().serverOpen(1).totalOpen(1).build())
        ).stats();

        assertThat(combined.getEventLoopStats(), nullValue());
    }

    private static AbstractHttpServerTransport transport(HttpStats httpStats) {
        return new AbstractHttpServerTransport(
            Settings.EMPTY,
            new NetworkService(Collections.emptyList()),
            null,
            null,
            null,
            null,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        ) {
            @Override
            public HttpStats stats() {
                return httpStats;
            }

            @Override
            protected void doStart() {}

            @Override
            protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                return null;
            }

            @Override
            protected void stopInternal() {}
        };
    }
}
