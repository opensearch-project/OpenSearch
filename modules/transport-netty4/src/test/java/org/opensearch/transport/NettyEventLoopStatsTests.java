/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.settings.Settings;
import org.opensearch.http.netty4.Netty4HttpServerTransport;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.netty4.Netty4Transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NettyEventLoopStatsTests extends OpenSearchTestCase {

    public void testPerWorkerPendingTasks() throws Exception {
        Settings settings = Settings.builder().put(Netty4Transport.WORKER_COUNT.getKey(), 2).build();
        SharedGroupFactory factory = new SharedGroupFactory(settings);
        SharedGroupFactory.SharedGroup group = factory.getTransportGroup();
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            List<EventExecutor> executors = executors(group);
            assertThat(executors, hasSize(2));
            for (EventExecutor executor : executors) {
                executor.execute(() -> {
                    started.countDown();
                    awaitUninterruptibly(release);
                });
            }
            assertTrue(started.await(10, TimeUnit.SECONDS));
            executors.get(0).execute(() -> {});
            for (int i = 0; i < 4; i++) {
                executors.get(1).execute(() -> {});
            }

            EventLoopStats stats = NettyEventLoopStats.collect(group.getLowLevelGroup(), factory.isHttpGroupShared());

            assertNotNull(stats);
            assertTrue(stats.isSharedGroup());
            assertThat(stats.getWorkerCount(), equalTo(2));
            assertThat(pendingTasks(stats, threadName(executors.get(0))), equalTo(1));
            assertThat(pendingTasks(stats, threadName(executors.get(1))), equalTo(4));
            assertThat(stats.getPendingTasks(), equalTo(5L));
            assertThat(stats.getMaxPendingTasks(), equalTo(4));
            assertThat(stats.getWorkers().get(0).getName(), containsString("transport_worker"));
        } finally {
            release.countDown();
            group.shutdown();
        }
    }

    public void testSharedHttpAndTransportReportTheSameQueue() throws Exception {
        Settings settings = Settings.builder().put(Netty4Transport.WORKER_COUNT.getKey(), 1).build();
        SharedGroupFactory factory = new SharedGroupFactory(settings);
        SharedGroupFactory.SharedGroup transportGroup = factory.getTransportGroup();
        SharedGroupFactory.SharedGroup httpGroup = factory.getHttpGroup();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertTrue(factory.isHttpGroupShared());
            assertSame(transportGroup.getLowLevelGroup(), httpGroup.getLowLevelGroup());
            EventExecutor executor = transportGroup.getLowLevelGroup().iterator().next();
            executor.execute(() -> {
                started.countDown();
                awaitUninterruptibly(release);
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            executor.execute(() -> {});
            executor.execute(() -> {});

            EventLoopStats transportStats = NettyEventLoopStats.collect(transportGroup.getLowLevelGroup(), true);
            EventLoopStats httpStats = NettyEventLoopStats.collect(httpGroup.getLowLevelGroup(), true);

            assertThat(transportStats, equalTo(httpStats));
            assertThat(transportStats.getPendingTasks(), equalTo(2L));
            assertThat(transportStats.getWorkers().get(0).getName(), containsString("transport_worker"));
        } finally {
            release.countDown();
            httpGroup.shutdown();
            transportGroup.shutdown();
        }
    }

    public void testDedicatedHttpGroupDoesNotSeeTransportQueue() throws Exception {
        Settings settings = Settings.builder()
            .put(Netty4Transport.WORKER_COUNT.getKey(), 1)
            .put(Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT.getKey(), 1)
            .build();
        SharedGroupFactory factory = new SharedGroupFactory(settings);
        SharedGroupFactory.SharedGroup transportGroup = factory.getTransportGroup();
        SharedGroupFactory.SharedGroup httpGroup = factory.getHttpGroup();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertFalse(factory.isHttpGroupShared());
            assertNotSame(transportGroup.getLowLevelGroup(), httpGroup.getLowLevelGroup());
            EventExecutor executor = transportGroup.getLowLevelGroup().iterator().next();
            executor.execute(() -> {
                started.countDown();
                awaitUninterruptibly(release);
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            executor.execute(() -> {});
            executor.execute(() -> {});

            EventLoopStats transportStats = NettyEventLoopStats.collect(transportGroup.getLowLevelGroup(), false);
            EventLoopStats httpStats = NettyEventLoopStats.collect(httpGroup.getLowLevelGroup(), false);

            assertFalse(transportStats.isSharedGroup());
            assertFalse(httpStats.isSharedGroup());
            assertThat(transportStats.getPendingTasks(), equalTo(2L));
            assertThat(httpStats.getPendingTasks(), equalTo(0L));
            assertThat(transportStats.getWorkers().get(0).getName(), containsString("transport_worker"));
            assertThat(httpStats.getWorkers().get(0).getName(), containsString("http_server_worker"));
        } finally {
            release.countDown();
            httpGroup.shutdown();
            transportGroup.shutdown();
        }
    }

    public void testShuttingDownGroupReturnsNull() throws Exception {
        SharedGroupFactory factory = new SharedGroupFactory(Settings.builder().put(Netty4Transport.WORKER_COUNT.getKey(), 1).build());
        SharedGroupFactory.SharedGroup group = factory.getTransportGroup();
        try {
            assertNull(NettyEventLoopStats.collect(null, true));
        } finally {
            group.shutdown();
        }
        assertNull(NettyEventLoopStats.collect(group.getLowLevelGroup(), true));
    }

    private static List<EventExecutor> executors(SharedGroupFactory.SharedGroup group) {
        List<EventExecutor> executors = new ArrayList<>();
        group.getLowLevelGroup().iterator().forEachRemaining(executors::add);
        return executors;
    }

    private static String threadName(EventExecutor executor) {
        return ((SingleThreadEventExecutor) executor).threadProperties().name();
    }

    private static int pendingTasks(EventLoopStats stats, String name) {
        for (EventLoopStats.Worker worker : stats.getWorkers()) {
            if (worker.getName().equals(name)) {
                return worker.getPendingTasks();
            }
        }
        fail("missing worker [" + name + "] in " + stats);
        return -1;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
