/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;

/**
 * Reads Netty event-loop queue depth without submitting work to those loops.
 * Submitting from a stats call would block when a worker is already saturated.
 */
public final class NettyEventLoopStats {

    private NettyEventLoopStats() {}

    /**
     * @param sharedGroup true when HTTP and transport are configured to use {@code group}
     * @return worker queue stats, or {@code null} when the group is closed or shutting down
     */
    public static EventLoopStats collect(EventLoopGroup group, boolean sharedGroup) {
        if (group == null || group.isShuttingDown()) {
            return null;
        }
        List<EventLoopStats.Worker> workers = new ArrayList<>();
        int index = 0;
        for (EventExecutor executor : group) {
            workers.add(workerStats(executor, index));
            index++;
        }
        return new EventLoopStats(sharedGroup, workers);
    }

    private static EventLoopStats.Worker workerStats(EventExecutor executor, int index) {
        if (executor instanceof SingleThreadEventExecutor eventExecutor) {
            // Read the queue before resolving the thread name. Resolving the name of an idle loop starts its thread,
            // and that startup task must not be counted as application backlog.
            int pendingTasks = eventExecutor.pendingTasks();
            return new EventLoopStats.Worker(threadName(eventExecutor, index), pendingTasks);
        }
        return new EventLoopStats.Worker("event_loop[" + index + "]", 0);
    }

    private static String threadName(SingleThreadEventExecutor executor, int index) {
        try {
            return executor.threadProperties().name();
        } catch (RuntimeException e) {
            return "event_loop[" + index + "]";
        }
    }
}
