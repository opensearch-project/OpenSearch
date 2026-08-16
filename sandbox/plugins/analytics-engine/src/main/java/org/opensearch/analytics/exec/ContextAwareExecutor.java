/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

/**
 * Wraps an {@link Executor} to propagate the {@code X-Opaque-Id} request header from
 * OpenSearch's {@link org.opensearch.common.util.concurrent.ThreadContext} into Log4j2's
 * MDC ({@link org.apache.logging.log4j.ThreadContext}) on the executing thread.
 *
 * <p>Use this for any analytics engine executor that dispatches work to a thread pool,
 * so that all downstream loggers automatically include the opaque ID without explicit
 * parameter passing.
 *
 * @opensearch.internal
 */
public final class ContextAwareExecutor {

    private ContextAwareExecutor() {}

    /**
     * Wraps the given executor so that tasks run with the {@code opaque_id} MDC key set
     * from the current thread's {@code X-Opaque-Id} header.
     */
    public static Executor wrap(Executor delegate, ThreadPool threadPool) {
        return cmd -> {
            String opaqueId = threadPool.getThreadContext().getHeader(Task.X_OPAQUE_ID);
            delegate.execute(() -> {
                if (opaqueId != null) {
                    org.apache.logging.log4j.ThreadContext.put("opaque_id", opaqueId);
                }
                try {
                    cmd.run();
                } finally {
                    org.apache.logging.log4j.ThreadContext.remove("opaque_id");
                }
            });
        };
    }
}
