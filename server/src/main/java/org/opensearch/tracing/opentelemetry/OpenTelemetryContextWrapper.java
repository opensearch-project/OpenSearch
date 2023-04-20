/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class OpenTelemetryContextWrapper {

    /**
     * Wraps the ExecutorService for context propagation across threads in the ThreadPool.
     * @param executorService executor service to be wrapped
     */
    public static ExecutorService wrapTask(ExecutorService executorService) {
        return new OpenSearchConcurrentExecutorService(executorService);
    }

    /**
     * Passes the context to the provided delegate executor
     * @param executor executor to be wrapped with.
     */
    public static Executor wrapTask(Executor executor) {
        return null;
    }
}
