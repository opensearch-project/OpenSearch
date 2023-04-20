/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.context.Context;

import java.util.concurrent.ExecutorService;

/**
 * SPI for event listener when a thread picks a task to be executed associated with Context aware ExecutorService.
 */
public interface TaskEventListener {

    /** Invoked when a thread start working a task associated with Context aware ExecutorService {@link org.opensearch.tracing.opentelemetry.OpenTelemetryContextWrapper#wrapTask(ExecutorService)}.
     * Context and Span{@link io.opentelemetry.api.trace.Span} information can be derived by calling Context{@link Context#current()}
     * when OpenTelemetry implementation is chosen, as current thread will have the context propagated to it. Span events can be added as necessary.
     * This can be used to start metering resource consumption by a thread.
     *
     * Note: current thread will have the Context set
     * @param t Thread to start working on a task
     */
    void onStart(String operationName, String eventName, Thread t);

    /** Invoked when a thread completes a task associated with Context aware ExecutorService{@link org.opensearch.tracing.opentelemetry.OpenTelemetryContextWrapper#wrapTask(ExecutorService)}
     * for both success and failure scenarios. Context and Span{@link io.opentelemetry.api.trace.Span} information can
     * be derived by calling Context{@link Context#current()} when OpenTelemetry implementation is chosen, as
     * current thread will have the context propagated to it. Span events can be added as necessary.
     *
     * This can be used to stop metering resource consumption by a thread.
     *
     * @param t Thread which completed the task
     */
    void onEnd(String operationName, String eventName, Thread t);

    /**
     * This is used to check if the TaskEventListener should be called for provided operation and/or event.
     * Contract here is, individual service provider should know which all operations are needs to be onboarded to
     * this TaskEventListener. It doesn't make sense to call all available TaskEventListeners for all operations using
     * current executor service.
     * @param operationName name of the operation associated with a trace.
     * @return
     */
    boolean isApplicable(String operationName, String eventName);
}
