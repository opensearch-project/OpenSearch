/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.listeners;

/**
 * Consumers of {@link TraceEventListener} should implement this interface to know about addition or removal of
 * TraceEventListener managed by {@link TraceEventListenerService}
 */
public interface TraceEventListenerConsumer {
    /**
     * Callback method invoked when a {@link TraceEventListener} is registered with the specified name.
     *
     * @param name              the name of the trace event listener
     * @param traceEventListener the trace event listener that was registered
     */
    void onTraceEventListenerRegister(String name, TraceEventListener traceEventListener);

    /**
     * Callback method invoked when a {@link TraceEventListener} is deregistered with the specified name.
     *
     * @param name the name of the trace event listener that was deregistered
     */
    void onTraceEventListenerDeregister(String name);

    void onDiagnosisSettingChange(boolean diagnosisEnabled);
}
