/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * An interface that represents a tracing span.
 * Spans are created by the Tracer.startSpan method.
 * Span must be ended by calling SpanScope.close which internally calls Span's endSpan.
 *
 * @opensearch.internal
 */
public interface Span {

    /**
     * Ends the span
     */
    void endSpan();

    /**
     * Returns span's parent span
     */
    Span getParentSpan();

    /**
     * Returns the name of the {@link Span}
     */
    String getSpanName();

    /**
     * Adds string type attribute in the span
     *
     * @param key of the attribute
     * @param value value of the attribute
     */
    void addAttribute(String key, String value);

    /**
     * Adds long type attribute in the span
     *
     * @param key of the attribute
     * @param value value of the attribute
     */
    void addAttribute(String key, Long value);

    /**
     * Adds double type attribute in the span
     *
     * @param key of the attribute
     * @param value value of the attribute
     */
    void addAttribute(String key, Double value);

    /**
     * Adds boolean type attribute in the span
     *
     * @param key of the attribute
     * @param value value of the attribute
     */
    void addAttribute(String key, Boolean value);

    /**
     * Records error in the span
     *
     * @param exception exception to be recorded
     */
    void setError(Exception exception);

    /**
     * Adds an event in the span
     *
     * @param event name of the event
     */
    void addEvent(String event);

    /**
     * Returns traceId of the span
     * @return span's traceId
     */
    String getTraceId();

    /**
     * Returns spanId of the span
     * @return span's spanId
     */
    String getSpanId();

}
