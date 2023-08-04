/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.opensearch.telemetry.metrics.MetricPoint;
import org.opensearch.telemetry.tracing.Span;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a diagnostic span that wraps an underlying span implementation.
 * Provides additional functionality to store metrics and baggage for retrieving the span attributes.
 */
public class DiagnosticSpan implements Span {

    private final Span delegateSpan;
    private final Map<String, Object> baggage;
    private final Map<String, MetricPoint> metricMap;

    private final static String SPAN_NAME = "span_name";

    /**
     * Constructs a DiagnosticSpan object with the specified underlying span.
     *
     * @param span the underlying span to wrap
     */
    public DiagnosticSpan(Span span) {
        this.delegateSpan = span;
        this.baggage = new HashMap<>();
        this.metricMap = new HashMap<>();
        baggage.put(SPAN_NAME, span.getSpanName());
    }

    /**
     * Removes the metric associated with the given ID from the metric map.
     *
     * @param id the ID of the metric to remove
     * @return the removed metric, or null if no metric was found for the given ID
     */
    public MetricPoint removeMetric(String id) {
        return metricMap.remove(id);
    }

    /**
     * Associates the specified metric with the given ID in the metric map.
     *
     * @param id     the ID of the metric
     * @param metric the metric to be associated with the ID
     */
    public void putMetric(String id, MetricPoint metric) {
        metricMap.put(id, metric);
    }

    /**
     * Ends the diagnostic span by calling the endSpan method of the underlying span.
     */
    @Override
    public void endSpan() {
        delegateSpan.endSpan();
    }

    /**
     * Returns the parent span of the diagnostic span.
     *
     * @return the parent span of the diagnostic span.
     */
    @Override
    public Span getParentSpan() {
        return delegateSpan.getParentSpan();
    }

    /**
     * Returns the name of the diagnostic span.
     *
     * @return the name of the diagnostic span.
     */
    @Override
    public String getSpanName() {
        return delegateSpan.getSpanName();
    }

    /**
     * Adds a string type attribute to the diagnostic span's baggage and the underlying span.
     *
     * @param key   the key of the attribute.
     * @param value the value of the attribute.
     */
    @Override
    public void addAttribute(String key, String value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    /**
     * Adds a long type attribute to the diagnostic span's baggage and the underlying span.
     *
     * @param key   the key of the attribute.
     * @param value the value of the attribute.
     */
    @Override
    public void addAttribute(String key, Long value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    /**
     * Adds a double type attribute to the diagnostic span's baggage and the underlying span.
     *
     * @param key   the key of the attribute.
     * @param value the value of the attribute.
     */
    @Override
    public void addAttribute(String key, Double value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    /**
     * Adds a boolean type attribute to the diagnostic span's baggage and the underlying span.
     *
     * @param key   the key of the attribute.
     * @param value the value of the attribute.
     */
    @Override
    public void addAttribute(String key, Boolean value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    /**
     * Returns the attributes in the diagnostic span's baggage.
     *
     * @return the attributes in the diagnostic span's baggage.
     */
    public Map<String, Object> getAttributes() {
        return baggage;
    }

    /**
     * Records an error in the diagnostic span by calling the setError method of the underlying span.
     *
     * @param exception the exception to be recorded.
     */
    @Override
    public void setError(Exception exception) {
        delegateSpan.setError(exception);
    }

    /**
     * Adds an event to the diagnostic span by calling the addEvent method of the underlying span.
     *
     * @param event the name of the event.
     */
    @Override
    public void addEvent(String event) {
        delegateSpan.addEvent(event);
    }

    /**
     * Returns the trace ID of the diagnostic span.
     *
     * @return the trace ID of the diagnostic span.
     */
    @Override
    public String getTraceId() {
        return delegateSpan.getTraceId();
    }

    /**
     * Returns the span ID of the diagnostic span.
     *
     * @return the span ID of the diagnostic span.
     */
    @Override
    public String getSpanId() {
        return delegateSpan.getSpanId();
    }

    /**
     * Checks if the diagnostic span has ended.
     *
     * @return true if the diagnostic span has ended, false otherwise.
     */
    @Override
    public boolean hasEnded() {
        return delegateSpan.hasEnded();
    }

    /**
     * Returns the original underlying span.
     *
     * @return the original underlying span.
     */
    public Span unwrap() {
        return delegateSpan;
    }
}
