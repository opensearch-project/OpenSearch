/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;
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
    private final Map<String, DiagnosticMetric> metricMap;

    /**
     * Constructs a DiagnosticSpan object with the specified underlying span.
     *
     * @param span the underlying span to wrap
     */
    public DiagnosticSpan(Span span) {
        this.delegateSpan = span;
        this.baggage = new HashMap<>();
        this.metricMap = new HashMap<>();
    }

    /**
     * Removes the metric associated with the given ID from the metric map.
     *
     * @param id the ID of the metric to remove
     * @return the removed metric, or null if no metric was found for the given ID
     */
    public DiagnosticMetric removeMetric(String id) {
        return metricMap.remove(id);
    }

    /**
     * Associates the specified metric with the given ID in the metric map.
     *
     * @param id     the ID of the metric
     * @param metric the metric to be associated with the ID
     */
    public void putMetric(String id, DiagnosticMetric metric) {
        metricMap.put(id, metric);
    }
    @Override
    public void endSpan() {
        delegateSpan.endSpan();
    }

    @Override
    public Span getParentSpan() {
        return delegateSpan.getParentSpan();
    }

    @Override
    public String getSpanName() {
        return delegateSpan.getSpanName();
    }

    @Override
    public void addAttribute(String key, String value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        baggage.put(key, value);
        delegateSpan.addAttribute(key, value);
    }

    public Map<String, Object> getAttributes() {
        return baggage;
    }

    @Override
    public void setError(Exception exception) {
        delegateSpan.setError(exception);
    }

    @Override
    public void addEvent(String event) {
        delegateSpan.addEvent(event);
    }

    @Override
    public String getTraceId() {
        return delegateSpan.getTraceId();
    }

    @Override
    public String getSpanId() {
        return delegateSpan.getSpanId();
    }

    @Override
    public boolean hasEnded() {
        return delegateSpan.hasEnded();
    }

    /**
     * returns the original span
     * @return original span
     */
    public Span unwrap() {
        return delegateSpan;
    }
}
