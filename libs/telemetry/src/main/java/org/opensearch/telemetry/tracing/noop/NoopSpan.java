/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.tracing.Span;

/**
 * No-op implementation of {@link org.opensearch.telemetry.tracing.Span}
 *
 * @opensearch.internal
 */
@InternalApi
public class NoopSpan implements Span {

    /**
     * No-op Span instance
     */
    public final static NoopSpan INSTANCE = new NoopSpan();

    private NoopSpan() {

    }

    @Override
    public void endSpan() {

    }

    @Override
    public Span getParentSpan() {
        return null;
    }

    @Override
    public String getSpanName() {
        return "noop-span";
    }

    @Override
    public void addAttribute(String key, String value) {

    }

    @Override
    public void addAttribute(String key, Long value) {

    }

    @Override
    public void addAttribute(String key, Double value) {

    }

    @Override
    public void addAttribute(String key, Boolean value) {

    }

    @Override
    public void setError(Exception exception) {

    }

    @Override
    public void addEvent(String event) {

    }

    @Override
    public String getTraceId() {
        return "noop-trace-id";
    }

    @Override
    public String getSpanId() {
        return "noop-span-id";
    }

    @Override
    public String getAttributeString(String key) {
        return "";
    }

    @Override
    public Boolean getAttributeBoolean(String key) {
        return false;
    }

    @Override
    public Long getAttributeLong(String key) {
        return 0L;
    }

    @Override
    public Double getAttributeDouble(String key) {
        return 0.0;
    }
}
