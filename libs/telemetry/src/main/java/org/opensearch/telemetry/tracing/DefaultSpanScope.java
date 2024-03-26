/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.tracing.attributes.AttributeType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Default implementation for {@link SpanScope}
 *
 * @opensearch.internal
 */
@InternalApi
class DefaultSpanScope implements SpanScope {
    private final Span span;
    private final SpanScope previousSpanScope;
    private final Span beforeSpan;
    private static final ThreadLocal<SpanScope> spanScopeThreadLocal = new ThreadLocal<>();
    private final TracerContextStorage<String, Span> tracerContextStorage;
    private final Map<String, AttributeType> commonAttributeMap = new HashMap<>();

    /**
     * Constructor
     * @param span span
     * @param previousSpanScope before attached span scope.
     */
    private DefaultSpanScope(
        Span span,
        final Span beforeSpan,
        SpanScope previousSpanScope,
        TracerContextStorage<String, Span> tracerContextStorage
    ) {
        this.span = Objects.requireNonNull(span);
        this.beforeSpan = beforeSpan;
        this.previousSpanScope = previousSpanScope;
        this.tracerContextStorage = tracerContextStorage;
        initializeCommonPropagationAttributes();
    }

    /**
     * Creates the SpanScope object.
     * @param span span.
     * @param tracerContextStorage tracer context storage.
     * @return SpanScope spanScope
     */
    public static SpanScope create(Span span, TracerContextStorage<String, Span> tracerContextStorage) {
        final SpanScope beforeSpanScope = spanScopeThreadLocal.get();
        final Span beforeSpan = tracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
        SpanScope newSpanScope = new DefaultSpanScope(span, beforeSpan, beforeSpanScope, tracerContextStorage);
        return newSpanScope;
    }

    /**
     * Common attributes need to be taken from parent and propagated to child span*
     */
    private void initializeCommonPropagationAttributes() {
        commonAttributeMap.put(TracerContextStorage.INFERRED_SAMPLER, AttributeType.BOOLEAN);
    }

    @Override
    public void close() {
        detach();
    }

    @Override
    public SpanScope attach() {
        spanScopeThreadLocal.set(this);
        tracerContextStorage.put(TracerContextStorage.CURRENT_SPAN, this.span);
        addCommonParentAttributes();
        return this;
    }

    private void addCommonParentAttributes() {
        // This work as common attribute propagator from parent to child
        for (String attribute : commonAttributeMap.keySet()) {
            if (beforeSpan != null && beforeSpan.getAttributes().containsKey(attribute)) {
                AttributeType attributeValue = commonAttributeMap.get(attribute);
                this.storeAttributeValue(attribute, attributeValue);
            }
        }
    }

    private void storeAttributeValue(String attribute, AttributeType attributeType) {
        switch (attributeType) {
            case BOOLEAN:
                span.addAttribute(attribute, (Boolean) beforeSpan.getAttribute(attribute));
                break;
            case LONG:
                span.addAttribute(attribute, (Long) beforeSpan.getAttribute(attribute));
                break;
            case STRING:
                span.addAttribute(attribute, (String) beforeSpan.getAttribute(attribute));
                break;
            case DOUBLE:
                span.addAttribute(attribute, (Double) beforeSpan.getAttribute(attribute));
                break;
            // Add more cases for other types if needed
        }
    }

    private void detach() {
        spanScopeThreadLocal.set(previousSpanScope);
        if (beforeSpan != null) {
            tracerContextStorage.put(TracerContextStorage.CURRENT_SPAN, beforeSpan);
        } else {
            tracerContextStorage.put(TracerContextStorage.CURRENT_SPAN, null);
        }
    }

    @Override
    public Span getSpan() {
        return span;
    }

    static SpanScope getCurrentSpanScope() {
        return spanScopeThreadLocal.get();
    }

}
