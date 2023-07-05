/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.telemetry.tracing.Span;

/**
 * Strict check span processor to validate the spans.
 */
public class StrictCheckSpanProcessor implements SpanProcessor {
    private final Map<String, StackTraceElement[]> spanMap = new ConcurrentHashMap<>();

    /**
     * Base constructor.
     */
    public StrictCheckSpanProcessor() {

    }

    @Override
    public void onStart(Span span) {
        spanMap.put(span.getSpanId(), Thread.currentThread().getStackTrace());
    }

    @Override
    public void onEnd(Span span) {
        spanMap.remove(span.getSpanId());
    }

    /**
     * Ensures that all the spans are closed. Throws exception message with stack trace of the method form
     * where the span was created. We can enhance it to print all the failed spans in a single go based on
     * the usability.
     */
    public void ensureAllSpansAreClosed() {
        if (!spanMap.isEmpty()) {
            for (Map.Entry<String, StackTraceElement[]> entry : spanMap.entrySet()) {
                StackTraceElement[] filteredStackTrace = getFilteredStackTrace(entry.getValue());
                AssertionError error = new AssertionError(
                    String.format(
                        Locale.ROOT,
                        " Total [%d] spans are not ended properly. " + "Find below the stack trace for one of the un-ended span",
                        spanMap.size()
                    )
                );
                error.setStackTrace(filteredStackTrace);
                spanMap.clear();
                throw error;
            }
        }
    }

    /**
     * Clears the state.
     */
    public void clear() {
        spanMap.clear();
    }

    private StackTraceElement[] getFilteredStackTrace(StackTraceElement[] stackTraceElements) {
        int filteredElementsCount = 0;
        while (filteredElementsCount < stackTraceElements.length) {
            String className = stackTraceElements[filteredElementsCount].getClassName();
            if (className.startsWith("java.lang.Thread")
                || className.startsWith("org.opensearch.telemetry")
                || className.startsWith("org.opensearch.tracing")) {
                filteredElementsCount++;
            } else {
                break;
            }
        }
        return Arrays.copyOfRange(stackTraceElements, filteredElementsCount, stackTraceElements.length);
    }
}
