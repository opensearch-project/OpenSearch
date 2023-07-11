/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Interface defining the tracing related context propagation
 *
 * @opensearch.internal
 */
public interface TracingContextPropagator {

    /**
     * Extracts current span from context
     * @param props properties
     * @return current span
     */
    Span extract(Map<String, String> props);

    /**
     * Injects tracing context
     *
     * @param currentSpan the current active span
     * @param setter to add tracing context in map
     */
    void inject(Span currentSpan, BiConsumer<String, String> setter);

}
