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
 */
public interface TracingContextPropagator {

    /**
     * Extracts current span from header
     * @param header Headers map
     * @return current span
     */
    Span extractSpanFromHeader(Map<String, String> header);

    /**
     * Injects current span in header
     * @return consumer to add current span in header map
     */
    BiConsumer<Map<String, String>, Map<String, Object>> injectSpanInHeader();

}
