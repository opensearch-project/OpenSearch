/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.http;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.util.List;
import java.util.Map;

/**
 * HttpTracer helps in creating a {@link Span} which reads the incoming tracing information
 * from the HttpRequest header and propagate the span accordingly.
 *
 * All methods on the Tracer object are multi-thread safe.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface HttpTracer {
    /**
     * Start the span with propagating the tracing info from the HttpRequest header.
     *
     * @param spanName span name.
     * @param header http request header.
     * @param attributes span attributes.
     * @return span.
     */
    Span startSpan(String spanName, Map<String, List<String>> header, Attributes attributes);
}
