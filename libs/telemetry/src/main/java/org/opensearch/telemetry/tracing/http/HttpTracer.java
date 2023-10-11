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
import org.opensearch.telemetry.tracing.SpanCreationContext;

import java.util.List;
import java.util.Map;

/**
 * HttpTracer helps in creating a {@link Span} which reads the incoming tracing information
 * from the HttpRequest header and propagate the span accordingly.
 * <p>
 * All methods on the Tracer object are multi-thread safe.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface HttpTracer {
    /**
     * Start the span with propagating the tracing info from the HttpRequest header.
     *
     * @param spanCreationContext span name.
     * @param header http request header.
     * @return span.
     */
    Span startSpan(SpanCreationContext spanCreationContext, Map<String, List<String>> header);
}
