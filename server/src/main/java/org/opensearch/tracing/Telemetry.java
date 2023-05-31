/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import java.io.Closeable;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Interface for Telemetry providers
 */
public interface Telemetry extends Closeable {

    Span createSpan(String spanName, Span parentSpan, Level level);

    Span extractSpanFromHeader(Map<String, String> header);

    BiConsumer<Map<String, String>, Map<String, Object>> injectSpanInHeader();

    void close();

}
