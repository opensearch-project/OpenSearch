/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.PublicApi;

/**
 * Type of Span.
 */
@PublicApi(since = "2.11.0")
public enum SpanKind {
    /**
     * Span represents the client side code.
     */
    CLIENT,
    /**
     * Span represents the server side code.
     */
    SERVER,

    /**
     * Span represents the internal calls. This is the default value of a span type.
     */
    INTERNAL;
}
