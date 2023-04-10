/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.otel;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;

public interface OtelEventListener {
    Attributes onEvent(Span span);
}
