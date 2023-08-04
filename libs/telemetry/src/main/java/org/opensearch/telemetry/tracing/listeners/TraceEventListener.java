/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listeners;

/**
 * Combines both RunnableEventListener and SpanEventListener. Usually both are used in conjunction.
 */
public interface TraceEventListener extends RunnableEventListener, SpanEventListener {}
