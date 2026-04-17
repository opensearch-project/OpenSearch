/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Coordinator-side sink that accepts Arrow Record Batches streamed from data nodes
 * and runs the root stage computation (final aggregate, sort, etc.) over them.
 *
 * <p>Implementations are backend-specific and created via {@link ExchangeSinkProvider}.
 * Methods will be added as part of the Scheduler and streaming transport implementation.
 *
 * @opensearch.internal
 */
public interface ExchangeSink {
}
