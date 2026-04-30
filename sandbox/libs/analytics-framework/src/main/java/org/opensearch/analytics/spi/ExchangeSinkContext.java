/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Context passed to {@link ExchangeSinkProvider#createSink} when a
 * coordinator-reduce stage is being set up. Carries everything the backend
 * needs to build an {@link ExchangeSink}: serialized plan, buffer allocator,
 * input schema, and the downstream sink the backend writes results to.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@code queryId} / {@code stageId} — correlation ids for backend logs
 *       and metrics.</li>
 *   <li>{@code fragmentBytes} — backend-specific serialized plan (e.g.
 *       Substrait) the backend will execute over the fed batches.</li>
 *   <li>{@code allocator} — the parent buffer allocator the backend should
 *       derive its own child allocators from. Sharing the allocator tree
 *       keeps output batches within the query's memory accounting.</li>
 *   <li>{@code inputSchema} — Arrow schema of batches fed into the sink
 *       (derived from the single child stage's fragment rowtype).</li>
 *   <li>{@code downstream} — sink the backend drains its reduced output
 *       into. The backend owns {@code downstream}'s lifecycle: it must
 *       feed every produced batch and close it when draining is complete.</li>
 * </ul>
 *
 * <p>Single-sink simplification: this context assumes exactly one input
 * stream. Per-child routing (e.g., joins with multiple inputs of different
 * schemas) will require a richer context and is not modeled yet.
 *
 * @opensearch.internal
 */
public record ExchangeSinkContext(String queryId, int stageId, byte[] fragmentBytes, BufferAllocator allocator, Schema inputSchema,
    ExchangeSink downstream) {
}
