/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;

import java.util.List;

/**
 * Context passed to {@link ExchangeSinkProvider#createSink} when a
 * coordinator-reduce stage is being set up. Carries everything the backend
 * needs to build an {@link ExchangeSink}: serialized plan, buffer allocator,
 * one or more child input descriptors, and the downstream sink the backend
 * writes results to.
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
 *   <li>{@code childInputs} — one entry per child stage. Each entry carries
 *       the child's stage id (used by the backend to register a per-child
 *       input partition under a stable name like {@code "input-<stageId>"})
 *       and the producer-side plan bytes (e.g. partial-aggregate substrait)
 *       the backend lowers to derive the input schema. For single-input
 *       shapes this list has size 1; for {@code UNION}-style multi-input
 *       shapes it has one entry per Union branch.</li>
 *   <li>{@code downstream} — sink the backend drains its reduced output
 *       into. The backend owns {@code downstream}'s lifecycle: it must
 *       feed every produced batch and close it when draining is complete.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public record ExchangeSinkContext(String queryId, int stageId, byte[] fragmentBytes, BufferAllocator allocator, List<
    ChildInput> childInputs, ExchangeSink downstream) implements CommonExecutionContext {

    /**
     * Per-child input descriptor: the child stage id and the producer-side plan bytes the
     * backend lowers when it registers the child's input partition. The actual Arrow schema
     * is learned at registration time, not declared here.
     */
    public record ChildInput(int childStageId, byte[] producerPlanBytes) {
    }
}
