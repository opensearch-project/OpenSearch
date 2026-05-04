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
 *       and the Arrow schema of the batches the child will feed in. For
 *       single-input shapes this list has size 1; for {@code UNION}-style
 *       multi-input shapes it has one entry per Union branch.</li>
 *   <li>{@code downstream} — sink the backend drains its reduced output
 *       into. The backend owns {@code downstream}'s lifecycle: it must
 *       feed every produced batch and close it when draining is complete.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public record ExchangeSinkContext(String queryId, int stageId, byte[] fragmentBytes, BufferAllocator allocator,
    List<ChildInput> childInputs, ExchangeSink downstream) {

    /** Per-child input descriptor: the child stage id and the schema of its outgoing batches. */
    public record ChildInput(int childStageId, Schema schema) {
    }

    /**
     * Convenience for single-input back-compat. Returns the schema of the sole
     * child input. Throws when {@link #childInputs} contains more than one entry —
     * multi-input callers must inspect {@link #childInputs} directly.
     */
    public Schema inputSchema() {
        if (childInputs.size() != 1) {
            throw new IllegalStateException(
                "inputSchema() requires exactly one child input; got " + childInputs.size() + " — use childInputs() instead"
            );
        }
        return childInputs.get(0).schema();
    }
}
