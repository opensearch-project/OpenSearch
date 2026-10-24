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
 *   <li>{@code taskId} — the parent {@code AnalyticsQueryTask}'s {@code nativeTaskId}:
 *       a JVM-unique id minted by {@link NativeTaskIdManager} (NOT {@code Task#getId()},
 *       which is only unique per node). Backends forward this to the native runtime as
 *       the query-tracking context id so one cancellation call from Java cascades to
 *       every native query scope registered under the same id.</li>
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
 *   <li>{@code importStagingAllocator} — node-scoped allocator the backend stages
 *       Arrow C Data Interface imports on. Unbounded and parented at the root so an
 *       import cannot fail part-way through an array (which strands the whole native
 *       batch — see {@link org.opensearch.analytics.backend.ShardScanExecutionContext#getImportStagingAllocator()}),
 *       and long-lived because the Flight transport keeps charging it after the
 *       importing stream closes. Caller-owned: the backend must never close it, and
 *       must never derive a per-stream child to import onto — an un-closed child stays
 *       registered in the root's {@code childAllocators} map until node restart.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public record ExchangeSinkContext(String queryId, int stageId, long taskId, byte[] fragmentBytes, BufferAllocator allocator, List<
    ChildInput> childInputs, ExchangeSink downstream, BufferAllocator importStagingAllocator) implements CommonExecutionContext {

    /**
     * Stages imports on {@code allocator} itself. For callers whose allocator is already an unbounded
     * root — chiefly tests: that is the pre-staging behaviour, correct but without the mid-import-OOM
     * mitigation described above. Production paths pass a dedicated staging allocator.
     */
    public ExchangeSinkContext(
        String queryId,
        int stageId,
        long taskId,
        byte[] fragmentBytes,
        BufferAllocator allocator,
        List<ChildInput> childInputs,
        ExchangeSink downstream
    ) {
        this(queryId, stageId, taskId, fragmentBytes, allocator, childInputs, downstream, allocator);
    }

    /**
     * Per-child input descriptor: the child stage id and the producer-side plan bytes the
     * backend lowers when it registers the child's input partition. The actual Arrow schema
     * is learned at registration time, not declared here.
     */
    public record ChildInput(int childStageId, byte[] producerPlanBytes) {
    }
}
