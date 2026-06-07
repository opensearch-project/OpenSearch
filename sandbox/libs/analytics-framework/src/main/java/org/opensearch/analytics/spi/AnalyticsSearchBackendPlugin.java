/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SPI extension point for backend query engine plugins.
 *
 * <p>A backend plugin is registered via {@link org.opensearch.plugins.ExtensiblePlugin} and
 * exposes three provider interfaces, each covering a distinct concern:
 * <ul>
 *   <li>{@link BackendCapabilityProvider} — capability declarations used by the coordinator-side
 *       planner to determine which backends can handle each operator, predicate, and expression.</li>
 *   <li>{@link SearchExecEngineProvider} — execution engine factory used at the data node to
 *       run a plan fragment and stream results.</li>
 *   <li>{@link FragmentConvertor} — plan serialization used by the planner and potentially
 *       at the data node to convert resolved RelNode fragments into backend-native form.</li>
 * </ul>
 *
 * <p>All three getter methods throw {@link UnsupportedOperationException} by default so that
 * backends fail fast if a component tries to use a capability the backend has not implemented.
 *
 * @opensearch.internal
 */
public interface AnalyticsSearchBackendPlugin {

    /** Unique backend name (e.g., "datafusion", "lucene"). */
    String name();

    /**
     * Returns the capability provider for this backend.
     * Used by the coordinator-side planner ({@code CapabilityRegistry}) to determine
     * which backends can handle each operator, predicate, aggregate call, and expression.
     */
    default BackendCapabilityProvider getCapabilityProvider() {
        throw new UnsupportedOperationException("getCapabilityProvider not implemented for [" + name() + "]");
    }

    /**
     * Returns the execution engine provider for this backend.
     * Used at the data node to create a {@link SearchExecEngineProvider} bound to an execution context.
     */
    default SearchExecEngineProvider getSearchExecEngineProvider() {
        throw new UnsupportedOperationException("getSearchExecEngineProvider not implemented for [" + name() + "]");
    }

    /**
     * Returns the fragment convertor for this backend.
     * Used by the planner to serialize resolved plan fragments into backend-native form,
     * and potentially at the data node for final executable conversion.
     */
    default FragmentConvertor getFragmentConvertor() {
        throw new UnsupportedOperationException("getFragmentConvertor not implemented for [" + name() + "]");
    }

    /**
     * Returns the exchange sink provider for this backend, or {@code null} if the backend
     * cannot act as a coordinator-side executor (i.e., cannot accept Arrow Record Batches
     * from data nodes and run computation over them).
     *
     * <p>Used by the planner to determine which backend handles the coordinator stage,
     * and by the Scheduler to create the sink when the query executes.
     */
    default ExchangeSinkProvider getExchangeSinkProvider() {
        return null;
    }

    /**
     * Returns the instruction handler factory for this backend. Used at the coordinator
     * to create instruction nodes (backend attaches custom config) and at the data node
     * to create handlers that apply instructions to the execution context.
     *
     * <p>Backends that declare {@code supportedDelegations} or participate in multi-stage
     * execution MUST implement this. Validation at startup ensures consistency.
     */
    default FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        throw new UnsupportedOperationException("getInstructionHandlerFactory not implemented for [" + name() + "]");
    }

    /**
     * Prepare a filter delegation handle for the given delegated expressions.
     * Called by Core after all instruction handlers have run, when the plan has delegation.
     *
     * <p>The accepting backend initializes its internal state (e.g., DirectoryReader,
     * QueryShardContext, compiled Queries) and returns a handle that the driving backend
     * will call into during execution.
     *
     * @param expressions the delegated expressions (annotationId + serialized query bytes)
     * @param ctx the shared execution context (Reader, MapperService, IndexSettings)
     * @return a handle the driving backend calls into via FFM upcalls
     */
    default FilterDelegationHandle getFilterDelegationHandle(List<DelegatedExpression> expressions, CommonExecutionContext ctx) {
        throw new UnsupportedOperationException("getFilterDelegationHandle not implemented for [" + name() + "]");
    }

    /**
     * Configure the driving backend to use the given delegation handle during execution.
     * Called by Core after obtaining the handle from the accepting backend.
     *
     * <p>The driving backend registers the handle so that FFM upcalls from Rust
     * (createProvider, createCollector, collectDocs) route to the correct per-query binding.
     *
     * @param contextId      the per-query identifier (task ID), threaded through every FFM upcall
     * @param handle         the delegation handle from the accepting backend
     * @param tracker        the thread tracker for resource attribution, or {@code null}
     * @param backendContext the driving backend's execution context (from instruction handlers)
     * @return a cleanup action that must be called (in a finally block) after query execution
     */
    default Runnable configureFilterDelegation(
        long contextId,
        FilterDelegationHandle handle,
        DelegationThreadTracker tracker,
        BackendExecutionContext backendContext
    ) {
        throw new UnsupportedOperationException("configureFilterDelegation not implemented for [" + name() + "]");
    }

    /**
     * Returns a snapshot of this backend's currently-tracked queries, keyed by {@code contextId}.
     *
     * <p>The map is a point-in-time view — entries can register or drain concurrently on the
     * backend side. Implementations MUST return a non-null map (empty when nothing is tracked)
     * and SHOULD make it unmodifiable so callers cannot mutate backend state.
     *
     * <p>Implementations MAY cap the result to a top-N subset by current memory usage to bound
     * the FFI cost (the DataFusion backend caps at the heaviest 10 live queries). Callers that
     * need a complete enumeration should not rely on this method.
     *
     * <p>Default implementation returns an empty map so backends that do not track per-query
     * metrics don't have to opt in.
     */
    default Map<Long, QueryExecutionMetrics> getTopQueriesByMemory() {
        return Collections.emptyMap();
    }

    /**
     * QTF fetch phase: reads specific rows by global row ID.
     * Row IDs are passed as a BigIntVector for zero-copy transfer to native.
     *
     * @param reader the index reader for the target shard
     * @param rowIdVector Arrow BigIntVector containing global row IDs
     * @param columns column names to read
     * @param allocator Arrow buffer allocator for result import
     * @return a result stream containing the requested rows
     */
    default EngineResultStream fetchByRowIds(
        Reader reader,
        BigIntVector rowIdVector,
        String[] columns,
        BufferAllocator allocator,
        long contextId
    ) {
        throw new UnsupportedOperationException("fetchByRowIds not implemented for [" + name() + "]");
    }

    /**
     * Converts a backend-specific exception into an appropriate OpenSearch exception type.
     *
     * <p>Called by the engine when a fragment execution fails. If the backend recognizes
     * the error (e.g., memory limit exceeded, admission rejected), it returns a converted
     * exception with correct HTTP status semantics. Otherwise returns the original unchanged.
     *
     * <p>Default implementation performs no conversion.
     *
     * @param original the exception from fragment execution
     * @return converted exception, or {@code original} if no conversion applies
     */
    default Exception convertException(Exception original) {
        return original;
    }

    /**
     * Returns the backend's subtree convertor for combining multiple delegated predicates
     * into a single serialized expression, or {@code null} if the backend cannot combine.
     * When {@code null}, the framework falls back to one {@link DelegatedExpression} per leaf.
     */
    default DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
        return null;
    }

    /**
     * Per-function serializers for delegated predicates this backend can accept.
     * Keyed by {@link ScalarFunction} — the framework dispatches to the matching
     * serializer during fragment conversion when a predicate is delegated to this backend.
     */
    default Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
        return Map.of();
    }
}
