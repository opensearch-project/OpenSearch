/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.index.engine.exec.IndexReaderProvider;

import java.util.List;

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
     * (createProvider, createCollector, collectDocs) route to it.
     *
     * @param handle the delegation handle from the accepting backend
     * @param backendContext the driving backend's execution context (from instruction handlers)
     */
    default void configureFilterDelegation(FilterDelegationHandle handle, BackendExecutionContext backendContext) {
        throw new UnsupportedOperationException("configureFilterDelegation not implemented for [" + name() + "]");
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
        IndexReaderProvider.Reader reader,
        org.apache.arrow.vector.BigIntVector rowIdVector,
        String[] columns,
        BufferAllocator allocator
    ) {
        throw new UnsupportedOperationException("fetchByRowIds not implemented for [" + name() + "]");
    }

    /**
     * Install a thread tracker for attribution of delegation callbacks executing on foreign threads.
     * Called after {@link #configureFilterDelegation}. Pass {@code null} to clear.
     */
    default void setDelegationThreadTracker(DelegationThreadTracker tracker) {}
}
