/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

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
     * Configure task-level resource tracking for delegation callbacks executing on foreign threads.
     * Called after {@link #configureFilterDelegation}. Backends should wrap their callback dispatch
     * with start/finish tracking calls for the given task.
     */
    default void configureTaskTracking(org.opensearch.tasks.TaskResourceTrackingService trackingService, long taskId) {}

    /**
     * Clear task tracking state after fragment execution completes.
     */
    default void clearTaskTracking() {}
}
