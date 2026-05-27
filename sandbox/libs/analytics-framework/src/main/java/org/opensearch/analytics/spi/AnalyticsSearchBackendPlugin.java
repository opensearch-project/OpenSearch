/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.cluster.ClusterState;

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
     * (createProvider, createCollector, collectDocs) route to it.
     *
     * @param handle the delegation handle from the accepting backend
     * @param backendContext the driving backend's execution context (from instruction handlers)
     */
    default void configureFilterDelegation(FilterDelegationHandle handle, BackendExecutionContext backendContext) {
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
     * Install a thread tracker for attribution of delegation callbacks executing on foreign threads.
     * Called after {@link #configureFilterDelegation}. Pass {@code null} to clear.
     */
    default void setDelegationThreadTracker(DelegationThreadTracker tracker) {}

    /**
     * Returns the default number of hash-shuffle output partitions this backend recommends
     * for the current cluster topology, or {@code 1} if the backend does not participate in
     * MPP hash-shuffle. Consulted by the strategy advisor when no explicit partition count is
     * configured via {@code analytics.mpp.shuffle_partitions}.
     *
     * <p>Returning {@code 1} (the default) effectively opts the backend out of HASH_SHUFFLE
     * because the split rule refuses to fire when partitionCount ≤ 1; joins on this backend
     * fall through to BROADCAST or COORDINATOR_CENTRIC. Backends that have a streaming engine
     * capable of consuming hash-partitioned input (DataFusion today; future Velox-style
     * backends) override this to return a real partition count — typically the number of
     * data nodes that hold any shard of the joined indices.
     *
     * <p>This is the engine-level default; an operator can still pin a specific N via the
     * cluster setting. The setting takes precedence when set to a positive value.
     *
     * @param state cluster state at the time of advisor invocation; backends may inspect the
     *              routing table to size partition count to the relevant node set.
     * @return desired partition count (≥ 1); {@code 1} disables shuffle for this backend.
     */
    default int defaultShuffleParallelism(ClusterState state) {
        return 1;
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
}
