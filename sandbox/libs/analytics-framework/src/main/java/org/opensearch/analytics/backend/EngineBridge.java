/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

/**
 * JNI boundary interface between the query planner (Java) and a native
 * execution engine (e.g., DataFusion/Rust).
 *
 * <p>The bridge has two responsibilities:
 * <ol>
 *   <li>{@link #convertFragment} — serialise a logical plan fragment into
 *       the engine's wire format (e.g., Substrait bytes).</li>
 *   <li>{@link #execute} — hand the serialised plan to the native engine
 *       and obtain an opaque handle to the result stream that lives
 *       entirely in native memory.</li>
 * </ol>
 *
 * <p>Arrow data never crosses the JNI boundary into the JVM heap.
 * Consumers read from the native stream via Arrow Flight or
 * direct native-memory access using the returned handle.
 *
 * @param <Fragment>     serialised plan type (e.g., {@code byte[]} for Substrait)
 * @param <Stream>       result stream handle
 * @param <LogicalPlan>> logical plan type (e.g., Calcite {@code RelNode})
 * @opensearch.internal
 */
public interface EngineBridge<Fragment, Stream, LogicalPlan> {

    /**
     * Initializes this bridge with shard-level execution context.
     *
     * <p>Called once per shard before any {@link #execute} calls. Back-end
     * plugins that need shard-level resources (Lucene readers, query
     * contexts, etc.) should acquire and cache them here for reuse
     * across multiple {@code execute} calls within the same shard.
     *
     * <p>The default implementation is a no-op, suitable for engines
     * that do not need shard-level initialization (e.g., native engines
     * with their own resource management).
     *
     * @param context shard execution context provided by the analytics engine
     */
    default void initialize(ShardExecutionContext context) {}

    /**
     * Converts a logical plan fragment into the native engine's serialised
     * format.
     *
     * @param fragment the logical plan subtree to serialise
     * @return the serialised plan in the engine's wire format
     */
    Fragment convertFragment(LogicalPlan fragment);

    /**
     * Submits the serialised plan to the native engine for execution and
     * returns an opaque handle to the result stream.
     *
     * <p>The returned handle is a pointer into native memory (e.g., a
     * {@code long} address of a Rust {@code RecordBatchStream}). The
     * caller must eventually close the stream through a corresponding
     * native call to avoid leaking resources.
     *
     * @param fragment the serialised plan produced by {@link #convertFragment}
     * @return an opaque handle to the native result stream
     */
    Stream execute(Fragment fragment);

    /**
     * Releases shard-level resources acquired during {@link #initialize}.
     *
     * <p>Called once after all {@code execute} calls for a shard are
     * complete. Back-end plugins should release any cached readers,
     * searchers, or contexts here.
     *
     * <p>The default implementation is a no-op.
     */
    default void close() {}
}
