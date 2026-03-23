/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import java.io.Closeable;

/**
 * Per-query facade that bridges the backend-specific engines with the analytics engine.
 * <p>
 * A bridge is created per query with a reader bound to a catalog snapshot.
 * It converts a logical plan to the engine's native format and executes it,
 * managing the backend context internally.
 *
 * @param <Fragment>    serialised plan type (e.g., {@code byte[]} for Substrait)
 * @param <Stream>      result stream type
 * @param <LogicalPlan> logical plan type (e.g., Calcite {@code RelNode})
 * @opensearch.internal
 */
public interface EngineBridge<Fragment, Stream, LogicalPlan> extends Closeable {

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
}
