/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.VectorSchemaRoot;

class FlightUtils {

    private FlightUtils() {}

    static long calculateVectorSchemaRootSize(VectorSchemaRoot root) {
        if (root == null) {
            return 0;
        }
        long totalSize = 0;
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            var vector = root.getVector(i);
            if (vector != null) {
                totalSize += vector.getBufferSize();
            }
        }
        return totalSize;
    }

    /** Cause-chain depth included in {@link #causeSummary}; deeper causes are elided. */
    private static final int MAX_CAUSE_DEPTH = 5;

    /**
     * Renders {@code t} and its cause chain as a single line, e.g.
     * {@code StreamException[UNAVAILABLE, ...]; caused by: FlightRuntimeException[...]}.
     *
     * <p>This exists so the client-side stream paths can report a failure without handing the throwable
     * to log4j. Those paths run on per-stream virtual threads, and log4j's extended stack-trace renderer
     * (which OpenSearch's JSON layout always applies) resolves every frame's declaring class via
     * {@code Class.forName}. The resulting {@code forName0} native frame cannot be unmounted, so a
     * contended classloader lock pins the carrier thread rather than yielding it — see
     * {@code FlightClientChannel#logFailure}. Only class names and messages are read here, so nothing is
     * loaded and no lock is taken.
     */
    static String causeSummary(Throwable t) {
        if (t == null) {
            return "none";
        }
        StringBuilder sb = new StringBuilder();
        Throwable current = t;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (depth > 0) {
                sb.append("; caused by: ");
            }
            sb.append(current.getClass().getSimpleName());
            if (current.getMessage() != null) {
                sb.append('[').append(current.getMessage()).append(']');
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        if (current != null) {
            sb.append("; ...");
        }
        return sb.toString();
    }
}
