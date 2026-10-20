/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Applies an {@link InstructionNode} to the execution context at the data node.
 * Each handler is created per-execution by the backend's
 * {@link FragmentInstructionHandlerFactory#createHandler(InstructionNode)}.
 *
 * @param <N> the concrete instruction node type this handler processes
 * @opensearch.internal
 */
public interface FragmentInstructionHandler<N extends InstructionNode> {

    /**
     * Applies the instruction, reading from Core's context and building upon the
     * backend's accumulated execution context from previous handlers.
     *
     * @param node the instruction node
     * @param commonContext Core-provided context (shard info or reduce info)
     * @param backendContext backend state from previous handler, or {@code null} for the first handler
     * @return updated backend execution context for the next handler or final consumer
     */
    BackendExecutionContext apply(N node, CommonExecutionContext commonContext, BackendExecutionContext backendContext);
}
