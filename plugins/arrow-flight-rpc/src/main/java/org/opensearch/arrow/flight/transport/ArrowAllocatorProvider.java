/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.common.annotation.ExperimentalApi;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Node-level Arrow allocator shared across plugins.
 *
 * <p>Every caller of {@link #newChildAllocator(String, long)} gets a child of one
 * {@link RootAllocator}. Cross-plugin buffer handoffs (e.g., producer → Flight stream,
 * Flight stream → consumer) pass Arrow's {@link org.apache.arrow.memory.AllocationManager}
 * associate check, which requires {@code source.getRoot() == target.getRoot()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@SuppressWarnings("removal")
public final class ArrowAllocatorProvider {

    private static final RootAllocator ROOT = AccessController.doPrivileged(
        (PrivilegedAction<RootAllocator>) () -> new RootAllocator(Long.MAX_VALUE)
    );

    private ArrowAllocatorProvider() {}

    /**
     * Creates a named child of the shared root with an independent memory limit.
     * Callers own the returned allocator and must close it.
     *
     * @param name descriptive name for debugging (e.g., "flight", "analytics-search")
     * @param limit maximum bytes this child can allocate
     * @return a new child allocator
     */
    public static BufferAllocator newChildAllocator(String name, long limit) {
        return ROOT.newChildAllocator(name, 0, limit);
    }
}
