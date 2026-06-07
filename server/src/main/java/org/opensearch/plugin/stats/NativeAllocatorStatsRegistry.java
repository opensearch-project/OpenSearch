/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.Nullable;

import java.util.function.Supplier;

/**
 * Component-published holder for a {@link NativeAllocatorPoolStats} supplier.
 *
 * <p>The plugin that owns the node-level Arrow allocator (today: {@code ArrowBasePlugin})
 * returns an instance of this class from its {@code createComponents()} so the server-side
 * {@code NodeService} can look it up via {@code pluginComponents} and invoke the supplier
 * on each {@code _nodes/stats} call.
 *
 * <p>Why a typed wrapper instead of a {@code Supplier<NativeAllocatorPoolStats>} component:
 * Java type erasure makes parameterised generics indistinguishable in {@code instanceof}
 * filters. Wrapping the supplier in a concrete class gives the server a stable lookup target
 * without leaking arrow-base or its plugin class onto the {@code :server} classpath.
 *
 * <p>Mirrors the cross-plugin discovery shape used elsewhere in {@code Node.java}, e.g. the
 * {@code SearchRequestOperationsListener} {@code instanceof} filter on {@code pluginComponents}.
 *
 * @opensearch.api
 */
public final class NativeAllocatorStatsRegistry {

    private final Supplier<NativeAllocatorPoolStats> supplier;

    /**
     * Constructs a registry wrapping the given supplier.
     *
     * @param supplier produces a fresh {@link NativeAllocatorPoolStats} snapshot on each call,
     *                 or returns {@code null} when the underlying allocator is unavailable
     *                 (e.g. plugin closed). Must not be {@code null} itself.
     */
    public NativeAllocatorStatsRegistry(Supplier<NativeAllocatorPoolStats> supplier) {
        this.supplier = supplier;
    }

    /**
     * Returns the wrapped supplier. Callers invoke this once at lookup time and may store the
     * result for repeated invocation; the supplier is expected to be safe to call concurrently.
     */
    public Supplier<NativeAllocatorPoolStats> supplier() {
        return supplier;
    }

    /**
     * Convenience accessor that invokes the supplier once. Returns {@code null} when the
     * supplier returns {@code null} (e.g. plugin closed) so callers don't need to null-check
     * the supplier reference itself.
     */
    @Nullable
    public NativeAllocatorPoolStats get() {
        return supplier.get();
    }
}
