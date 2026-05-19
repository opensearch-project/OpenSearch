/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

/**
 * Callback invoked when a pool's limit changes.
 *
 * <p>Consumers that mirror a Java-side pool limit to a separate accountant
 * (for example, a native runtime that holds its own memory pool) implement
 * this interface and register it via {@link NativeAllocator#addListener} so
 * resize events propagate without polling.
 *
 * <p>The interface is allocator-agnostic — no Arrow types appear in the
 * signature — so non-Arrow consumers can also subscribe.
 *
 * <p><b>Threading:</b> listeners are invoked synchronously on the thread that
 * caused the resize (a settings-update thread or the rebalancer thread).
 * Implementations must not block or take locks held by the caller; if either
 * is required, dispatch the work to another executor.
 *
 * @opensearch.api
 */
@FunctionalInterface
public interface NativeAllocatorListener {

    /**
     * Invoked after a pool's limit has been updated.
     *
     * @param poolName logical pool name
     * @param newLimit the new limit in bytes
     */
    void onPoolLimitChanged(String poolName, long newLimit);
}
