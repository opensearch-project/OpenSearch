/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pagecache;

import java.io.Closeable;

/**
 * A node-level page cache shared across plugins.
 *
 * <p>Implementations manage the lifecycle of the underlying cache resources.
 * {@link #close()} must be called at node shutdown and must be idempotent.
 *
 * <p>The interface is intentionally minimal — methods will be added as concrete
 * callers are introduced. Planned additions include:
 * <ul>
 *   <li>{@code evict(String prefix)} — invalidate entries when a shard closes</li>
 *   <li>{@code diskCapacityBytes()} — for budget management and stats</li>
 *   <li>{@code diskUsageBytes()} — for stats reporting</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public interface PageCache extends Closeable {

    /**
     * Release all resources held by this cache.
     *
     * <p>Must be idempotent: calling {@code close()} more than once is a no-op.
     */
    @Override
    void close();
}
