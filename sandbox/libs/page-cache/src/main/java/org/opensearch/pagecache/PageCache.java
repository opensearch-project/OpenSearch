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
 * A node-level cache for units of data read from remote storage.
 *
 * <p>Analogous to an OS page cache, but operating at the SSD tier rather than DRAM —
 * caching data locally to avoid repeated fetches from object storage.
 * This interface is agnostic of entry granularity (fixed blocks or variable ranges)
 * and of the backing implementation.
 *
 * <p>Implementations manage the lifecycle of underlying cache resources.
 * {@link #close()} must be called at node shutdown and must be idempotent.
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
