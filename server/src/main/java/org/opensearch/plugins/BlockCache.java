/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * Node-scoped block cache contract — backend-neutral.
 *
 * <p>This interface deliberately carries only lifecycle and observability
 * methods. Backend-specific surface (e.g. Caffeine's pin/unpin reference
 * counting, or Foyer's native cache pointer) lives on concrete subtypes and
 * is consumed by code that explicitly knows which backend it is talking to.
 * Core only ever uses the two methods declared here.
 *
 * <p>A block cache stores variable-size contiguous byte ranges (file ranges,
 * Parquet column chunks, remote-object ranges, etc.). The exact key and
 * value shape is an implementation detail and is not part of this interface
 * — different backends may use path-and-offset keys, repository-and-range
 * keys, native pointers, or anything else.
 *
 * <p>Implementations must be thread-safe and idempotent on {@link #close()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BlockCache extends Closeable {

    /**
     * Release all resources held by this cache. Idempotent: calling more than
     * once must be a no-op.
     */
    @Override
    void close();

    /**
     * Returns a point-in-time snapshot of cache counters.
     *
     * <p>Implementations that do not track a particular metric should return
     * zero for that field rather than throwing. The snapshot is not
     * guaranteed to be internally consistent across concurrent cache
     * activity.
     *
     * @return counter snapshot; never {@code null}
     */
    BlockCacheStats stats();
}
