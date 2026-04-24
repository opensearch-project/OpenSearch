/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache;

import java.io.Closeable;


/**
 * A node-level cache for variable-size blocks of data read from remote storage.
 *
 * <p>Sits at the SSD tier to reduce repeated fetches from object storage.
 * "Block" here is used in the storage sense — a contiguous byte range read
 * as an indivisible I/O unit — not a fixed-size disk sector.
 * Entry granularity is determined by the calling layer (e.g. Parquet column
 * chunks, Lucene segment files) and may vary from kilobytes to tens of megabytes.
 *
 * <p>This interface is agnostic of backing implementation; the current
 * implementation delegates to the Foyer Rust library via FFM.
 *
 * <p>Implementations manage the lifecycle of underlying native resources.
 * {@link #close()} must be called at node shutdown and must be idempotent.
 *
 * @opensearch.experimental
 */
public interface BlockCache extends Closeable {

    /**
     * Release all resources held by this cache.
     *
     * <p>Must be idempotent: calling {@code close()} more than once is a no-op.
     */
    @Override
    void close();
}
