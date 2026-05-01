/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.spi;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Point-in-time snapshot of {@link org.opensearch.blockcache.BlockCache} counters.
 *
 * <p>Emitted for node-stats reporting and logging. The exact metric set captured by
 * any given implementation may be richer; this record carries only the universally
 * available counters that every {@code BlockCache} implementation can be expected
 * to surface.
 *
 * <ul>
 *   <li>{@code hits} — cumulative number of lookups served from the cache.</li>
 *   <li>{@code misses} — cumulative number of lookups that did not find an entry
 *       in the cache.</li>
 *   <li>{@code evictions} — cumulative number of entries removed from the cache
 *       to make room for new entries.</li>
 *   <li>{@code memoryBytesUsed} — current number of bytes occupied by entries in
 *       the in-memory tier.</li>
 *   <li>{@code diskBytesUsed} — current number of bytes occupied by entries in
 *       the on-disk tier (zero for implementations without a disk tier).</li>
 * </ul>
 *
 * <p>Values are a snapshot at the moment the record is constructed; they are not
 * guaranteed to be internally consistent with each other across concurrent
 * cache activity.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlockCacheStats(long hits, long misses, long evictions, long memoryBytesUsed, long diskBytesUsed) {
}
