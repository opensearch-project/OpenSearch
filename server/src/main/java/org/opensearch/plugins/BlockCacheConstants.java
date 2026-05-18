/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Constants for block cache backends.
 *
 * <p>The constant value is the logical cache name used to look up a registered
 * {@link BlockCache} from a {@link BlockCacheRegistry}. It identifies the <em>role</em>
 * (disk-tier caching) rather than the underlying implementation library.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BlockCacheConstants {

    /**
     * Name of the disk-tier block cache.
     *
     * <p>Currently backed by the Foyer library, but the name is kept
     * implementation-agnostic so it remains stable if the backend changes.
     */
    public static final String DISK_CACHE = "disk";

    private BlockCacheConstants() {}
}
