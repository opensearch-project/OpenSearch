/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.opensearch.index.shard.SearchOperationListener;

/**
 * Search operation listener that prefetches stored fields for tiered storage indices.
 * Implementation will hook into onPreFetchPhase to prefetch stored fields before the fetch phase,
 * reducing latency for remote storage reads.
 */
public class StoredFieldsPrefetch implements SearchOperationListener {

    /** Constructs a new StoredFieldsPrefetch. */
    public StoredFieldsPrefetch() {}
}
