/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;

/**
 * Common supertype for node-level cache statistics snapshots.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface NodeCacheStats extends Writeable, ToXContentFragment {
    // Marker interface — concrete implementations carry their own type-specific accessors.
}
