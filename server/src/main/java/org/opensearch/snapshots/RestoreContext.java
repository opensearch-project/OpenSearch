/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;

import java.util.List;
import java.util.Objects;

/**
 * What one finished snapshot restore made available, passed to
 * {@link org.opensearch.plugins.RestoreListenerPlugin#onRestore}.
 *
 * @param snapshot        the snapshot that was restored
 * @param restoreUUID     the restore's id, which stays the same if the restore is reported more than once
 * @param restoredIndices the indices whose every shard was restored, under the names they were restored as
 * @param partialFailure  true when at least one shard of this restore was not restored; the indices it belongs to are
 *                        left out of {@code restoredIndices}
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record RestoreContext(Snapshot snapshot, String restoreUUID, List<Index> restoredIndices, boolean partialFailure) {

    public RestoreContext {
        Objects.requireNonNull(snapshot);
        Objects.requireNonNull(restoreUUID);
        restoredIndices = List.copyOf(restoredIndices);
    }
}
