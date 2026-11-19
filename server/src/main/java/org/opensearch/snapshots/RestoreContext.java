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
 * @opensearch.experimental
 */
@ExperimentalApi
public final class RestoreContext {

    private final Snapshot snapshot;
    private final String restoreUUID;
    private final List<Index> restoredIndices;
    private final boolean partialFailure;

    /**
     * @param snapshot        the snapshot that was restored
     * @param restoreUUID     the restore's id, which stays the same if the restore is reported more than once
     * @param restoredIndices the indices whose every shard was restored, under the names they were restored as
     * @param partialFailure  true when at least one shard of this restore was not restored; the indices it belongs to
     *                        are left out of {@code restoredIndices}
     */
    public RestoreContext(Snapshot snapshot, String restoreUUID, List<Index> restoredIndices, boolean partialFailure) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.restoreUUID = Objects.requireNonNull(restoreUUID);
        this.restoredIndices = List.copyOf(restoredIndices);
        this.partialFailure = partialFailure;
    }

    /** @return the snapshot that was restored */
    public Snapshot snapshot() {
        return snapshot;
    }

    /** @return the restore's id, which stays the same if the restore is reported more than once */
    public String restoreUUID() {
        return restoreUUID;
    }

    /** @return the indices whose every shard was restored, under the names they were restored as */
    public List<Index> restoredIndices() {
        return restoredIndices;
    }

    /** @return true when at least one shard of this restore was not restored */
    public boolean partialFailure() {
        return partialFailure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreContext that = (RestoreContext) o;
        return partialFailure == that.partialFailure
            && snapshot.equals(that.snapshot)
            && restoreUUID.equals(that.restoreUUID)
            && restoredIndices.equals(that.restoredIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot, restoreUUID, restoredIndices, partialFailure);
    }

    @Override
    public String toString() {
        return "RestoreContext[snapshot="
            + snapshot
            + ", restoreUUID="
            + restoreUUID
            + ", restoredIndices="
            + restoredIndices
            + ", partialFailure="
            + partialFailure
            + "]";
    }
}
