/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autorecover;


import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.MasterNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal request that is used to send changes in index clean to restore from past snapshot status to master
 */
public class UpdateIndexCleanStatusRequest extends MasterNodeRequest<UpdateIndexCleanStatusRequest> {
    private final String indexName;
    private final boolean indexCleanToRestoreFromSnapshot;
    private final long indexCleanToRestoreFromSnapshotUpdateTime;

    public UpdateIndexCleanStatusRequest(StreamInput in) throws IOException {
        super(in);
        indexCleanToRestoreFromSnapshot = in.readBoolean();
        indexCleanToRestoreFromSnapshotUpdateTime = in.readLong();
        indexName = in.readString();
    }

    public UpdateIndexCleanStatusRequest(boolean indexCleanToRestoreFromSnapshot, long indexCleanToRestoreFromSnapshotUpdateTime,
                                         String indexName) {
        this.indexCleanToRestoreFromSnapshot = indexCleanToRestoreFromSnapshot;
        this.indexCleanToRestoreFromSnapshotUpdateTime = indexCleanToRestoreFromSnapshotUpdateTime;
        this.indexName = indexName;
        // By default, we keep trying to post index clean status getting stuck.
        this.masterNodeTimeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(indexCleanToRestoreFromSnapshot);
        out.writeLong(indexCleanToRestoreFromSnapshotUpdateTime);
        out.writeString(indexName);
    }

    @Override
    public String toString() {
        return "Index [" + indexName + "], indexCleanToRestoreFromSnapshot [" + indexCleanToRestoreFromSnapshot + "], " +
            "indexCleanToRestoreFromSnapshotUpdateTime [" + indexCleanToRestoreFromSnapshotUpdateTime + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UpdateIndexCleanStatusRequest that = (UpdateIndexCleanStatusRequest) o;
        return indexCleanToRestoreFromSnapshot == (that.indexCleanToRestoreFromSnapshot) &&
            indexCleanToRestoreFromSnapshotUpdateTime == (that.indexCleanToRestoreFromSnapshotUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexCleanToRestoreFromSnapshot, indexCleanToRestoreFromSnapshotUpdateTime);
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean isIndexCleanToRestoreFromSnapshot() {
        return indexCleanToRestoreFromSnapshot;
    }

    public long getIndexCleanToRestoreFromSnapshotUpdateTime() {
        return indexCleanToRestoreFromSnapshotUpdateTime;
    }
}
