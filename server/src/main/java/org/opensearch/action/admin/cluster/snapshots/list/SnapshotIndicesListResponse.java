/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.list;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response payload for snapshot indices list.
 *
 * @opensearch.internal
 */
public class SnapshotIndicesListResponse extends ActionResponse {

    public static class IndexRow {
        public String index;
        public int shardsTotal;
        public int shardsDone;
        public int shardsFailed;
        public long fileCount;
        public long sizeInBytes;
        public long startTimeInMillis;
        public long timeInMillis;

        public IndexRow() {}

        public IndexRow(StreamInput in) throws IOException {
            this.index = in.readString();
            this.shardsTotal = in.readVInt();
            this.shardsDone = in.readVInt();
            this.shardsFailed = in.readVInt();
            this.fileCount = in.readVLong();
            this.sizeInBytes = in.readVLong();
            this.startTimeInMillis = in.readVLong();
            this.timeInMillis = in.readVLong();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeVInt(shardsTotal);
            out.writeVInt(shardsDone);
            out.writeVInt(shardsFailed);
            out.writeVLong(fileCount);
            out.writeVLong(sizeInBytes);
            out.writeVLong(startTimeInMillis);
            out.writeVLong(timeInMillis);
        }
    }

    private List<IndexRow> rows = new ArrayList<>();

    public SnapshotIndicesListResponse() {}

    public SnapshotIndicesListResponse(StreamInput in) throws IOException {
        super(in);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            rows.add(new IndexRow(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rows.size());
        for (IndexRow r : rows) {
            r.writeTo(out);
        }
    }

    public List<IndexRow> rows() { return rows; }
}


