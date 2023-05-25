/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.cluster.coordination;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PendingClusterStatsQueue
*
* @opensearch.internal
*/
public class ProtobufPendingClusterStateStats implements ProtobufWriteable, ToXContentFragment {

    private final int total;
    private final int pending;
    private final int committed;

    public ProtobufPendingClusterStateStats(int total, int pending, int committed) {
        this.total = total;
        this.pending = pending;
        this.committed = committed;
    }

    public ProtobufPendingClusterStateStats(CodedInputStream in) throws IOException {
        total = in.readInt32();
        pending = in.readInt32();
        committed = in.readInt32();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(total);
        out.writeInt32NoTag(pending);
        out.writeInt32NoTag(committed);
    }

    public int getCommitted() {
        return committed;
    }

    public int getPending() {
        return pending;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.QUEUE);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.PENDING, pending);
        builder.field(Fields.COMMITTED, committed);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String QUEUE = "cluster_state_queue";
        static final String TOTAL = "total";
        static final String PENDING = "pending";
        static final String COMMITTED = "committed";
    }

    @Override
    public String toString() {
        return "ProtobufPendingClusterStateStats(total=" + total + ", pending=" + pending + ", committed=" + committed + ")";
    }
}
