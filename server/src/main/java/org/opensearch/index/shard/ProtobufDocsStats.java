/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.shard;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.StoreStats;

import java.io.IOException;

/**
 * Document statistics
*
* @opensearch.internal
*/
public class ProtobufDocsStats implements ProtobufWriteable, ToXContentFragment {

    private long count = 0;
    private long deleted = 0;
    private long totalSizeInBytes = 0;

    public ProtobufDocsStats() {

    }

    public ProtobufDocsStats(CodedInputStream in) throws IOException {
        count = in.readInt64();
        deleted = in.readInt64();
        totalSizeInBytes = in.readInt64();
    }

    public ProtobufDocsStats(long count, long deleted, long totalSizeInBytes) {
        this.count = count;
        this.deleted = deleted;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public void add(ProtobufDocsStats other) {
        if (other == null) {
            return;
        }
        if (this.totalSizeInBytes == -1) {
            this.totalSizeInBytes = other.totalSizeInBytes;
        } else if (other.totalSizeInBytes != -1) {
            this.totalSizeInBytes += other.totalSizeInBytes;
        }
        this.count += other.count;
        this.deleted += other.deleted;
    }

    public long getCount() {
        return this.count;
    }

    public long getDeleted() {
        return this.deleted;
    }

    /**
     * Returns the total size in bytes of all documents in this stats.
    * This value may be more reliable than {@link StoreStats#getSizeInBytes()} in estimating the index size.
    */
    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    /**
     * Returns the average size in bytes of all documents in this stats.
    */
    public long getAverageSizeInBytes() {
        long totalDocs = count + deleted;
        return totalDocs == 0 ? 0 : totalSizeInBytes / totalDocs;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(count);
        out.writeInt64NoTag(deleted);
        out.writeInt64NoTag(totalSizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCS);
        builder.field(Fields.COUNT, count);
        builder.field(Fields.DELETED, deleted);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for document statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String DOCS = "docs";
        static final String COUNT = "count";
        static final String DELETED = "deleted";
    }
}
