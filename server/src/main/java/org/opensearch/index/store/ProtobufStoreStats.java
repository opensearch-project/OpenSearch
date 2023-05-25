/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.store;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics about an OpenSearch Store
*
* @opensearch.internal
*/
public class ProtobufStoreStats implements ProtobufWriteable, ToXContentFragment {

    /**
     * Sentinel value for cases where the shard does not yet know its reserved size so we must fall back to an estimate, for instance
    * prior to receiving the list of files in a peer recovery.
    */
    public static final long UNKNOWN_RESERVED_BYTES = -1L;
    private long sizeInBytes;
    private long reservedSize;

    public ProtobufStoreStats() {

    }

    public ProtobufStoreStats(CodedInputStream in) throws IOException {
        sizeInBytes = in.readInt64();
        reservedSize = in.readInt64();
    }

    /**
     * @param sizeInBytes the size of the store in bytes
    * @param reservedSize a prediction of how much larger the store is expected to grow, or {@link ProtobufStoreStats#UNKNOWN_RESERVED_BYTES}.
    */
    public ProtobufStoreStats(long sizeInBytes, long reservedSize) {
        assert reservedSize == UNKNOWN_RESERVED_BYTES || reservedSize >= 0 : reservedSize;
        this.sizeInBytes = sizeInBytes;
        this.reservedSize = reservedSize;
    }

    public void add(ProtobufStoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;
        reservedSize = ignoreIfUnknown(reservedSize) + ignoreIfUnknown(stats.reservedSize);
    }

    private static long ignoreIfUnknown(long reservedSize) {
        return reservedSize == UNKNOWN_RESERVED_BYTES ? 0L : reservedSize;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    /**
     * A prediction of how much larger this store will eventually grow. For instance, if we are currently doing a peer recovery or restoring
    * a snapshot into this store then we can account for the rest of the recovery using this field. A value of {@code -1B} indicates that
    * the reserved size is unknown.
    */
    public ByteSizeValue getReservedSize() {
        return new ByteSizeValue(reservedSize);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(sizeInBytes);
        out.writeInt64NoTag(reservedSize);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STORE);
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, size());
        builder.humanReadableField(Fields.RESERVED_IN_BYTES, Fields.RESERVED, getReservedSize());
        builder.endObject();
        return builder;
    }

    /**
     * Fields for store statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String STORE = "store";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String RESERVED = "reserved";
        static final String RESERVED_IN_BYTES = "reserved_in_bytes";
    }
}
