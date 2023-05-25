/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.translog;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Translog statistics
*
* @opensearch.internal
*/
public class ProtobufTranslogStats implements ProtobufWriteable, ToXContentFragment {

    private long translogSizeInBytes;
    private int numberOfOperations;
    private long uncommittedSizeInBytes;
    private int uncommittedOperations;
    private long earliestLastModifiedAge;

    public ProtobufTranslogStats() {}

    public ProtobufTranslogStats(CodedInputStream in) throws IOException {
        numberOfOperations = in.readInt32();
        translogSizeInBytes = in.readInt64();
        uncommittedOperations = in.readInt32();
        uncommittedSizeInBytes = in.readInt64();
        earliestLastModifiedAge = in.readInt64();
    }

    public ProtobufTranslogStats(
        int numberOfOperations,
        long translogSizeInBytes,
        int uncommittedOperations,
        long uncommittedSizeInBytes,
        long earliestLastModifiedAge
    ) {
        if (numberOfOperations < 0) {
            throw new IllegalArgumentException("numberOfOperations must be >= 0");
        }
        if (translogSizeInBytes < 0) {
            throw new IllegalArgumentException("translogSizeInBytes must be >= 0");
        }
        if (uncommittedOperations < 0) {
            throw new IllegalArgumentException("uncommittedOperations must be >= 0");
        }
        if (uncommittedSizeInBytes < 0) {
            throw new IllegalArgumentException("uncommittedSizeInBytes must be >= 0");
        }
        if (earliestLastModifiedAge < 0) {
            throw new IllegalArgumentException("earliestLastModifiedAge must be >= 0");
        }
        this.numberOfOperations = numberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
        this.uncommittedSizeInBytes = uncommittedSizeInBytes;
        this.uncommittedOperations = uncommittedOperations;
        this.earliestLastModifiedAge = earliestLastModifiedAge;
    }

    public void add(ProtobufTranslogStats translogStats) {
        if (translogStats == null) {
            return;
        }

        this.numberOfOperations += translogStats.numberOfOperations;
        this.translogSizeInBytes += translogStats.translogSizeInBytes;
        this.uncommittedOperations += translogStats.uncommittedOperations;
        this.uncommittedSizeInBytes += translogStats.uncommittedSizeInBytes;
        if (this.earliestLastModifiedAge == 0) {
            this.earliestLastModifiedAge = translogStats.earliestLastModifiedAge;
        } else {
            this.earliestLastModifiedAge = Math.min(this.earliestLastModifiedAge, translogStats.earliestLastModifiedAge);
        }
    }

    public long getTranslogSizeInBytes() {
        return translogSizeInBytes;
    }

    public int estimatedNumberOfOperations() {
        return numberOfOperations;
    }

    /** the size of the generations in the translog that weren't yet to committed to lucene */
    public long getUncommittedSizeInBytes() {
        return uncommittedSizeInBytes;
    }

    /** the number of operations in generations of the translog that weren't yet to committed to lucene */
    public int getUncommittedOperations() {
        return uncommittedOperations;
    }

    public long getEarliestLastModifiedAge() {
        return earliestLastModifiedAge;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("translog");
        builder.field("operations", numberOfOperations);
        builder.humanReadableField("size_in_bytes", "size", new ByteSizeValue(translogSizeInBytes));
        builder.field("uncommitted_operations", uncommittedOperations);
        builder.humanReadableField("uncommitted_size_in_bytes", "uncommitted_size", new ByteSizeValue(uncommittedSizeInBytes));
        builder.field("earliest_last_modified_age", earliestLastModifiedAge);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(numberOfOperations);
        out.writeInt64NoTag(translogSizeInBytes);
        out.writeInt32NoTag(uncommittedOperations);
        out.writeInt64NoTag(uncommittedSizeInBytes);
        out.writeInt64NoTag(earliestLastModifiedAge);
    }
}
