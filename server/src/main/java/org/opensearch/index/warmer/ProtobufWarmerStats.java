/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.warmer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats collected about the warmer
*
* @opensearch.internal
*/
public class ProtobufWarmerStats implements ProtobufWriteable, ToXContentFragment {

    private long current;

    private long total;

    private long totalTimeInMillis;

    public ProtobufWarmerStats() {

    }

    public ProtobufWarmerStats(CodedInputStream in) throws IOException {
        current = in.readInt64();
        total = in.readInt64();
        totalTimeInMillis = in.readInt64();
    }

    public ProtobufWarmerStats(long current, long total, long totalTimeInMillis) {
        this.current = current;
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
    }

    public void add(long current, long total, long totalTimeInMillis) {
        this.current += current;
        this.total += total;
        this.totalTimeInMillis += totalTimeInMillis;
    }

    public void add(ProtobufWarmerStats warmerStats) {
        if (warmerStats == null) {
            return;
        }
        this.current += warmerStats.current;
        this.total += warmerStats.total;
        this.totalTimeInMillis += warmerStats.totalTimeInMillis;
    }

    public long current() {
        return this.current;
    }

    /**
     * The total number of warmer executed.
    */
    public long total() {
        return this.total;
    }

    /**
     * The total time warmer have been executed (in milliseconds).
    */
    public long totalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time warmer have been executed.
    */
    public TimeValue totalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.WARMER);
        builder.field(Fields.CURRENT, current);
        builder.field(Fields.TOTAL, total);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, totalTime());
        builder.endObject();
        return builder;
    }

    /**
     * Fields for warmer statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String WARMER = "warmer";
        static final String CURRENT = "current";
        static final String TOTAL = "total";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(current);
        out.writeInt64NoTag(total);
        out.writeInt64NoTag(totalTimeInMillis);
    }
}
