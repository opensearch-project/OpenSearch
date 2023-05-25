/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.flush;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Encapsulates statistics for flush
*
* @opensearch.internal
*/
public class ProtobufFlushStats implements ProtobufWriteable, ToXContentFragment {

    private long total;
    private long periodic;
    private long totalTimeInMillis;

    public ProtobufFlushStats() {

    }

    public ProtobufFlushStats(CodedInputStream in) throws IOException {
        total = in.readInt64();
        totalTimeInMillis = in.readInt64();
        periodic = in.readInt64();
    }

    public ProtobufFlushStats(long total, long periodic, long totalTimeInMillis) {
        this.total = total;
        this.periodic = periodic;
        this.totalTimeInMillis = totalTimeInMillis;
    }

    public void add(long total, long periodic, long totalTimeInMillis) {
        this.total += total;
        this.periodic += periodic;
        this.totalTimeInMillis += totalTimeInMillis;
    }

    public void add(ProtobufFlushStats flushStats) {
        addTotals(flushStats);
    }

    public void addTotals(ProtobufFlushStats flushStats) {
        if (flushStats == null) {
            return;
        }
        this.total += flushStats.total;
        this.periodic += flushStats.periodic;
        this.totalTimeInMillis += flushStats.totalTimeInMillis;
    }

    /**
     * The total number of flush executed.
    */
    public long getTotal() {
        return this.total;
    }

    /**
     * The number of flushes that were periodically triggered when translog exceeded the flush threshold.
    */
    public long getPeriodic() {
        return periodic;
    }

    /**
     * The total time merges have been executed (in milliseconds).
    */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time merges have been executed.
    */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FLUSH);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.PERIODIC, periodic);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.endObject();
        return builder;
    }

    /**
     * Fields for flush statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String FLUSH = "flush";
        static final String TOTAL = "total";
        static final String PERIODIC = "periodic";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(total);
        out.writeInt64NoTag(totalTimeInMillis);
        out.writeInt64NoTag(periodic);
    }
}
