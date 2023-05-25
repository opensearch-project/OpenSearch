/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.merge;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stores stats about a merge process
*
* @opensearch.internal
*/
public class ProtobufMergeStats implements ProtobufWriteable, ToXContentFragment {

    private long total;
    private long totalTimeInMillis;
    private long totalNumDocs;
    private long totalSizeInBytes;
    private long current;
    private long currentNumDocs;
    private long currentSizeInBytes;

    /** Total millis that large merges were stopped so that smaller merges would finish. */
    private long totalStoppedTimeInMillis;

    /** Total millis that we slept during writes so merge IO is throttled. */
    private long totalThrottledTimeInMillis;

    private long totalBytesPerSecAutoThrottle;

    public ProtobufMergeStats() {

    }

    public ProtobufMergeStats(CodedInputStream in) throws IOException {
        total = in.readInt64();
        totalTimeInMillis = in.readInt64();
        totalNumDocs = in.readInt64();
        totalSizeInBytes = in.readInt64();
        current = in.readInt64();
        currentNumDocs = in.readInt64();
        currentSizeInBytes = in.readInt64();
        // Added in 2.0:
        totalStoppedTimeInMillis = in.readInt64();
        totalThrottledTimeInMillis = in.readInt64();
        totalBytesPerSecAutoThrottle = in.readInt64();
    }

    public void add(
        long totalMerges,
        long totalMergeTime,
        long totalNumDocs,
        long totalSizeInBytes,
        long currentMerges,
        long currentNumDocs,
        long currentSizeInBytes,
        long stoppedTimeMillis,
        long throttledTimeMillis,
        double mbPerSecAutoThrottle
    ) {
        this.total += totalMerges;
        this.totalTimeInMillis += totalMergeTime;
        this.totalNumDocs += totalNumDocs;
        this.totalSizeInBytes += totalSizeInBytes;
        this.current += currentMerges;
        this.currentNumDocs += currentNumDocs;
        this.currentSizeInBytes += currentSizeInBytes;
        this.totalStoppedTimeInMillis += stoppedTimeMillis;
        this.totalThrottledTimeInMillis += throttledTimeMillis;
        long bytesPerSecAutoThrottle = (long) (mbPerSecAutoThrottle * 1024 * 1024);
        if (this.totalBytesPerSecAutoThrottle == Long.MAX_VALUE || bytesPerSecAutoThrottle == Long.MAX_VALUE) {
            this.totalBytesPerSecAutoThrottle = Long.MAX_VALUE;
        } else {
            this.totalBytesPerSecAutoThrottle += bytesPerSecAutoThrottle;
        }
    }

    public void add(ProtobufMergeStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        this.current += mergeStats.current;
        this.currentNumDocs += mergeStats.currentNumDocs;
        this.currentSizeInBytes += mergeStats.currentSizeInBytes;

        addTotals(mergeStats);
    }

    public void addTotals(ProtobufMergeStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        this.total += mergeStats.total;
        this.totalTimeInMillis += mergeStats.totalTimeInMillis;
        this.totalNumDocs += mergeStats.totalNumDocs;
        this.totalSizeInBytes += mergeStats.totalSizeInBytes;
        this.totalStoppedTimeInMillis += mergeStats.totalStoppedTimeInMillis;
        this.totalThrottledTimeInMillis += mergeStats.totalThrottledTimeInMillis;
        if (this.totalBytesPerSecAutoThrottle == Long.MAX_VALUE || mergeStats.totalBytesPerSecAutoThrottle == Long.MAX_VALUE) {
            this.totalBytesPerSecAutoThrottle = Long.MAX_VALUE;
        } else {
            this.totalBytesPerSecAutoThrottle += mergeStats.totalBytesPerSecAutoThrottle;
        }
    }

    /**
     * The total number of merges executed.
    */
    public long getTotal() {
        return this.total;
    }

    /**
     * The total time merges have been executed (in milliseconds).
    */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time large merges were stopped so smaller merges could finish.
    */
    public long getTotalStoppedTimeInMillis() {
        return this.totalStoppedTimeInMillis;
    }

    /**
     * The total time large merges were stopped so smaller merges could finish.
    */
    public TimeValue getTotalStoppedTime() {
        return new TimeValue(totalStoppedTimeInMillis);
    }

    /**
     * The total time merge IO writes were throttled.
    */
    public long getTotalThrottledTimeInMillis() {
        return this.totalThrottledTimeInMillis;
    }

    /**
     * The total time merge IO writes were throttled.
    */
    public TimeValue getTotalThrottledTime() {
        return new TimeValue(totalThrottledTimeInMillis);
    }

    /**
     * The total time merges have been executed.
    */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    public long getTotalNumDocs() {
        return this.totalNumDocs;
    }

    public long getTotalSizeInBytes() {
        return this.totalSizeInBytes;
    }

    public ByteSizeValue getTotalSize() {
        return new ByteSizeValue(totalSizeInBytes);
    }

    public long getTotalBytesPerSecAutoThrottle() {
        return totalBytesPerSecAutoThrottle;
    }

    /**
     * The current number of merges executing.
    */
    public long getCurrent() {
        return this.current;
    }

    public long getCurrentNumDocs() {
        return this.currentNumDocs;
    }

    public long getCurrentSizeInBytes() {
        return this.currentSizeInBytes;
    }

    public ByteSizeValue getCurrentSize() {
        return new ByteSizeValue(currentSizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MERGES);
        builder.field(Fields.CURRENT, current);
        builder.field(Fields.CURRENT_DOCS, currentNumDocs);
        builder.humanReadableField(Fields.CURRENT_SIZE_IN_BYTES, Fields.CURRENT_SIZE, getCurrentSize());
        builder.field(Fields.TOTAL, total);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.field(Fields.TOTAL_DOCS, totalNumDocs);
        builder.humanReadableField(Fields.TOTAL_SIZE_IN_BYTES, Fields.TOTAL_SIZE, getTotalSize());
        builder.humanReadableField(Fields.TOTAL_STOPPED_TIME_IN_MILLIS, Fields.TOTAL_STOPPED_TIME, getTotalStoppedTime());
        builder.humanReadableField(Fields.TOTAL_THROTTLED_TIME_IN_MILLIS, Fields.TOTAL_THROTTLED_TIME, getTotalThrottledTime());
        if (builder.humanReadable() && totalBytesPerSecAutoThrottle != -1) {
            builder.field(Fields.TOTAL_THROTTLE_BYTES_PER_SEC).value(new ByteSizeValue(totalBytesPerSecAutoThrottle).toString());
        }
        builder.field(Fields.TOTAL_THROTTLE_BYTES_PER_SEC_IN_BYTES, totalBytesPerSecAutoThrottle);
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for merge statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String MERGES = "merges";
        static final String CURRENT = "current";
        static final String CURRENT_DOCS = "current_docs";
        static final String CURRENT_SIZE = "current_size";
        static final String CURRENT_SIZE_IN_BYTES = "current_size_in_bytes";
        static final String TOTAL = "total";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String TOTAL_STOPPED_TIME = "total_stopped_time";
        static final String TOTAL_STOPPED_TIME_IN_MILLIS = "total_stopped_time_in_millis";
        static final String TOTAL_THROTTLED_TIME = "total_throttled_time";
        static final String TOTAL_THROTTLED_TIME_IN_MILLIS = "total_throttled_time_in_millis";
        static final String TOTAL_DOCS = "total_docs";
        static final String TOTAL_SIZE = "total_size";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
        static final String TOTAL_THROTTLE_BYTES_PER_SEC_IN_BYTES = "total_auto_throttle_in_bytes";
        static final String TOTAL_THROTTLE_BYTES_PER_SEC = "total_auto_throttle";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(total);
        out.writeInt64NoTag(totalTimeInMillis);
        out.writeInt64NoTag(totalNumDocs);
        out.writeInt64NoTag(totalSizeInBytes);
        out.writeInt64NoTag(current);
        out.writeInt64NoTag(currentNumDocs);
        out.writeInt64NoTag(currentSizeInBytes);
        // Added in 2.0:
        out.writeInt64NoTag(totalStoppedTimeInMillis);
        out.writeInt64NoTag(totalThrottledTimeInMillis);
        out.writeInt64NoTag(totalBytesPerSecAutoThrottle);
    }
}
