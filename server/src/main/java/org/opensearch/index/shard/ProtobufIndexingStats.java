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
import org.opensearch.Version;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Map;

/**
 * Tracks indexing statistics
*
* @opensearch.internal
*/
public class ProtobufIndexingStats implements ProtobufWriteable, ToXContentFragment {

    /**
     * Internal statistics for indexing
    *
    * @opensearch.internal
    */
    public static class Stats implements ProtobufWriteable, ToXContentFragment {

        private long indexCount;
        private long indexTimeInMillis;
        private long indexCurrent;
        private long indexFailedCount;
        private long deleteCount;
        private long deleteTimeInMillis;
        private long deleteCurrent;
        private long noopUpdateCount;
        private long throttleTimeInMillis;
        private boolean isThrottled;

        Stats() {}

        public Stats(CodedInputStream in) throws IOException {
            indexCount = in.readInt64();
            indexTimeInMillis = in.readInt64();
            indexCurrent = in.readInt64();
            indexFailedCount = in.readInt64();
            deleteCount = in.readInt64();
            deleteTimeInMillis = in.readInt64();
            deleteCurrent = in.readInt64();
            noopUpdateCount = in.readInt64();
            isThrottled = in.readBool();
            throttleTimeInMillis = in.readInt64();
        }

        public Stats(
            long indexCount,
            long indexTimeInMillis,
            long indexCurrent,
            long indexFailedCount,
            long deleteCount,
            long deleteTimeInMillis,
            long deleteCurrent,
            long noopUpdateCount,
            boolean isThrottled,
            long throttleTimeInMillis
        ) {
            this.indexCount = indexCount;
            this.indexTimeInMillis = indexTimeInMillis;
            this.indexCurrent = indexCurrent;
            this.indexFailedCount = indexFailedCount;
            this.deleteCount = deleteCount;
            this.deleteTimeInMillis = deleteTimeInMillis;
            this.deleteCurrent = deleteCurrent;
            this.noopUpdateCount = noopUpdateCount;
            this.isThrottled = isThrottled;
            this.throttleTimeInMillis = throttleTimeInMillis;
        }

        public void add(Stats stats) {
            indexCount += stats.indexCount;
            indexTimeInMillis += stats.indexTimeInMillis;
            indexCurrent += stats.indexCurrent;
            indexFailedCount += stats.indexFailedCount;

            deleteCount += stats.deleteCount;
            deleteTimeInMillis += stats.deleteTimeInMillis;
            deleteCurrent += stats.deleteCurrent;

            noopUpdateCount += stats.noopUpdateCount;
            throttleTimeInMillis += stats.throttleTimeInMillis;
            if (isThrottled != stats.isThrottled) {
                isThrottled = true; // When combining if one is throttled set result to throttled.
            }
        }

        /**
         * The total number of indexing operations
        */
        public long getIndexCount() {
            return indexCount;
        }

        /**
         * The number of failed indexing operations
        */
        public long getIndexFailedCount() {
            return indexFailedCount;
        }

        /**
         * The total amount of time spend on executing index operations.
        */
        public TimeValue getIndexTime() {
            return new TimeValue(indexTimeInMillis);
        }

        /**
         * Returns the currently in-flight indexing operations.
        */
        public long getIndexCurrent() {
            return indexCurrent;
        }

        /**
         * Returns the number of delete operation executed
        */
        public long getDeleteCount() {
            return deleteCount;
        }

        /**
         * Returns if the index is under merge throttling control
        */
        public boolean isThrottled() {
            return isThrottled;
        }

        /**
         * Gets the amount of time in a TimeValue that the index has been under merge throttling control
        */
        public TimeValue getThrottleTime() {
            return new TimeValue(throttleTimeInMillis);
        }

        /**
         * The total amount of time spend on executing delete operations.
        */
        public TimeValue getDeleteTime() {
            return new TimeValue(deleteTimeInMillis);
        }

        /**
         * Returns the currently in-flight delete operations
        */
        public long getDeleteCurrent() {
            return deleteCurrent;
        }

        public long getNoopUpdateCount() {
            return noopUpdateCount;
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeInt64NoTag(indexCount);
            out.writeInt64NoTag(indexTimeInMillis);
            out.writeInt64NoTag(indexCurrent);
            out.writeInt64NoTag(indexFailedCount);
            out.writeInt64NoTag(deleteCount);
            out.writeInt64NoTag(deleteTimeInMillis);
            out.writeInt64NoTag(deleteCurrent);
            out.writeInt64NoTag(noopUpdateCount);
            out.writeBoolNoTag(isThrottled);
            out.writeInt64NoTag(throttleTimeInMillis);

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.INDEX_TOTAL, indexCount);
            builder.humanReadableField(Fields.INDEX_TIME_IN_MILLIS, Fields.INDEX_TIME, getIndexTime());
            builder.field(Fields.INDEX_CURRENT, indexCurrent);
            builder.field(Fields.INDEX_FAILED, indexFailedCount);

            builder.field(Fields.DELETE_TOTAL, deleteCount);
            builder.humanReadableField(Fields.DELETE_TIME_IN_MILLIS, Fields.DELETE_TIME, getDeleteTime());
            builder.field(Fields.DELETE_CURRENT, deleteCurrent);

            builder.field(Fields.NOOP_UPDATE_TOTAL, noopUpdateCount);

            builder.field(Fields.IS_THROTTLED, isThrottled);
            builder.humanReadableField(Fields.THROTTLED_TIME_IN_MILLIS, Fields.THROTTLED_TIME, getThrottleTime());
            return builder;
        }
    }

    private final Stats totalStats;

    public ProtobufIndexingStats() {
        totalStats = new Stats();
    }

    public ProtobufIndexingStats(CodedInputStream in) throws IOException {
        totalStats = new Stats(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        if (protobufStreamInput.getVersion().before(Version.V_2_0_0)) {
            if (protobufStreamInput.readBoolean()) {
                Map<String, Stats> typeStats = protobufStreamInput.readMap(CodedInputStream::readString, Stats::new);
                assert typeStats.size() == 1;
                assert typeStats.containsKey(MapperService.SINGLE_MAPPING_NAME);
            }
        }
    }

    public ProtobufIndexingStats(Stats totalStats) {
        this.totalStats = totalStats;
    }

    public void add(ProtobufIndexingStats indexingStats) {
        if (indexingStats == null) {
            return;
        }
        addTotals(indexingStats);
    }

    public void addTotals(ProtobufIndexingStats indexingStats) {
        if (indexingStats == null) {
            return;
        }
        totalStats.add(indexingStats.totalStats);
    }

    public Stats getTotal() {
        return this.totalStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEXING);
        totalStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for indexing statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String INDEXING = "indexing";
        static final String INDEX_TOTAL = "index_total";
        static final String INDEX_TIME = "index_time";
        static final String INDEX_TIME_IN_MILLIS = "index_time_in_millis";
        static final String INDEX_CURRENT = "index_current";
        static final String INDEX_FAILED = "index_failed";
        static final String DELETE_TOTAL = "delete_total";
        static final String DELETE_TIME = "delete_time";
        static final String DELETE_TIME_IN_MILLIS = "delete_time_in_millis";
        static final String DELETE_CURRENT = "delete_current";
        static final String NOOP_UPDATE_TOTAL = "noop_update_total";
        static final String IS_THROTTLED = "is_throttled";
        static final String THROTTLED_TIME_IN_MILLIS = "throttle_time_in_millis";
        static final String THROTTLED_TIME = "throttle_time";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        totalStats.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        if (protobufStreamOutput.getVersion().before(Version.V_2_0_0)) {
            out.writeBoolNoTag(false);
        }
    }
}
