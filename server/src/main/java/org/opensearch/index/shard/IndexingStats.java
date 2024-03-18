/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.shard;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks indexing statistics
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexingStats implements Writeable, ToXContentFragment {

    /**
     * Internal statistics for indexing
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Stats implements Writeable, ToXContentFragment {

        /**
         * Tracks item level rest category class codes during indexing
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        public static class DocStatusStats implements Writeable, ToXContentFragment {

            final AtomicLong[] docStatusCounter;

            public DocStatusStats() {
                docStatusCounter = new AtomicLong[5];
                for (int i = 0; i < docStatusCounter.length; ++i) {
                    docStatusCounter[i] = new AtomicLong(0);
                }
            }

            public DocStatusStats(StreamInput in) throws IOException {
                docStatusCounter = in.readArray(i -> new AtomicLong(i.readLong()), AtomicLong[]::new);

                assert docStatusCounter.length == 5 : "Length of incoming array should be 5! Got " + docStatusCounter.length;
            }

            /**
             * Increment counter for status
             *
             * @param status {@link RestStatus}
             */
            public void inc(final RestStatus status) {
                add(status, 1L);
            }

            /**
             * Increment counter for status by count
             *
             * @param status {@link RestStatus}
             * @param delta The value to add
             */
            void add(final RestStatus status, final long delta) {
                docStatusCounter[status.getStatusFamilyCode() - 1].addAndGet(delta);
            }

            /**
             * Accumulate stats from the passed Object
             *
             * @param stats Instance storing {@link DocStatusStats}
             */
            public void add(final DocStatusStats stats) {
                if (null == stats) {
                    return;
                }

                for (int i = 0; i < docStatusCounter.length; ++i) {
                    docStatusCounter[i].addAndGet(stats.docStatusCounter[i].longValue());
                }
            }

            public AtomicLong[] getDocStatusCounter() {
                return docStatusCounter;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject(Fields.DOC_STATUS);

                for (int i = 0; i < docStatusCounter.length; ++i) {
                    long value = docStatusCounter[i].longValue();

                    if (value > 0) {
                        String key = i + 1 + "xx";
                        builder.field(key, value);
                    }
                }

                return builder.endObject();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeArray((o, v) -> o.writeLong(v.longValue()), docStatusCounter);
            }

        }

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
        private final DocStatusStats docStatusStats;

        Stats() {
            docStatusStats = new DocStatusStats();
        }

        public Stats(StreamInput in) throws IOException {
            indexCount = in.readVLong();
            indexTimeInMillis = in.readVLong();
            indexCurrent = in.readVLong();
            indexFailedCount = in.readVLong();
            deleteCount = in.readVLong();
            deleteTimeInMillis = in.readVLong();
            deleteCurrent = in.readVLong();
            noopUpdateCount = in.readVLong();
            isThrottled = in.readBoolean();
            throttleTimeInMillis = in.readLong();

            if (in.getVersion().onOrAfter(Version.V_2_11_0)) {
                docStatusStats = in.readOptionalWriteable(DocStatusStats::new);
            } else {
                docStatusStats = null;
            }
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
            long throttleTimeInMillis,
            DocStatusStats docStatusStats
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
            this.docStatusStats = docStatusStats;
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
            isThrottled |= stats.isThrottled; // When combining if one is throttled set result to throttled.

            if (getDocStatusStats() != null) {
                getDocStatusStats().add(stats.getDocStatusStats());
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

        public DocStatusStats getDocStatusStats() {
            return docStatusStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(indexCount);
            out.writeVLong(indexTimeInMillis);
            out.writeVLong(indexCurrent);
            out.writeVLong(indexFailedCount);
            out.writeVLong(deleteCount);
            out.writeVLong(deleteTimeInMillis);
            out.writeVLong(deleteCurrent);
            out.writeVLong(noopUpdateCount);
            out.writeBoolean(isThrottled);
            out.writeLong(throttleTimeInMillis);

            if (out.getVersion().onOrAfter(Version.V_2_11_0)) {
                out.writeOptionalWriteable(docStatusStats);
            }
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

            if (getDocStatusStats() != null) {
                getDocStatusStats().toXContent(builder, params);
            }

            return builder;
        }

    }

    private final Stats totalStats;

    public IndexingStats() {
        totalStats = new Stats();
    }

    public IndexingStats(StreamInput in) throws IOException {
        totalStats = new Stats(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            if (in.readBoolean()) {
                Map<String, Stats> typeStats = in.readMap(StreamInput::readString, Stats::new);
                assert typeStats.size() == 1;
                assert typeStats.containsKey(MapperService.SINGLE_MAPPING_NAME);
            }
        }
    }

    public IndexingStats(Stats totalStats) {
        this.totalStats = totalStats;
    }

    public void add(IndexingStats indexingStats) {
        if (indexingStats == null) {
            return;
        }
        addTotals(indexingStats);
    }

    public void addTotals(IndexingStats indexingStats) {
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
    private static final class Fields {
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
        static final String DOC_STATUS = "doc_status";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeBoolean(false);
        }
    }

}
