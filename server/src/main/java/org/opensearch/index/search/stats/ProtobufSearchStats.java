/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.search.stats;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates stats for search time
*
* @opensearch.internal
*/
public class ProtobufSearchStats implements ProtobufWriteable, ToXContentFragment {

    /**
     * Statistics for search
    *
    * @opensearch.internal
    */
    public static class Stats implements ProtobufWriteable, ToXContentFragment {

        private long queryCount;
        private long queryTimeInMillis;
        private long queryCurrent;

        private long fetchCount;
        private long fetchTimeInMillis;
        private long fetchCurrent;

        private long scrollCount;
        private long scrollTimeInMillis;
        private long scrollCurrent;

        private long suggestCount;
        private long suggestTimeInMillis;
        private long suggestCurrent;

        private long pitCount;
        private long pitTimeInMillis;
        private long pitCurrent;

        private Stats() {
            // for internal use, initializes all counts to 0
        }

        public Stats(
            long queryCount,
            long queryTimeInMillis,
            long queryCurrent,
            long fetchCount,
            long fetchTimeInMillis,
            long fetchCurrent,
            long scrollCount,
            long scrollTimeInMillis,
            long scrollCurrent,
            long pitCount,
            long pitTimeInMillis,
            long pitCurrent,
            long suggestCount,
            long suggestTimeInMillis,
            long suggestCurrent
        ) {
            this.queryCount = queryCount;
            this.queryTimeInMillis = queryTimeInMillis;
            this.queryCurrent = queryCurrent;

            this.fetchCount = fetchCount;
            this.fetchTimeInMillis = fetchTimeInMillis;
            this.fetchCurrent = fetchCurrent;

            this.scrollCount = scrollCount;
            this.scrollTimeInMillis = scrollTimeInMillis;
            this.scrollCurrent = scrollCurrent;

            this.suggestCount = suggestCount;
            this.suggestTimeInMillis = suggestTimeInMillis;
            this.suggestCurrent = suggestCurrent;

            this.pitCount = pitCount;
            this.pitTimeInMillis = pitTimeInMillis;
            this.pitCurrent = pitCurrent;
        }

        private Stats(CodedInputStream in) throws IOException {
            queryCount = in.readInt64();
            queryTimeInMillis = in.readInt64();
            queryCurrent = in.readInt64();

            fetchCount = in.readInt64();
            fetchTimeInMillis = in.readInt64();
            fetchCurrent = in.readInt64();

            scrollCount = in.readInt64();
            scrollTimeInMillis = in.readInt64();
            scrollCurrent = in.readInt64();

            suggestCount = in.readInt64();
            suggestTimeInMillis = in.readInt64();
            suggestCurrent = in.readInt64();

            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
            if (protobufStreamInput.getVersion().onOrAfter(Version.V_2_4_0)) {
                pitCount = in.readInt64();
                pitTimeInMillis = in.readInt64();
                pitCurrent = in.readInt64();
            }
        }

        public void add(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryCurrent += stats.queryCurrent;

            fetchCount += stats.fetchCount;
            fetchTimeInMillis += stats.fetchTimeInMillis;
            fetchCurrent += stats.fetchCurrent;

            scrollCount += stats.scrollCount;
            scrollTimeInMillis += stats.scrollTimeInMillis;
            scrollCurrent += stats.scrollCurrent;

            suggestCount += stats.suggestCount;
            suggestTimeInMillis += stats.suggestTimeInMillis;
            suggestCurrent += stats.suggestCurrent;

            pitCount += stats.pitCount;
            pitTimeInMillis += stats.pitTimeInMillis;
            pitCurrent += stats.pitCurrent;
        }

        public void addForClosingShard(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;

            fetchCount += stats.fetchCount;
            fetchTimeInMillis += stats.fetchTimeInMillis;

            scrollCount += stats.scrollCount;
            scrollTimeInMillis += stats.scrollTimeInMillis;
            // need consider the count of the shard's current scroll
            scrollCount += stats.scrollCurrent;

            suggestCount += stats.suggestCount;
            suggestTimeInMillis += stats.suggestTimeInMillis;

            pitCount += stats.pitCount;
            pitTimeInMillis += stats.pitTimeInMillis;
            pitCurrent += stats.pitCurrent;
        }

        public long getQueryCount() {
            return queryCount;
        }

        public TimeValue getQueryTime() {
            return new TimeValue(queryTimeInMillis);
        }

        public long getQueryTimeInMillis() {
            return queryTimeInMillis;
        }

        public long getQueryCurrent() {
            return queryCurrent;
        }

        public long getFetchCount() {
            return fetchCount;
        }

        public TimeValue getFetchTime() {
            return new TimeValue(fetchTimeInMillis);
        }

        public long getFetchTimeInMillis() {
            return fetchTimeInMillis;
        }

        public long getFetchCurrent() {
            return fetchCurrent;
        }

        public long getScrollCount() {
            return scrollCount;
        }

        public TimeValue getScrollTime() {
            return new TimeValue(scrollTimeInMillis);
        }

        public long getScrollTimeInMillis() {
            return scrollTimeInMillis;
        }

        public long getScrollCurrent() {
            return scrollCurrent;
        }

        public long getPitCount() {
            return pitCount;
        }

        public TimeValue getPitTime() {
            return new TimeValue(pitTimeInMillis);
        }

        public long getPitTimeInMillis() {
            return pitTimeInMillis;
        }

        public long getPitCurrent() {
            return pitCurrent;
        }

        public long getSuggestCount() {
            return suggestCount;
        }

        public long getSuggestTimeInMillis() {
            return suggestTimeInMillis;
        }

        public TimeValue getSuggestTime() {
            return new TimeValue(suggestTimeInMillis);
        }

        public long getSuggestCurrent() {
            return suggestCurrent;
        }

        public static Stats readStats(CodedInputStream in) throws IOException {
            return new Stats(in);
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeInt64NoTag(queryCount);
            out.writeInt64NoTag(queryTimeInMillis);
            out.writeInt64NoTag(queryCurrent);

            out.writeInt64NoTag(fetchCount);
            out.writeInt64NoTag(fetchTimeInMillis);
            out.writeInt64NoTag(fetchCurrent);

            out.writeInt64NoTag(scrollCount);
            out.writeInt64NoTag(scrollTimeInMillis);
            out.writeInt64NoTag(scrollCurrent);

            out.writeInt64NoTag(suggestCount);
            out.writeInt64NoTag(suggestTimeInMillis);
            out.writeInt64NoTag(suggestCurrent);

            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            if (protobufStreamOutput.getVersion().onOrAfter(Version.V_2_4_0)) {
                out.writeInt64NoTag(pitCount);
                out.writeInt64NoTag(pitTimeInMillis);
                out.writeInt64NoTag(pitCurrent);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.QUERY_TOTAL, queryCount);
            builder.humanReadableField(Fields.QUERY_TIME_IN_MILLIS, Fields.QUERY_TIME, getQueryTime());
            builder.field(Fields.QUERY_CURRENT, queryCurrent);

            builder.field(Fields.FETCH_TOTAL, fetchCount);
            builder.humanReadableField(Fields.FETCH_TIME_IN_MILLIS, Fields.FETCH_TIME, getFetchTime());
            builder.field(Fields.FETCH_CURRENT, fetchCurrent);

            builder.field(Fields.SCROLL_TOTAL, scrollCount);
            builder.humanReadableField(Fields.SCROLL_TIME_IN_MILLIS, Fields.SCROLL_TIME, getScrollTime());
            builder.field(Fields.SCROLL_CURRENT, scrollCurrent);

            builder.field(Fields.PIT_TOTAL, pitCount);
            builder.humanReadableField(Fields.PIT_TIME_IN_MILLIS, Fields.PIT_TIME, getPitTime());
            builder.field(Fields.PIT_CURRENT, pitCurrent);

            builder.field(Fields.SUGGEST_TOTAL, suggestCount);
            builder.humanReadableField(Fields.SUGGEST_TIME_IN_MILLIS, Fields.SUGGEST_TIME, getSuggestTime());
            builder.field(Fields.SUGGEST_CURRENT, suggestCurrent);

            return builder;
        }
    }

    private final Stats totalStats;
    private long openContexts;

    @Nullable
    private Map<String, Stats> groupStats;

    public ProtobufSearchStats() {
        totalStats = new Stats();
    }

    public ProtobufSearchStats(Stats totalStats, long openContexts, @Nullable Map<String, Stats> groupStats) {
        this.totalStats = totalStats;
        this.openContexts = openContexts;
        this.groupStats = groupStats;
    }

    public ProtobufSearchStats(CodedInputStream in) throws IOException {
        totalStats = Stats.readStats(in);
        openContexts = in.readInt64();
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        if (protobufStreamInput.readBoolean()) {
            groupStats = protobufStreamInput.readMap(CodedInputStream::readString, Stats::readStats);
        }
    }

    public void add(ProtobufSearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        addTotals(searchStats);
        openContexts += searchStats.openContexts;
        if (searchStats.groupStats != null && !searchStats.groupStats.isEmpty()) {
            if (groupStats == null) {
                groupStats = new HashMap<>(searchStats.groupStats.size());
            }
            for (Map.Entry<String, Stats> entry : searchStats.groupStats.entrySet()) {
                groupStats.putIfAbsent(entry.getKey(), new Stats());
                groupStats.get(entry.getKey()).add(entry.getValue());
            }
        }
    }

    public void addTotals(ProtobufSearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        totalStats.add(searchStats.totalStats);
    }

    public void addTotalsForClosingShard(ProtobufSearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        totalStats.addForClosingShard(searchStats.totalStats);
    }

    public Stats getTotal() {
        return this.totalStats;
    }

    public long getOpenContexts() {
        return this.openContexts;
    }

    @Nullable
    public Map<String, Stats> getGroupStats() {
        return this.groupStats != null ? Collections.unmodifiableMap(this.groupStats) : null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SEARCH);
        builder.field(Fields.OPEN_CONTEXTS, openContexts);
        totalStats.toXContent(builder, params);
        if (groupStats != null && !groupStats.isEmpty()) {
            builder.startObject(Fields.GROUPS);
            for (Map.Entry<String, Stats> entry : groupStats.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }

    /**
     * Fields for search statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String SEARCH = "search";
        static final String OPEN_CONTEXTS = "open_contexts";
        static final String GROUPS = "groups";
        static final String QUERY_TOTAL = "query_total";
        static final String QUERY_TIME = "query_time";
        static final String QUERY_TIME_IN_MILLIS = "query_time_in_millis";
        static final String QUERY_CURRENT = "query_current";
        static final String FETCH_TOTAL = "fetch_total";
        static final String FETCH_TIME = "fetch_time";
        static final String FETCH_TIME_IN_MILLIS = "fetch_time_in_millis";
        static final String FETCH_CURRENT = "fetch_current";
        static final String SCROLL_TOTAL = "scroll_total";
        static final String SCROLL_TIME = "scroll_time";
        static final String SCROLL_TIME_IN_MILLIS = "scroll_time_in_millis";
        static final String SCROLL_CURRENT = "scroll_current";
        static final String PIT_TOTAL = "point_in_time_total";
        static final String PIT_TIME = "point_in_time_time";
        static final String PIT_TIME_IN_MILLIS = "point_in_time_time_in_millis";
        static final String PIT_CURRENT = "point_in_time_current";
        static final String SUGGEST_TOTAL = "suggest_total";
        static final String SUGGEST_TIME = "suggest_time";
        static final String SUGGEST_TIME_IN_MILLIS = "suggest_time_in_millis";
        static final String SUGGEST_CURRENT = "suggest_current";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        totalStats.writeTo(out);
        out.writeInt64NoTag(openContexts);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        if (groupStats == null || groupStats.isEmpty()) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            protobufStreamOutput.writeMap(groupStats, CodedOutputStream::writeStringNoTag, (stream, stats) -> stats.writeTo(stream));
        }
    }
}
