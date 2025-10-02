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

package org.opensearch.index.search.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
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
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchStats implements Writeable, ToXContentFragment {

    /**
     * Holds statistic values for a particular phase.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class PhaseStatsLongHolder implements Writeable {
        private static final Logger logger = LogManager.getLogger(PhaseStatsLongHolder.class);
        long current;
        long total;
        long timeInMillis;

        public long getCurrent() {
            return current;
        }

        public long getTotal() {
            return total;
        }

        public long getTimeInMillis() {
            return timeInMillis;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (current < 0) {
                out.writeVLong(0);
            } else {
                out.writeVLong(current);
            }
            out.writeVLong(total);
            out.writeVLong(timeInMillis);
        }

        PhaseStatsLongHolder() {
            this(0, 0, 0);
        }

        PhaseStatsLongHolder(long current, long total, long timeInMillis) {
            this.current = current;
            this.total = total;
            this.timeInMillis = timeInMillis;
        }

        PhaseStatsLongHolder(StreamInput in) throws IOException {
            this.current = in.readVLong();
            this.total = in.readVLong();
            this.timeInMillis = in.readVLong();
        }

    }

    /**
     * Holds all requests stats.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class RequestStatsLongHolder {

        Map<String, PhaseStatsLongHolder> requestStatsHolder = new HashMap<>();

        public Map<String, PhaseStatsLongHolder> getRequestStatsHolder() {
            return requestStatsHolder;
        }

        RequestStatsLongHolder() {
            requestStatsHolder.put(Fields.TOOK, new PhaseStatsLongHolder());
            for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                requestStatsHolder.put(searchPhaseName.getName(), new PhaseStatsLongHolder());
            }
        }
    }

    /**
     * Holder of statistics values
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Stats implements Writeable, ToXContentFragment {

        private long queryCount;
        private long queryTimeInMillis;
        private long queryCurrent;
        private long queryFailedCount;

        private long concurrentQueryCount;
        private long concurrentQueryTimeInMillis;
        private long concurrentQueryCurrent;
        private long queryConcurrency;

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

        private long searchIdleReactivateCount;

        private long starTreeQueryCount;
        private long starTreeQueryTimeInMillis;
        private long starTreeQueryCurrent;
        private long starTreeQueryFailed;

        @Nullable
        private RequestStatsLongHolder requestStatsLongHolder;

        public RequestStatsLongHolder getRequestStatsLongHolder() {
            return requestStatsLongHolder;
        }

        Stats() {
            // for internal use, initializes all counts to 0
        }

        /**
         * Private constructor that takes a builder.
         * This is the sole entry point for creating a new Stats object.
         * @param builder The builder instance containing all the values.
         */
        private Stats(Builder builder) {
            this.requestStatsLongHolder = builder.requestStatsLongHolder;
            this.queryCount = builder.queryCount;
            this.queryTimeInMillis = builder.queryTimeInMillis;
            this.queryCurrent = builder.queryCurrent;
            this.queryFailedCount = builder.queryFailedCount;

            this.concurrentQueryCount = builder.concurrentQueryCount;
            this.concurrentQueryTimeInMillis = builder.concurrentQueryTimeInMillis;
            this.concurrentQueryCurrent = builder.concurrentQueryCurrent;
            this.queryConcurrency = builder.queryConcurrency;

            this.fetchCount = builder.fetchCount;
            this.fetchTimeInMillis = builder.fetchTimeInMillis;
            this.fetchCurrent = builder.fetchCurrent;

            this.scrollCount = builder.scrollCount;
            this.scrollTimeInMillis = builder.scrollTimeInMillis;
            this.scrollCurrent = builder.scrollCurrent;

            this.suggestCount = builder.suggestCount;
            this.suggestTimeInMillis = builder.suggestTimeInMillis;
            this.suggestCurrent = builder.suggestCurrent;

            this.pitCount = builder.pitCount;
            this.pitTimeInMillis = builder.pitTimeInMillis;
            this.pitCurrent = builder.pitCurrent;

            this.searchIdleReactivateCount = builder.searchIdleReactivateCount;

            this.starTreeQueryCount = builder.starTreeQueryCount;
            this.starTreeQueryTimeInMillis = builder.starTreeQueryTimeInMillis;
            this.starTreeQueryCurrent = builder.starTreeQueryCurrent;
            this.starTreeQueryFailed = builder.starTreeQueryFailed;
        }

        /**
         * This constructor will be deprecated in 4.0
         * Use Builder to create Stats object
         */
        @Deprecated
        public Stats(
            long queryCount,
            long queryTimeInMillis,
            long queryCurrent,
            long concurrentQueryCount,
            long concurrentQueryTimeInMillis,
            long concurrentQueryCurrent,
            long queryConcurrency,
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
            long suggestCurrent,
            long searchIdleReactivateCount
        ) {
            this.requestStatsLongHolder = new RequestStatsLongHolder();
            this.queryCount = queryCount;
            this.queryTimeInMillis = queryTimeInMillis;
            this.queryCurrent = queryCurrent;

            this.concurrentQueryCount = concurrentQueryCount;
            this.concurrentQueryTimeInMillis = concurrentQueryTimeInMillis;
            this.concurrentQueryCurrent = concurrentQueryCurrent;
            this.queryConcurrency = queryConcurrency;

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

            this.searchIdleReactivateCount = searchIdleReactivateCount;
        }

        private Stats(StreamInput in) throws IOException {
            queryCount = in.readVLong();
            queryTimeInMillis = in.readVLong();
            queryCurrent = in.readVLong();

            fetchCount = in.readVLong();
            fetchTimeInMillis = in.readVLong();
            fetchCurrent = in.readVLong();

            scrollCount = in.readVLong();
            scrollTimeInMillis = in.readVLong();
            scrollCurrent = in.readVLong();

            suggestCount = in.readVLong();
            suggestTimeInMillis = in.readVLong();
            suggestCurrent = in.readVLong();

            if (in.getVersion().onOrAfter(Version.V_2_4_0)) {
                pitCount = in.readVLong();
                pitTimeInMillis = in.readVLong();
                pitCurrent = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_2_11_0)) {
                this.requestStatsLongHolder = new RequestStatsLongHolder();
                requestStatsLongHolder.requestStatsHolder = in.readMap(StreamInput::readString, PhaseStatsLongHolder::new);
            }
            if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
                concurrentQueryCount = in.readVLong();
                concurrentQueryTimeInMillis = in.readVLong();
                concurrentQueryCurrent = in.readVLong();
                queryConcurrency = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_2_14_0)) {
                searchIdleReactivateCount = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_3_2_0)) {
                starTreeQueryCount = in.readVLong();
                starTreeQueryTimeInMillis = in.readVLong();
                starTreeQueryCurrent = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
                queryFailedCount = in.readVLong();
                starTreeQueryFailed = in.readVLong();
            }
        }

        public void add(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryCurrent += stats.queryCurrent;
            queryFailedCount += stats.queryFailedCount;

            concurrentQueryCount += stats.concurrentQueryCount;
            concurrentQueryTimeInMillis += stats.concurrentQueryTimeInMillis;
            concurrentQueryCurrent += stats.concurrentQueryCurrent;
            queryConcurrency += stats.queryConcurrency;

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

            searchIdleReactivateCount += stats.searchIdleReactivateCount;

            starTreeQueryCount += stats.starTreeQueryCount;
            starTreeQueryTimeInMillis += stats.starTreeQueryTimeInMillis;
            starTreeQueryCurrent += stats.starTreeQueryCurrent;
            starTreeQueryFailed += stats.starTreeQueryFailed;
        }

        public void addForClosingShard(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryFailedCount += stats.queryFailedCount;

            concurrentQueryCount += stats.concurrentQueryCount;
            concurrentQueryTimeInMillis += stats.concurrentQueryTimeInMillis;

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
            queryConcurrency += stats.queryConcurrency;

            searchIdleReactivateCount += stats.searchIdleReactivateCount;

            starTreeQueryCount += stats.starTreeQueryCount;
            starTreeQueryTimeInMillis += stats.starTreeQueryTimeInMillis;
            starTreeQueryFailed += stats.starTreeQueryFailed;
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

        public long getQueryFailedCount() {
            return queryFailedCount;
        }

        public long getConcurrentQueryCount() {
            return concurrentQueryCount;
        }

        public TimeValue getConcurrentQueryTime() {
            return new TimeValue(concurrentQueryTimeInMillis);
        }

        public double getConcurrentAvgSliceCount() {
            if (concurrentQueryCount == 0) {
                return 0;
            } else {
                return queryConcurrency / (double) concurrentQueryCount;
            }
        }

        public long getConcurrentQueryTimeInMillis() {
            return concurrentQueryTimeInMillis;
        }

        public long getConcurrentQueryCurrent() {
            return concurrentQueryCurrent;
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

        public long getSearchIdleReactivateCount() {
            return searchIdleReactivateCount;
        }

        public long getStarTreeQueryCount() {
            return starTreeQueryCount;
        }

        public TimeValue getStarTreeQueryTime() {
            return new TimeValue(starTreeQueryTimeInMillis);
        }

        public long getStarTreeQueryTimeInMillis() {
            return starTreeQueryTimeInMillis;
        }

        public long getStarTreeQueryCurrent() {
            return starTreeQueryCurrent;
        }

        public long getStarTreeQueryFailed() {
            return starTreeQueryFailed;
        }

        public static Stats readStats(StreamInput in) throws IOException {
            return new Stats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(queryCount);
            out.writeVLong(queryTimeInMillis);
            out.writeVLong(queryCurrent);

            out.writeVLong(fetchCount);
            out.writeVLong(fetchTimeInMillis);
            out.writeVLong(fetchCurrent);

            out.writeVLong(scrollCount);
            out.writeVLong(scrollTimeInMillis);
            out.writeVLong(scrollCurrent);

            out.writeVLong(suggestCount);
            out.writeVLong(suggestTimeInMillis);
            out.writeVLong(suggestCurrent);

            if (out.getVersion().onOrAfter(Version.V_2_4_0)) {
                out.writeVLong(pitCount);
                out.writeVLong(pitTimeInMillis);
                out.writeVLong(pitCurrent);
            }

            if (out.getVersion().onOrAfter(Version.V_2_11_0)) {
                if (requestStatsLongHolder == null) {
                    requestStatsLongHolder = new RequestStatsLongHolder();
                }
                requestStatsLongHolder.requestStatsHolder.forEach((phaseName, phaseStats) -> {
                    if (phaseStats.current < 0) {
                        PhaseStatsLongHolder.logger.warn(
                            "SearchRequestStats 'current' is negative for phase '{}': {}",
                            phaseName,
                            phaseStats.current
                        );
                    }
                });
                out.writeMap(
                    requestStatsLongHolder.getRequestStatsHolder(),
                    StreamOutput::writeString,
                    (stream, stats) -> stats.writeTo(stream)
                );
            }

            if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
                out.writeVLong(concurrentQueryCount);
                out.writeVLong(concurrentQueryTimeInMillis);
                out.writeVLong(concurrentQueryCurrent);
                out.writeVLong(queryConcurrency);
            }

            if (out.getVersion().onOrAfter(Version.V_2_14_0)) {
                out.writeVLong(searchIdleReactivateCount);
            }

            if (out.getVersion().onOrAfter(Version.V_3_2_0)) {
                out.writeVLong(starTreeQueryCount);
                out.writeVLong(starTreeQueryTimeInMillis);
                out.writeVLong(starTreeQueryCurrent);
            }

            if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
                out.writeVLong(queryFailedCount);
                out.writeVLong(starTreeQueryFailed);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.QUERY_TOTAL, queryCount);
            builder.humanReadableField(Fields.QUERY_TIME_IN_MILLIS, Fields.QUERY_TIME, getQueryTime());
            builder.field(Fields.QUERY_CURRENT, queryCurrent);
            builder.field(Fields.QUERY_FAILED_TOTAL, queryFailedCount);

            builder.field(Fields.CONCURRENT_QUERY_TOTAL, concurrentQueryCount);
            builder.humanReadableField(Fields.CONCURRENT_QUERY_TIME_IN_MILLIS, Fields.CONCURRENT_QUERY_TIME, getConcurrentQueryTime());
            builder.field(Fields.CONCURRENT_QUERY_CURRENT, concurrentQueryCurrent);
            builder.field(Fields.CONCURRENT_AVG_SLICE_COUNT, getConcurrentAvgSliceCount());

            builder.field(Fields.STARTREE_QUERY_TOTAL, starTreeQueryCount);
            builder.humanReadableField(Fields.STARTREE_QUERY_TIME_IN_MILLIS, Fields.STARTREE_QUERY_TIME, getStarTreeQueryTime());
            builder.field(Fields.STARTREE_QUERY_CURRENT, getStarTreeQueryCurrent());
            builder.field(Fields.STARTREE_QUERY_FAILED, getStarTreeQueryFailed());

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

            builder.field(Fields.SEARCH_IDLE_REACTIVATE_COUNT_TOTAL, searchIdleReactivateCount);

            if (requestStatsLongHolder != null) {
                builder.startObject(Fields.REQUEST);

                PhaseStatsLongHolder tookStatsLongHolder = requestStatsLongHolder.requestStatsHolder.get(Fields.TOOK);
                if (tookStatsLongHolder != null) {
                    builder.startObject(Fields.TOOK);
                    builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(tookStatsLongHolder.timeInMillis));
                    builder.field(Fields.CURRENT, tookStatsLongHolder.current);
                    builder.field(Fields.TOTAL, tookStatsLongHolder.total);
                    builder.endObject();
                }

                for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                    PhaseStatsLongHolder statsLongHolder = requestStatsLongHolder.requestStatsHolder.get(searchPhaseName.getName());
                    if (statsLongHolder == null) {
                        continue;
                    }
                    builder.startObject(searchPhaseName.getName());
                    builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(statsLongHolder.timeInMillis));
                    builder.field(Fields.CURRENT, statsLongHolder.current);
                    builder.field(Fields.TOTAL, statsLongHolder.total);
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder;
        }

        /**
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private long queryCount = 0;
            private long queryTimeInMillis = 0;
            private long queryCurrent = 0;
            private long queryFailedCount = 0;
            private long concurrentQueryCount = 0;
            private long concurrentQueryTimeInMillis = 0;
            private long concurrentQueryCurrent = 0;
            private long queryConcurrency = 0;
            private long fetchCount = 0;
            private long fetchTimeInMillis = 0;
            private long fetchCurrent = 0;
            private long scrollCount = 0;
            private long scrollTimeInMillis = 0;
            private long scrollCurrent = 0;
            private long suggestCount = 0;
            private long suggestTimeInMillis = 0;
            private long suggestCurrent = 0;
            private long pitCount = 0;
            private long pitTimeInMillis = 0;
            private long pitCurrent = 0;
            private long searchIdleReactivateCount = 0;
            private long starTreeQueryCount = 0;
            private long starTreeQueryTimeInMillis = 0;
            private long starTreeQueryCurrent = 0;
            private long starTreeQueryFailed = 0;
            @Nullable
            private RequestStatsLongHolder requestStatsLongHolder = null;

            public Builder() {}

            public Builder queryCount(long count) {
                this.queryCount = count;
                return this;
            }

            public Builder queryTimeInMillis(long time) {
                this.queryTimeInMillis = time;
                return this;
            }

            public Builder queryCurrent(long current) {
                this.queryCurrent = current;
                return this;
            }

            public Builder queryFailed(long count) {
                this.queryFailedCount = count;
                return this;
            }

            public Builder concurrentQueryCount(long count) {
                this.concurrentQueryCount = count;
                return this;
            }

            public Builder concurrentQueryTimeInMillis(long time) {
                this.concurrentQueryTimeInMillis = time;
                return this;
            }

            public Builder concurrentQueryCurrent(long current) {
                this.concurrentQueryCurrent = current;
                return this;
            }

            public Builder queryConcurrency(long concurrency) {
                this.queryConcurrency = concurrency;
                return this;
            }

            public Builder fetchCount(long count) {
                this.fetchCount = count;
                return this;
            }

            public Builder fetchTimeInMillis(long time) {
                this.fetchTimeInMillis = time;
                return this;
            }

            public Builder fetchCurrent(long current) {
                this.fetchCurrent = current;
                return this;
            }

            public Builder scrollCount(long count) {
                this.scrollCount = count;
                return this;
            }

            public Builder scrollTimeInMillis(long time) {
                this.scrollTimeInMillis = time;
                return this;
            }

            public Builder scrollCurrent(long current) {
                this.scrollCurrent = current;
                return this;
            }

            public Builder suggestCount(long count) {
                this.suggestCount = count;
                return this;
            }

            public Builder suggestTimeInMillis(long time) {
                this.suggestTimeInMillis = time;
                return this;
            }

            public Builder suggestCurrent(long current) {
                this.suggestCurrent = current;
                return this;
            }

            public Builder pitCount(long count) {
                this.pitCount = count;
                return this;
            }

            public Builder pitTimeInMillis(long time) {
                this.pitTimeInMillis = time;
                return this;
            }

            public Builder pitCurrent(long current) {
                this.pitCurrent = current;
                return this;
            }

            public Builder searchIdleReactivateCount(long count) {
                this.searchIdleReactivateCount = count;
                return this;
            }

            public Builder starTreeQueryCount(long count) {
                this.starTreeQueryCount = count;
                return this;
            }

            public Builder starTreeQueryTimeInMillis(long time) {
                this.starTreeQueryTimeInMillis = time;
                return this;
            }

            public Builder starTreeQueryCurrent(long current) {
                this.starTreeQueryCurrent = current;
                return this;
            }

            public Builder starTreeQueryFailed(long count) {
                this.starTreeQueryFailed = count;
                return this;
            }

            /**
             * Creates a {@link Stats} object from the builder's current state.
             * @return A new Stats instance.
             */
            public Stats build() {
                return new Stats(this);
            }
        }
    }

    private final Stats totalStats;
    private long openContexts;

    @Nullable
    private Map<String, Stats> groupStats;

    public SearchStats() {
        totalStats = new Stats();
    }

    // Set the different Request Stats fields in here
    public void setSearchRequestStats(SearchRequestStats searchRequestStats) {
        if (totalStats.requestStatsLongHolder == null) {
            totalStats.requestStatsLongHolder = new RequestStatsLongHolder();
        }

        // Set took stats
        totalStats.requestStatsLongHolder.requestStatsHolder.put(
            Fields.TOOK,
            new PhaseStatsLongHolder(
                searchRequestStats.getTookCurrent(),
                searchRequestStats.getTookTotal(),
                searchRequestStats.getTookMetric()
            )
        );

        // Set phase stats
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            totalStats.requestStatsLongHolder.requestStatsHolder.put(
                searchPhaseName.getName(),
                new PhaseStatsLongHolder(
                    searchRequestStats.getPhaseCurrent(searchPhaseName),
                    searchRequestStats.getPhaseTotal(searchPhaseName),
                    searchRequestStats.getPhaseMetric(searchPhaseName)
                )
            );
        }
    }

    public SearchStats(Stats totalStats, long openContexts, @Nullable Map<String, Stats> groupStats) {
        this.totalStats = totalStats;
        this.openContexts = openContexts;
        this.groupStats = groupStats;
    }

    public SearchStats(StreamInput in) throws IOException {
        totalStats = Stats.readStats(in);
        openContexts = in.readVLong();
        if (in.readBoolean()) {
            groupStats = in.readMap(StreamInput::readString, Stats::readStats);
        }
    }

    public void add(SearchStats searchStats) {
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

    public void addTotals(SearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        totalStats.add(searchStats.totalStats);
    }

    public void addTotalsForClosingShard(SearchStats searchStats) {
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
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
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
        static final String QUERY_FAILED_TOTAL = "query_failed";
        static final String CONCURRENT_QUERY_TOTAL = "concurrent_query_total";
        static final String CONCURRENT_QUERY_TIME = "concurrent_query_time";
        static final String CONCURRENT_QUERY_TIME_IN_MILLIS = "concurrent_query_time_in_millis";
        static final String CONCURRENT_QUERY_CURRENT = "concurrent_query_current";
        static final String CONCURRENT_AVG_SLICE_COUNT = "concurrent_avg_slice_count";
        static final String STARTREE_QUERY_TOTAL = "startree_query_total";
        static final String STARTREE_QUERY_TIME = "startree_query_time";
        static final String STARTREE_QUERY_TIME_IN_MILLIS = "startree_query_time_in_millis";
        static final String STARTREE_QUERY_CURRENT = "startree_query_current";
        static final String STARTREE_QUERY_FAILED = "startree_query_failed";
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
        static final String REQUEST = "request";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String TIME = "time";
        static final String CURRENT = "current";
        static final String TOTAL = "total";
        static final String SEARCH_IDLE_REACTIVATE_COUNT_TOTAL = "search_idle_reactivate_count_total";
        static final String TOOK = "took";

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVLong(openContexts);
        if (groupStats == null || groupStats.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(groupStats, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        }
    }
}
