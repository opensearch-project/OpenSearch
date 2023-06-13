/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Serializable, immutable search pipeline statistics to be returned via stats APIs.
 *
 * @opensearch.internal
 */
public class SearchPipelineStats implements Writeable, ToXContentFragment {

    private final Stats totalRequestStats;
    private final Stats totalResponseStats;
    private final List<PipelineStats> pipelineStats;
    private final Map<String, PipelineDetailStats> perPipelineProcessorStats;

    public SearchPipelineStats(
        Stats totalRequestStats,
        Stats totalResponseStats,
        List<PipelineStats> pipelineStats,
        Map<String, PipelineDetailStats> perPipelineProcessorStats
    ) {
        this.totalRequestStats = totalRequestStats;
        this.totalResponseStats = totalResponseStats;
        this.pipelineStats = pipelineStats;
        this.perPipelineProcessorStats = perPipelineProcessorStats;
    }

    public SearchPipelineStats(StreamInput in) throws IOException {
        this.totalRequestStats = new Stats(in);
        this.totalResponseStats = new Stats(in);
        int size = in.readVInt();
        List<PipelineStats> pipelineStats = new ArrayList<>(size);
        Map<String, PipelineDetailStats> pipelineDetailStatsMap = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String pipelineId = in.readString();
            Stats pipelineRequestStats = new Stats(in);
            Stats pipelineResponseStats = new Stats(in);
            pipelineStats.add(new PipelineStats(pipelineId, pipelineRequestStats, pipelineResponseStats));
            int numRequestProcessors = in.readVInt();
            List<ProcessorStats> requestProcessorStats = new ArrayList<>(numRequestProcessors);
            for (int j = 0; j < numRequestProcessors; j++) {
                String processorName = in.readString();
                String processorType = in.readString();
                Stats processorStats = new Stats(in);
                requestProcessorStats.add(new ProcessorStats(processorName, processorType, processorStats));
            }
            int numResponseProcessors = in.readVInt();
            List<ProcessorStats> responseProcessorStats = new ArrayList<>(numResponseProcessors);
            for (int j = 0; j < numResponseProcessors; j++) {
                String processorName = in.readString();
                String processorType = in.readString();
                Stats processorStats = new Stats(in);
                responseProcessorStats.add(new ProcessorStats(processorName, processorType, processorStats));
            }
            pipelineDetailStatsMap.put(
                pipelineId,
                new PipelineDetailStats(unmodifiableList(requestProcessorStats), unmodifiableList(responseProcessorStats))
            );
        }
        this.pipelineStats = unmodifiableList(pipelineStats);
        this.perPipelineProcessorStats = unmodifiableMap(pipelineDetailStatsMap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_pipeline");
        builder.startObject("total_request");
        totalRequestStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("total_response");
        totalResponseStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("pipelines");
        for (PipelineStats pipelineStat : pipelineStats) {
            builder.startObject(pipelineStat.pipelineId);
            builder.startObject("request");
            pipelineStat.requestStats.toXContent(builder, params);
            builder.endObject();
            builder.startObject("response");
            pipelineStat.responseStats.toXContent(builder, params);
            builder.endObject();

            PipelineDetailStats pipelineDetailStats = perPipelineProcessorStats.get(pipelineStat.pipelineId);
            builder.startArray("request_processors");
            for (ProcessorStats processorStats : pipelineDetailStats.requestProcessorStats) {
                builder.startObject();
                processorStats.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            builder.startArray("response_processors");
            for (ProcessorStats processorStats : pipelineDetailStats.responseProcessorStats) {
                builder.startObject();
                processorStats.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalRequestStats.writeTo(out);
        totalResponseStats.writeTo(out);
        out.writeVInt(pipelineStats.size());
        for (PipelineStats pipelineStat : pipelineStats) {
            out.writeString(pipelineStat.pipelineId);
            pipelineStat.requestStats.writeTo(out);
            pipelineStat.responseStats.writeTo(out);
            PipelineDetailStats pipelineDetailStats = perPipelineProcessorStats.get(pipelineStat.pipelineId);
            out.writeVInt(pipelineDetailStats.requestProcessorStats.size());
            for (ProcessorStats processorStats : pipelineDetailStats.requestProcessorStats) {
                out.writeString(processorStats.processorName);
                out.writeString(processorStats.processorType);
                processorStats.stats.writeTo(out);
            }
            out.writeVInt(pipelineDetailStats.responseProcessorStats.size());
            for (ProcessorStats processorStats : pipelineDetailStats.responseProcessorStats) {
                out.writeString(processorStats.processorName);
                out.writeString(processorStats.processorType);
                processorStats.stats.writeTo(out);
            }
        }
    }

    static class Builder {
        private Stats totalRequestStats;
        private Stats totalResponseStats;
        private final List<PipelineStats> pipelineStats = new ArrayList<>();
        private final Map<String, List<ProcessorStats>> requestProcessorStatsPerPipeline = new HashMap<>();
        private final Map<String, List<ProcessorStats>> responseProcessorStatsPerPipeline = new HashMap<>();

        Builder withTotalStats(SearchPipelineMetrics totalRequestMetrics, SearchPipelineMetrics totalResponseMetrics) {
            this.totalRequestStats = totalRequestMetrics.createStats();
            this.totalResponseStats = totalResponseMetrics.createStats();
            return this;
        }

        Builder addPipelineStats(
            String pipelineId,
            SearchPipelineMetrics pipelineRequestMetrics,
            SearchPipelineMetrics pipelineResponseMetrics
        ) {
            this.pipelineStats.add(
                new PipelineStats(pipelineId, pipelineRequestMetrics.createStats(), pipelineResponseMetrics.createStats())
            );
            return this;
        }

        Builder addRequestProcessorStats(
            String pipelineId,
            String processorName,
            String processorType,
            SearchPipelineMetrics processorMetrics
        ) {
            this.requestProcessorStatsPerPipeline.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStats(processorName, processorType, processorMetrics.createStats()));
            return this;
        }

        Builder addResponseProcessorStats(
            String pipelineId,
            String processorName,
            String processorType,
            SearchPipelineMetrics processorMetrics
        ) {
            this.responseProcessorStatsPerPipeline.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStats(processorName, processorType, processorMetrics.createStats()));
            return this;
        }

        SearchPipelineStats build() {
            Map<String, PipelineDetailStats> pipelineDetailStatsMap = new TreeMap<>();
            for (PipelineStats pipelineStat : pipelineStats) {
                List<ProcessorStats> requestProcessorStats = requestProcessorStatsPerPipeline.getOrDefault(
                    pipelineStat.pipelineId,
                    emptyList()
                );
                List<ProcessorStats> responseProcessorStats = responseProcessorStatsPerPipeline.getOrDefault(
                    pipelineStat.pipelineId,
                    emptyList()
                );
                PipelineDetailStats pipelineDetailStats = new PipelineDetailStats(
                    unmodifiableList(requestProcessorStats),
                    unmodifiableList(responseProcessorStats)
                );
                pipelineDetailStatsMap.put(pipelineStat.pipelineId, pipelineDetailStats);
            }
            return new SearchPipelineStats(
                totalRequestStats,
                totalResponseStats,
                unmodifiableList(pipelineStats),
                unmodifiableMap(pipelineDetailStatsMap)
            );
        }
    }

    static class PipelineStats {
        private final String pipelineId;
        private final Stats requestStats;
        private final Stats responseStats;

        public PipelineStats(String pipelineId, Stats requestStats, Stats responseStats) {
            this.pipelineId = pipelineId;
            this.requestStats = requestStats;
            this.responseStats = responseStats;
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public Stats getRequestStats() {
            return requestStats;
        }

        public Stats getResponseStats() {
            return responseStats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PipelineStats that = (PipelineStats) o;
            return pipelineId.equals(that.pipelineId) && requestStats.equals(that.requestStats) && responseStats.equals(that.responseStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineId, requestStats, responseStats);
        }
    }

    static class PipelineDetailStats {
        private final List<ProcessorStats> requestProcessorStats;
        private final List<ProcessorStats> responseProcessorStats;

        public PipelineDetailStats(List<ProcessorStats> requestProcessorStats, List<ProcessorStats> responseProcessorStats) {
            this.requestProcessorStats = requestProcessorStats;
            this.responseProcessorStats = responseProcessorStats;
        }

        public List<ProcessorStats> requestProcessorStats() {
            return requestProcessorStats;
        }

        public List<ProcessorStats> responseProcessorStats() {
            return responseProcessorStats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PipelineDetailStats that = (PipelineDetailStats) o;
            return requestProcessorStats.equals(that.requestProcessorStats) && responseProcessorStats.equals(that.responseProcessorStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestProcessorStats, responseProcessorStats);
        }
    }

    static class ProcessorStats implements ToXContentFragment {
        private final String processorName; // type:tag
        private final String processorType;
        private final Stats stats;

        public ProcessorStats(String processorName, String processorType, Stats stats) {
            this.processorName = processorName;
            this.processorType = processorType;
            this.stats = stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProcessorStats that = (ProcessorStats) o;
            return processorName.equals(that.processorName) && processorType.equals(that.processorType) && stats.equals(that.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(processorName, processorType, stats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(processorName);
            builder.field("type", processorType);
            builder.startObject("stats");
            stats.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        String getProcessorName() {
            return processorName;
        }

        String getProcessorType() {
            return processorType;
        }

        Stats getStats() {
            return stats;
        }
    }

    static class Stats implements Writeable, ToXContentFragment {

        private final long count;
        private final long totalTimeInMillis;
        private final long current;
        private final long failedCount;

        public Stats(long count, long totalTimeInMillis, long current, long failedCount) {
            this.count = count;
            this.totalTimeInMillis = totalTimeInMillis;
            this.current = current;
            this.failedCount = failedCount;
        }

        /**
         * Read from a stream.
         */
        public Stats(StreamInput in) throws IOException {
            count = in.readVLong();
            totalTimeInMillis = in.readVLong();
            current = in.readVLong();
            failedCount = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(count);
            out.writeVLong(totalTimeInMillis);
            out.writeVLong(current);
            out.writeVLong(failedCount);
        }

        /**
         * @return The total number of executed operations.
         */
        public long getCount() {
            return count;
        }

        /**
         * @return The total time spent of in millis.
         */
        public long getTotalTimeInMillis() {
            return totalTimeInMillis;
        }

        /**
         * @return The total number of operations currently executing.
         */
        public long getCurrent() {
            return current;
        }

        /**
         * @return The total number of operations that have failed.
         */
        public long getFailedCount() {
            return failedCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("count", count)
                .humanReadableField("time_in_millis", "time", new TimeValue(totalTimeInMillis, TimeUnit.MILLISECONDS))
                .field("current", current)
                .field("failed", failedCount);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SearchPipelineStats.Stats that = (SearchPipelineStats.Stats) o;
            return Objects.equals(count, that.count)
                && Objects.equals(totalTimeInMillis, that.totalTimeInMillis)
                && Objects.equals(failedCount, that.failedCount)
                && Objects.equals(current, that.current);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, totalTimeInMillis, failedCount, current);
        }
    }

    Stats getTotalRequestStats() {
        return totalRequestStats;
    }

    Stats getTotalResponseStats() {
        return totalResponseStats;
    }

    List<PipelineStats> getPipelineStats() {
        return pipelineStats;
    }

    Map<String, PipelineDetailStats> getPerPipelineProcessorStats() {
        return perPipelineProcessorStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchPipelineStats stats = (SearchPipelineStats) o;
        return totalRequestStats.equals(stats.totalRequestStats)
            && totalResponseStats.equals(stats.totalResponseStats)
            && pipelineStats.equals(stats.pipelineStats)
            && perPipelineProcessorStats.equals(stats.perPipelineProcessorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRequestStats, totalResponseStats, pipelineStats, perPipelineProcessorStats);
    }
}
