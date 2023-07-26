/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Serializable, immutable search pipeline statistics to be returned via stats APIs.
 *
 * @opensearch.internal
 */
public class SearchPipelineStats implements Writeable, ToXContentFragment {

    private final OperationStats totalRequestStats;
    private final OperationStats totalResponseStats;
    private final List<PerPipelineStats> perPipelineStats;
    private final Map<String, PipelineDetailStats> perPipelineProcessorStats;

    public SearchPipelineStats(
        OperationStats totalRequestStats,
        OperationStats totalResponseStats,
        List<PerPipelineStats> perPipelineStats,
        Map<String, PipelineDetailStats> perPipelineProcessorStats
    ) {
        this.totalRequestStats = totalRequestStats;
        this.totalResponseStats = totalResponseStats;
        this.perPipelineStats = perPipelineStats;
        this.perPipelineProcessorStats = perPipelineProcessorStats;
    }

    public SearchPipelineStats(StreamInput in) throws IOException {
        this.totalRequestStats = new OperationStats(in);
        this.totalResponseStats = new OperationStats(in);
        int size = in.readVInt();
        List<PerPipelineStats> perPipelineStats = new ArrayList<>(size);
        Map<String, PipelineDetailStats> pipelineDetailStatsMap = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String pipelineId = in.readString();
            OperationStats pipelineRequestStats = new OperationStats(in);
            OperationStats pipelineResponseStats = new OperationStats(in);
            perPipelineStats.add(new PerPipelineStats(pipelineId, pipelineRequestStats, pipelineResponseStats));
            int numRequestProcessors = in.readVInt();
            List<ProcessorStats> requestProcessorStats = new ArrayList<>(numRequestProcessors);
            for (int j = 0; j < numRequestProcessors; j++) {
                String processorName = in.readString();
                String processorType = in.readString();
                OperationStats processorStats = new OperationStats(in);
                requestProcessorStats.add(new ProcessorStats(processorName, processorType, processorStats));
            }
            int numResponseProcessors = in.readVInt();
            List<ProcessorStats> responseProcessorStats = new ArrayList<>(numResponseProcessors);
            for (int j = 0; j < numResponseProcessors; j++) {
                String processorName = in.readString();
                String processorType = in.readString();
                OperationStats processorStats = new OperationStats(in);
                responseProcessorStats.add(new ProcessorStats(processorName, processorType, processorStats));
            }
            pipelineDetailStatsMap.put(
                pipelineId,
                new PipelineDetailStats(unmodifiableList(requestProcessorStats), unmodifiableList(responseProcessorStats))
            );
        }
        this.perPipelineStats = unmodifiableList(perPipelineStats);
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
        for (PerPipelineStats pipelineStat : perPipelineStats) {
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
        out.writeVInt(perPipelineStats.size());
        for (PerPipelineStats pipelineStat : perPipelineStats) {
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
        private OperationStats totalRequestStats;
        private OperationStats totalResponseStats;
        private final List<PerPipelineStats> perPipelineStats = new ArrayList<>();
        private final Map<String, List<ProcessorStats>> requestProcessorStatsPerPipeline = new HashMap<>();
        private final Map<String, List<ProcessorStats>> responseProcessorStatsPerPipeline = new HashMap<>();

        Builder withTotalStats(OperationMetrics totalRequestMetrics, OperationMetrics totalResponseMetrics) {
            this.totalRequestStats = totalRequestMetrics.createStats();
            this.totalResponseStats = totalResponseMetrics.createStats();
            return this;
        }

        Builder addPipelineStats(String pipelineId, OperationMetrics pipelineRequestMetrics, OperationMetrics pipelineResponseMetrics) {
            this.perPipelineStats.add(
                new PerPipelineStats(pipelineId, pipelineRequestMetrics.createStats(), pipelineResponseMetrics.createStats())
            );
            return this;
        }

        Builder addRequestProcessorStats(String pipelineId, String processorName, String processorType, OperationMetrics processorMetrics) {
            this.requestProcessorStatsPerPipeline.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStats(processorName, processorType, processorMetrics.createStats()));
            return this;
        }

        Builder addResponseProcessorStats(
            String pipelineId,
            String processorName,
            String processorType,
            OperationMetrics processorMetrics
        ) {
            this.responseProcessorStatsPerPipeline.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStats(processorName, processorType, processorMetrics.createStats()));
            return this;
        }

        SearchPipelineStats build() {
            Map<String, PipelineDetailStats> pipelineDetailStatsMap = new TreeMap<>();
            for (PerPipelineStats pipelineStat : perPipelineStats) {
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
                unmodifiableList(perPipelineStats),
                unmodifiableMap(pipelineDetailStatsMap)
            );
        }
    }

    static class PerPipelineStats {
        private final String pipelineId;
        private final OperationStats requestStats;
        private final OperationStats responseStats;

        public PerPipelineStats(String pipelineId, OperationStats requestStats, OperationStats responseStats) {
            this.pipelineId = pipelineId;
            this.requestStats = requestStats;
            this.responseStats = responseStats;
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public OperationStats getRequestStats() {
            return requestStats;
        }

        public OperationStats getResponseStats() {
            return responseStats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PerPipelineStats that = (PerPipelineStats) o;
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
        private final OperationStats stats;

        public ProcessorStats(String processorName, String processorType, OperationStats stats) {
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

        OperationStats getStats() {
            return stats;
        }
    }

    OperationStats getTotalRequestStats() {
        return totalRequestStats;
    }

    OperationStats getTotalResponseStats() {
        return totalResponseStats;
    }

    List<PerPipelineStats> getPipelineStats() {
        return perPipelineStats;
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
            && perPipelineStats.equals(stats.perPipelineStats)
            && perPipelineProcessorStats.equals(stats.perPipelineProcessorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRequestStats, totalResponseStats, perPipelineStats, perPipelineProcessorStats);
    }
}
