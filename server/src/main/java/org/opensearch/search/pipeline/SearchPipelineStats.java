/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.Version;
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

import reactor.util.annotation.NonNull;

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
    private FactoryDetailStats systemGeneratedFactoryStats;
    private PipelineDetailStats systemGeneratedProcessorStats;

    public SearchPipelineStats(
        OperationStats totalRequestStats,
        OperationStats totalResponseStats,
        List<PerPipelineStats> perPipelineStats,
        Map<String, PipelineDetailStats> perPipelineProcessorStats,
        FactoryDetailStats systemGeneratedFactoryStats,
        PipelineDetailStats systemGeneratedProcessorStats
    ) {
        this.totalRequestStats = totalRequestStats;
        this.totalResponseStats = totalResponseStats;
        this.perPipelineStats = perPipelineStats;
        this.perPipelineProcessorStats = perPipelineProcessorStats;
        this.systemGeneratedFactoryStats = systemGeneratedFactoryStats;
        this.systemGeneratedProcessorStats = systemGeneratedProcessorStats;
    }

    public SearchPipelineStats(StreamInput in) throws IOException {
        this.totalRequestStats = new OperationStats(in);
        this.totalResponseStats = new OperationStats(in);
        int size = in.readVInt();
        List<PerPipelineStats> perPipelineStats = new ArrayList<>(size);
        Map<String, PipelineDetailStats> pipelineDetailStatsMap = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            PerPipelineStats perPipelineStat = new PerPipelineStats(in);
            perPipelineStats.add(perPipelineStat);
            pipelineDetailStatsMap.put(perPipelineStat.pipelineId, new PipelineDetailStats(in));
        }
        this.perPipelineStats = unmodifiableList(perPipelineStats);
        this.perPipelineProcessorStats = unmodifiableMap(pipelineDetailStatsMap);
        if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
            this.systemGeneratedFactoryStats = new FactoryDetailStats(in);
            this.systemGeneratedProcessorStats = new PipelineDetailStats(in);
        }
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
            pipelineDetailStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        if (systemGeneratedProcessorStats != null) {
            builder.startObject("system_generated_processors");
            systemGeneratedProcessorStats.toXContent(builder, params);
            builder.endObject();
        }
        if (systemGeneratedFactoryStats != null) {
            builder.startObject("system_generated_factories");
            systemGeneratedFactoryStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalRequestStats.writeTo(out);
        totalResponseStats.writeTo(out);
        out.writeVInt(perPipelineStats.size());
        for (PerPipelineStats pipelineStat : perPipelineStats) {
            pipelineStat.writeTo(out);
            PipelineDetailStats pipelineDetailStats = perPipelineProcessorStats.get(pipelineStat.pipelineId);
            pipelineDetailStats.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            systemGeneratedFactoryStats.writeTo(out);
            systemGeneratedProcessorStats.writeTo(out);
        }
    }

    static class Builder {
        private OperationStats totalRequestStats;
        private OperationStats totalResponseStats;
        private final List<PerPipelineStats> perPipelineStats = new ArrayList<>();
        private final Map<String, List<ProcessorStats>> requestProcessorStatsPerPipeline = new HashMap<>();
        private final Map<String, List<ProcessorStats>> responseProcessorStatsPerPipeline = new HashMap<>();
        private FactoryDetailStats systemGeneratedFactoryStats = new FactoryDetailStats(emptyList(), emptyList());
        private PipelineDetailStats systemGeneratedProcessorStats = new PipelineDetailStats(emptyList(), emptyList());

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

        Builder withSystemGeneratedProcessorMetrics(@NonNull final SystemGeneratedProcessorMetrics systemGeneratedProcessorMetrics) {
            systemGeneratedFactoryStats = systemGeneratedProcessorMetrics.getFactoryStats();
            systemGeneratedProcessorStats = systemGeneratedProcessorMetrics.getProcessorStats();
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
                unmodifiableMap(pipelineDetailStatsMap),
                systemGeneratedFactoryStats,
                systemGeneratedProcessorStats
            );
        }
    }

    static class PerPipelineStats implements Writeable {
        private final String pipelineId;
        private final OperationStats requestStats;
        private final OperationStats responseStats;

        public PerPipelineStats(String pipelineId, OperationStats requestStats, OperationStats responseStats) {
            this.pipelineId = pipelineId;
            this.requestStats = requestStats;
            this.responseStats = responseStats;
        }

        public PerPipelineStats(StreamInput in) throws IOException {
            pipelineId = in.readString();
            requestStats = new OperationStats(in);
            responseStats = new OperationStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pipelineId);
            requestStats.writeTo(out);
            responseStats.writeTo(out);
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

    static class PipelineDetailStats implements ToXContentFragment, Writeable {
        private final List<ProcessorStats> requestProcessorStats;
        private final List<ProcessorStats> responseProcessorStats;

        public PipelineDetailStats(List<ProcessorStats> requestProcessorStats, List<ProcessorStats> responseProcessorStats) {
            this.requestProcessorStats = requestProcessorStats;
            this.responseProcessorStats = responseProcessorStats;
        }

        public PipelineDetailStats(StreamInput in) throws IOException {
            int numRequestProcessors = in.readVInt();
            requestProcessorStats = new ArrayList<>(numRequestProcessors);
            for (int j = 0; j < numRequestProcessors; j++) {
                requestProcessorStats.add(new ProcessorStats(in));
            }
            int numResponseProcessors = in.readVInt();
            responseProcessorStats = new ArrayList<>(numResponseProcessors);
            for (int j = 0; j < numResponseProcessors; j++) {
                responseProcessorStats.add(new ProcessorStats(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(requestProcessorStats.size());
            for (ProcessorStats processorStats : requestProcessorStats) {
                processorStats.writeTo(out);
            }
            out.writeVInt(responseProcessorStats.size());
            for (ProcessorStats processorStats : responseProcessorStats) {
                processorStats.writeTo(out);
            }
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            writeProcessorStatsArray(builder, params, "request_processors", requestProcessorStats);
            writeProcessorStatsArray(builder, params, "response_processors", responseProcessorStats);
            return builder;
        }

        private void writeProcessorStatsArray(XContentBuilder builder, Params params, String fieldName, List<ProcessorStats> stats)
            throws IOException {
            builder.startArray(fieldName);
            for (ProcessorStats processorStats : stats) {
                builder.startObject();
                processorStats.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    static class ProcessorStats implements ToXContentFragment, Writeable {
        private final String processorName; // type:tag
        private final String processorType;
        private final OperationStats stats;

        public ProcessorStats(String processorName, String processorType, OperationStats stats) {
            this.processorName = processorName;
            this.processorType = processorType;
            this.stats = stats;
        }

        public ProcessorStats(StreamInput in) throws IOException {
            processorName = in.readString();
            processorType = in.readString();
            stats = new OperationStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(processorName);
            out.writeString(processorType);
            stats.writeTo(out);
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

    static class FactoryStats implements ToXContentFragment, Writeable {
        private final String factoryType;
        private final OperationStats evaluationStats;
        private final OperationStats generationStats;

        FactoryStats(String factoryType, OperationStats evaluationStats, OperationStats generationStats) {
            this.factoryType = factoryType;
            this.evaluationStats = evaluationStats;
            this.generationStats = generationStats;
        }

        FactoryStats(StreamInput in) throws IOException {
            factoryType = in.readString();
            evaluationStats = new OperationStats(in);
            generationStats = new OperationStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(factoryType);
            evaluationStats.writeTo(out);
            generationStats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(factoryType);
            builder.field("type", factoryType);
            builder.startObject("evaluation_stats");
            evaluationStats.toXContent(builder, params);
            builder.endObject();
            builder.startObject("generation_stats");
            generationStats.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FactoryStats that = (FactoryStats) o;
            return factoryType.equals(that.factoryType)
                && evaluationStats.equals(that.evaluationStats)
                && generationStats.equals(that.generationStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(factoryType, evaluationStats, generationStats);
        }
    }

    static class FactoryDetailStats implements ToXContentFragment, Writeable {
        private final List<FactoryStats> requestProcessorFactoryStats;
        private final List<FactoryStats> responseProcessorFactoryStats;

        FactoryDetailStats(List<FactoryStats> requestProcessorFactoryStats, List<FactoryStats> responseProcessorFactoryStats) {
            this.requestProcessorFactoryStats = requestProcessorFactoryStats;
            this.responseProcessorFactoryStats = responseProcessorFactoryStats;
        }

        FactoryDetailStats(StreamInput in) throws IOException {
            int numRequestProcessorFactories = in.readVInt();
            requestProcessorFactoryStats = new ArrayList<>(numRequestProcessorFactories);
            for (int j = 0; j < numRequestProcessorFactories; j++) {
                requestProcessorFactoryStats.add(new FactoryStats(in));
            }
            int numResponseProcessorFactories = in.readVInt();
            responseProcessorFactoryStats = new ArrayList<>(numResponseProcessorFactories);
            for (int j = 0; j < numResponseProcessorFactories; j++) {
                responseProcessorFactoryStats.add(new FactoryStats(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(requestProcessorFactoryStats.size());
            for (FactoryStats factoryStats : requestProcessorFactoryStats) {
                factoryStats.writeTo(out);
            }
            out.writeVInt(responseProcessorFactoryStats.size());
            for (FactoryStats factoryStats : responseProcessorFactoryStats) {
                factoryStats.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            writeFactoryStatsArray(builder, params, "request_processor_factories", requestProcessorFactoryStats);
            writeFactoryStatsArray(builder, params, "response_processor_factories", responseProcessorFactoryStats);
            return builder;
        }

        private void writeFactoryStatsArray(XContentBuilder builder, Params params, String fieldName, List<FactoryStats> stats)
            throws IOException {
            builder.startArray(fieldName);
            for (FactoryStats factoryStats : stats) {
                builder.startObject();
                factoryStats.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FactoryDetailStats that = (FactoryDetailStats) o;
            return requestProcessorFactoryStats.equals(that.requestProcessorFactoryStats)
                && responseProcessorFactoryStats.equals(that.responseProcessorFactoryStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestProcessorFactoryStats, responseProcessorFactoryStats);
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
            && perPipelineProcessorStats.equals(stats.perPipelineProcessorStats)
            && systemGeneratedFactoryStats.equals(stats.systemGeneratedFactoryStats)
            && systemGeneratedProcessorStats.equals(stats.systemGeneratedProcessorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalRequestStats,
            totalResponseStats,
            perPipelineStats,
            perPipelineProcessorStats,
            systemGeneratedFactoryStats,
            systemGeneratedProcessorStats
        );
    }
}
