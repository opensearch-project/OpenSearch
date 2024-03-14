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

package org.opensearch.ingest;

import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * OperationStats for an ingest processor pipeline
 *
 * @opensearch.internal
 */
public class IngestStats implements Writeable, ToXContentFragment {
    private final OperationStats totalStats;
    private final List<PipelineStat> pipelineStats;
    private final Map<String, List<ProcessorStat>> processorStats;

    /**
     * @param totalStats - The total stats for Ingest. This is the logically the sum of all pipeline stats,
     *                   and pipeline stats are logically the sum of the processor stats.
     * @param pipelineStats - The stats for a given ingest pipeline.
     * @param processorStats - The per-processor stats for a given pipeline. A map keyed by the pipeline identifier.
     */
    public IngestStats(OperationStats totalStats, List<PipelineStat> pipelineStats, Map<String, List<ProcessorStat>> processorStats) {
        this.totalStats = totalStats;
        this.pipelineStats = pipelineStats;
        this.processorStats = processorStats;
    }

    /**
     * Read from a stream.
     */
    public IngestStats(StreamInput in) throws IOException {
        this.totalStats = new OperationStats(in);
        int size = in.readVInt();
        this.pipelineStats = new ArrayList<>(size);
        this.processorStats = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String pipelineId = in.readString();
            OperationStats pipelineStat = new OperationStats(in);
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineStat));
            int processorsSize = in.readVInt();
            List<ProcessorStat> processorStatsPerPipeline = new ArrayList<>(processorsSize);
            for (int j = 0; j < processorsSize; j++) {
                String processorName = in.readString();
                String processorType = "_NOT_AVAILABLE";
                processorType = in.readString();
                OperationStats processorStat = new OperationStats(in);
                processorStatsPerPipeline.add(new ProcessorStat(processorName, processorType, processorStat));
            }
            this.processorStats.put(pipelineId, processorStatsPerPipeline);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVInt(pipelineStats.size());
        for (PipelineStat pipelineStat : pipelineStats) {
            out.writeString(pipelineStat.getPipelineId());
            pipelineStat.getStats().writeTo(out);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.getPipelineId());
            if (processorStatsForPipeline == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(processorStatsForPipeline.size());
                for (ProcessorStat processorStat : processorStatsForPipeline) {
                    out.writeString(processorStat.getName());
                    out.writeString(processorStat.getType());
                    processorStat.getStats().writeTo(out);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ingest");
        builder.startObject("total");
        totalStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("pipelines");
        for (PipelineStat pipelineStat : pipelineStats) {
            builder.startObject(pipelineStat.getPipelineId());
            pipelineStat.getStats().toXContent(builder, params);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.getPipelineId());
            builder.startArray("processors");
            if (processorStatsForPipeline != null) {
                for (ProcessorStat processorStat : processorStatsForPipeline) {
                    builder.startObject();
                    builder.startObject(processorStat.getName());
                    builder.field("type", processorStat.getType());
                    builder.startObject("stats");
                    processorStat.getStats().toXContent(builder, params);
                    builder.endObject();
                    builder.endObject();
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public OperationStats getTotalStats() {
        return totalStats;
    }

    public List<PipelineStat> getPipelineStats() {
        return pipelineStats;
    }

    public Map<String, List<ProcessorStat>> getProcessorStats() {
        return processorStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestStats that = (IngestStats) o;
        return Objects.equals(totalStats, that.totalStats)
            && Objects.equals(pipelineStats, that.pipelineStats)
            && Objects.equals(processorStats, that.processorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalStats, pipelineStats, processorStats);
    }

    /**
     * Easy conversion from scoped {@link OperationMetrics} objects to a serializable OperationStats objects
     */
    static class Builder {
        private OperationStats totalStats;
        private List<PipelineStat> pipelineStats = new ArrayList<>();
        private Map<String, List<ProcessorStat>> processorStats = new HashMap<>();

        Builder addTotalMetrics(OperationMetrics totalMetric) {
            this.totalStats = totalMetric.createStats();
            return this;
        }

        Builder addPipelineMetrics(String pipelineId, OperationMetrics pipelineMetric) {
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineMetric.createStats()));
            return this;
        }

        Builder addProcessorMetrics(String pipelineId, String processorName, String processorType, OperationMetrics metric) {
            this.processorStats.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStat(processorName, processorType, metric.createStats()));
            return this;
        }

        IngestStats build() {
            return new IngestStats(totalStats, Collections.unmodifiableList(pipelineStats), Collections.unmodifiableMap(processorStats));
        }
    }

    /**
     * Container for pipeline stats.
     */
    public static class PipelineStat {
        private final String pipelineId;
        private final OperationStats stats;

        public PipelineStat(String pipelineId, OperationStats stats) {
            this.pipelineId = pipelineId;
            this.stats = stats;
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public OperationStats getStats() {
            return stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IngestStats.PipelineStat that = (IngestStats.PipelineStat) o;
            return Objects.equals(pipelineId, that.pipelineId) && Objects.equals(stats, that.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineId, stats);
        }
    }

    /**
     * Container for processor stats.
     */
    public static class ProcessorStat {
        private final String name;
        private final String type;
        private final OperationStats stats;

        public ProcessorStat(String name, String type, OperationStats stats) {
            this.name = name;
            this.type = type;
            this.stats = stats;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public OperationStats getStats() {
            return stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IngestStats.ProcessorStat that = (IngestStats.ProcessorStat) o;
            return Objects.equals(name, that.name) && Objects.equals(type, that.type) && Objects.equals(stats, that.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, stats);
        }
    }
}
