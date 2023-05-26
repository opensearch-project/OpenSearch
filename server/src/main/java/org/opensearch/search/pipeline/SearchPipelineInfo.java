/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ReportingService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Information about a search pipelines event
 *
 * @opensearch.internal
 */
public class SearchPipelineInfo implements ReportingService.Info {

    private final Map<String, Set<ProcessorInfo>> processors = new TreeMap<>();

    public SearchPipelineInfo(Map<String, List<ProcessorInfo>> processors) {
        for (Map.Entry<String, List<ProcessorInfo>> processorsEntry : processors.entrySet()) {
            // we use a treeset here to have a test-able / predictable order
            this.processors.put(processorsEntry.getKey(), new TreeSet<>(processorsEntry.getValue()));
        }
    }

    /**
     * Read from a stream.
     */
    public SearchPipelineInfo(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_2_8_0)) {
            // Prior to version 2.8, we had a flat list of processors. For best compatibility, assume they're valid
            // request and response processor, since we couldn't tell the difference back then.
            final int size = in.readVInt();
            Set<ProcessorInfo> processorInfos = new TreeSet<>();
            for (int i = 0; i < size; i++) {
                processorInfos.add(new ProcessorInfo(in));
            }
            processors.put(Pipeline.REQUEST_PROCESSORS_KEY, processorInfos);
            processors.put(Pipeline.RESPONSE_PROCESSORS_KEY, processorInfos);
        } else {
            final int numTypes = in.readVInt();
            for (int i = 0; i < numTypes; i++) {
                String type = in.readString();
                int numProcessors = in.readVInt();
                Set<ProcessorInfo> processorInfos = new TreeSet<>();
                for (int j = 0; j < numProcessors; j++) {
                    processorInfos.add(new ProcessorInfo(in));
                }
                processors.put(type, processorInfos);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_pipelines");
        for (Map.Entry<String, Set<ProcessorInfo>> processorEntry : processors.entrySet()) {
            builder.startArray(processorEntry.getKey());
            for (ProcessorInfo info : processorEntry.getValue()) {
                info.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_2_8_0)) {
            // Prior to version 2.8, we grouped all processors into a single list.
            Set<ProcessorInfo> processorInfos = new TreeSet<>();
            processorInfos.addAll(processors.getOrDefault(Pipeline.REQUEST_PROCESSORS_KEY, Collections.emptySet()));
            processorInfos.addAll(processors.getOrDefault(Pipeline.RESPONSE_PROCESSORS_KEY, Collections.emptySet()));
            out.writeVInt(processorInfos.size());
            for (ProcessorInfo processorInfo : processorInfos) {
                processorInfo.writeTo(out);
            }
        } else {
            out.write(processors.size());
            for (Map.Entry<String, Set<ProcessorInfo>> processorsEntry : processors.entrySet()) {
                out.writeString(processorsEntry.getKey());
                out.writeVInt(processorsEntry.getValue().size());
                for (ProcessorInfo processorInfo : processorsEntry.getValue()) {
                    processorInfo.writeTo(out);
                }
            }
        }
    }

    public boolean containsProcessor(String processorType, String type) {
        return processors.containsKey(processorType) && processors.get(processorType).contains(new ProcessorInfo(type));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchPipelineInfo that = (SearchPipelineInfo) o;
        return Objects.equals(processors, that.processors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors);
    }
}
