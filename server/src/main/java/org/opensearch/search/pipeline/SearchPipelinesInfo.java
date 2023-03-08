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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.ReportingService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Information about a search pipelines event
 *
 * @opensearch.internal
 */
public class SearchPipelinesInfo implements ReportingService.Info {

    private final Set<ProcessorInfo> processors;

    public SearchPipelinesInfo(List<ProcessorInfo> processors) {
        this.processors = new TreeSet<>(processors);  // we use a treeset here to have a test-able / predictable order
    }

    /**
     * Read from a stream.
     */
    public SearchPipelinesInfo(StreamInput in) throws IOException {
        processors = new TreeSet<>();
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            processors.add(new ProcessorInfo(in));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_pipelines");
        builder.startArray("processors");
        for (ProcessorInfo info : processors) {
            info.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.write(processors.size());
        for (ProcessorInfo info : processors) {
            info.writeTo(out);
        }
    }

    public boolean containsProcessor(String type) {
        return processors.contains(new ProcessorInfo(type));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchPipelinesInfo that = (SearchPipelinesInfo) o;
        return Objects.equals(processors, that.processors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors);
    }
}
