/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.search.pipeline;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.ProtobufProcessorInfo;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Information about a search pipelines event
*
* @opensearch.internal
*/
public class ProtobufSearchPipelineInfo implements ProtobufReportingService.ProtobufInfo {

    private final Set<ProtobufProcessorInfo> processors;

    public ProtobufSearchPipelineInfo(List<ProtobufProcessorInfo> processors) {
        this.processors = new TreeSet<>(processors);  // we use a treeset here to have a test-able / predictable order
    }

    /**
     * Read from a stream.
    */
    public ProtobufSearchPipelineInfo(CodedInputStream in) throws IOException {
        processors = new TreeSet<>();
        final int size = in.readInt32();
        for (int i = 0; i < size; i++) {
            processors.add(new ProtobufProcessorInfo(in));
        }
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(processors.size());
        for (ProtobufProcessorInfo info : processors) {
            info.writeTo(out);
        }
    }

    public boolean containsProcessor(String type) {
        return processors.contains(new ProtobufProcessorInfo(type));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_pipelines");
        builder.startArray("processors");
        for (ProtobufProcessorInfo info : processors) {
            info.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
