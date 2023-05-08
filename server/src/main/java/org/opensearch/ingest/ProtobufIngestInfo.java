/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.ingest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Information about an ingest event
*
* @opensearch.internal
*/
public class ProtobufIngestInfo implements ProtobufReportingService.ProtobufInfo {

    private final Set<ProtobufProcessorInfo> processors;

    public ProtobufIngestInfo(List<ProtobufProcessorInfo> processors) {
        this.processors = new TreeSet<>(processors);  // we use a treeset here to have a test-able / predictable order
    }

    /**
     * Read from a stream.
    */
    public ProtobufIngestInfo(CodedInputStream in) throws IOException {
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

    public Iterable<ProtobufProcessorInfo> getProcessors() {
        return processors;
    }

    public boolean containsProcessor(String type) {
        return processors.contains(new ProtobufProcessorInfo(type));
    }
}
