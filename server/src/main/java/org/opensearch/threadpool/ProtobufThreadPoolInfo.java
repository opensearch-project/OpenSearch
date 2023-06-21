/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.threadpool;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Information about a threadpool
*
* @opensearch.internal
*/
public class ProtobufThreadPoolInfo implements ProtobufReportingService.ProtobufInfo, Iterable<ThreadPool.Info> {

    private final List<ThreadPool.Info> infos;

    public ProtobufThreadPoolInfo(List<ThreadPool.Info> infos) {
        this.infos = Collections.unmodifiableList(infos);
    }

    public ProtobufThreadPoolInfo(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        this.infos = Collections.unmodifiableList(protobufStreamInput.readList(ThreadPool.Info::new));
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeCollection(infos, (o, v) -> v.writeTo(o));
    }

    @Override
    public Iterator<ThreadPool.Info> iterator() {
        return infos.iterator();
    }
}
