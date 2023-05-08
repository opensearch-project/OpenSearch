/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.monitor.os;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;

/**
 * Holds Operating System Information
*
* @opensearch.internal
*/
public class ProtobufOsInfo implements ProtobufReportingService.ProtobufInfo {

    private final long refreshInterval;
    private final int availableProcessors;
    private final int allocatedProcessors;
    private final String name;
    private final String prettyName;
    private final String arch;
    private final String version;

    public ProtobufOsInfo(
        final long refreshInterval,
        final int availableProcessors,
        final int allocatedProcessors,
        final String name,
        final String prettyName,
        final String arch,
        final String version
    ) {
        this.refreshInterval = refreshInterval;
        this.availableProcessors = availableProcessors;
        this.allocatedProcessors = allocatedProcessors;
        this.name = name;
        this.prettyName = prettyName;
        this.arch = arch;
        this.version = version;
    }

    public ProtobufOsInfo(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput();
        this.refreshInterval = in.readInt64();
        this.availableProcessors = in.readInt32();
        this.allocatedProcessors = in.readInt32();
        this.name = protobufStreamInput.readOptionalString(in);
        this.prettyName = protobufStreamInput.readOptionalString(in);
        this.arch = protobufStreamInput.readOptionalString(in);
        this.version = protobufStreamInput.readOptionalString(in);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput();
        out.writeInt64NoTag(refreshInterval);
        out.writeInt32NoTag(availableProcessors);
        out.writeInt32NoTag(allocatedProcessors);
        protobufStreamOutput.writeOptionalString(name, out);
        protobufStreamOutput.writeOptionalString(prettyName, out);
        protobufStreamOutput.writeOptionalString(arch, out);
        protobufStreamOutput.writeOptionalString(version, out);
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    public int getAvailableProcessors() {
        return this.availableProcessors;
    }

    public int getAllocatedProcessors() {
        return this.allocatedProcessors;
    }

    public String getName() {
        return name;
    }

    public String getPrettyName() {
        return prettyName;
    }

    public String getArch() {
        return arch;
    }

    public String getVersion() {
        return version;
    }
}
