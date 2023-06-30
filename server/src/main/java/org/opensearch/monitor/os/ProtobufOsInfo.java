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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.XContentBuilder;
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
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        this.refreshInterval = in.readInt64();
        this.availableProcessors = in.readInt32();
        this.allocatedProcessors = in.readInt32();
        this.name = protobufStreamInput.readOptionalString();
        this.prettyName = protobufStreamInput.readOptionalString();
        this.arch = protobufStreamInput.readOptionalString();
        this.version = protobufStreamInput.readOptionalString();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt64NoTag(refreshInterval);
        out.writeInt32NoTag(availableProcessors);
        out.writeInt32NoTag(allocatedProcessors);
        protobufStreamOutput.writeOptionalString(name);
        protobufStreamOutput.writeOptionalString(prettyName);
        protobufStreamOutput.writeOptionalString(arch);
        protobufStreamOutput.writeOptionalString(version);
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

    static final class Fields {
        static final String OS = "os";
        static final String NAME = "name";
        static final String PRETTY_NAME = "pretty_name";
        static final String ARCH = "arch";
        static final String VERSION = "version";
        static final String REFRESH_INTERVAL = "refresh_interval";
        static final String REFRESH_INTERVAL_IN_MILLIS = "refresh_interval_in_millis";
        static final String AVAILABLE_PROCESSORS = "available_processors";
        static final String ALLOCATED_PROCESSORS = "allocated_processors";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.OS);
        builder.humanReadableField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, new TimeValue(refreshInterval));
        if (name != null) {
            builder.field(Fields.NAME, name);
        }
        if (prettyName != null) {
            builder.field(Fields.PRETTY_NAME, prettyName);
        }
        if (arch != null) {
            builder.field(Fields.ARCH, arch);
        }
        if (version != null) {
            builder.field(Fields.VERSION, version);
        }
        builder.field(Fields.AVAILABLE_PROCESSORS, availableProcessors);
        builder.field(Fields.ALLOCATED_PROCESSORS, allocatedProcessors);
        builder.endObject();
        return builder;
    }
}
