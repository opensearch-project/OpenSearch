/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.monitor.process;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Holds stats for the process
*
* @opensearch.internal
*/
public class ProtobufProcessStats implements ProtobufWriteable, ToXContentFragment {

    private final long timestamp;
    private final long openFileDescriptors;
    private final long maxFileDescriptors;
    private final Cpu cpu;
    private final Mem mem;

    public ProtobufProcessStats(long timestamp, long openFileDescriptors, long maxFileDescriptors, Cpu cpu, Mem mem) {
        this.timestamp = timestamp;
        this.openFileDescriptors = openFileDescriptors;
        this.maxFileDescriptors = maxFileDescriptors;
        this.cpu = cpu;
        this.mem = mem;
    }

    public ProtobufProcessStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        timestamp = in.readInt64();
        openFileDescriptors = in.readInt64();
        maxFileDescriptors = in.readInt64();
        cpu = protobufStreamInput.readOptionalWriteable(Cpu::new);
        mem = protobufStreamInput.readOptionalWriteable(Mem::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt64NoTag(timestamp);
        out.writeInt64NoTag(openFileDescriptors);
        out.writeInt64NoTag(maxFileDescriptors);
        protobufStreamOutput.writeOptionalWriteable(cpu);
        protobufStreamOutput.writeOptionalWriteable(mem);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getOpenFileDescriptors() {
        return openFileDescriptors;
    }

    public long getMaxFileDescriptors() {
        return maxFileDescriptors;
    }

    public Cpu getCpu() {
        return cpu;
    }

    public Mem getMem() {
        return mem;
    }

    static final class Fields {
        static final String PROCESS = "process";
        static final String TIMESTAMP = "timestamp";
        static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
        static final String MAX_FILE_DESCRIPTORS = "max_file_descriptors";

        static final String CPU = "cpu";
        static final String PERCENT = "percent";
        static final String TOTAL = "total";
        static final String TOTAL_IN_MILLIS = "total_in_millis";

        static final String MEM = "mem";
        static final String TOTAL_VIRTUAL = "total_virtual";
        static final String TOTAL_VIRTUAL_IN_BYTES = "total_virtual_in_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PROCESS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.OPEN_FILE_DESCRIPTORS, openFileDescriptors);
        builder.field(Fields.MAX_FILE_DESCRIPTORS, maxFileDescriptors);
        if (cpu != null) {
            builder.startObject(Fields.CPU);
            builder.field(Fields.PERCENT, cpu.percent);
            builder.humanReadableField(Fields.TOTAL_IN_MILLIS, Fields.TOTAL, new TimeValue(cpu.total));
            builder.endObject();
        }
        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.humanReadableField(Fields.TOTAL_VIRTUAL_IN_BYTES, Fields.TOTAL_VIRTUAL, new ByteSizeValue(mem.totalVirtual));
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Process memory information.
    *
    * @opensearch.internal
    */
    public static class Mem implements ProtobufWriteable {

        private final long totalVirtual;

        public Mem(long totalVirtual) {
            this.totalVirtual = totalVirtual;
        }

        public Mem(CodedInputStream in) throws IOException {
            totalVirtual = in.readInt64();
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeInt64NoTag(totalVirtual);
        }

        public ByteSizeValue getTotalVirtual() {
            return new ByteSizeValue(totalVirtual);
        }
    }

    /**
     * Process CPU information.
    *
    * @opensearch.internal
    */
    public static class Cpu implements ProtobufWriteable {

        private final short percent;
        private final long total;

        public Cpu(short percent, long total) {
            this.percent = percent;
            this.total = total;
        }

        public Cpu(CodedInputStream in) throws IOException {
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
            percent = protobufStreamInput.readShort();
            total = in.readInt64();
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeShort(percent);
            out.writeInt64NoTag(total);
        }

        /**
         * Get the Process cpu usage.
        * <p>
        * Supported Platforms: All.
        */
        public short getPercent() {
            return percent;
        }

        /**
         * Get the Process cpu time (sum of User and Sys).
        * <p>
        * Supported Platforms: All.
        */
        public TimeValue getTotal() {
            return new TimeValue(total);
        }
    }
}
