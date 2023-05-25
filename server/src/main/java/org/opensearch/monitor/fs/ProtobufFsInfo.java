/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.monitor.fs;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * FileSystem information
*
* @opensearch.internal
*/
public class ProtobufFsInfo implements Iterable<ProtobufFsInfo.Path>, ProtobufWriteable, ToXContentFragment {

    /**
     * Path for the file system
    *
    * @opensearch.internal
    */
    public static class Path implements ProtobufWriteable, ToXContentObject {

        String path;
        @Nullable
        String mount;
        /** File system type from {@code java.nio.file.FileStore type()}, if available. */
        @Nullable
        String type;
        long total = -1;
        long free = -1;
        long available = -1;
        long fileCacheReserved = -1;
        long fileCacheUtilized = 0;

        public Path() {}

        public Path(String path, @Nullable String mount, long total, long free, long available) {
            this.path = path;
            this.mount = mount;
            this.total = total;
            this.free = free;
            this.available = available;
        }

        /**
         * Read from a stream.
        */
        public Path(CodedInputStream in) throws IOException {
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
            path = protobufStreamInput.readOptionalString();
            mount = protobufStreamInput.readOptionalString();
            type = protobufStreamInput.readOptionalString();
            total = in.readInt64();
            free = in.readInt64();
            available = in.readInt64();
            if (protobufStreamInput.getVersion().onOrAfter(Version.V_2_7_0)) {
                fileCacheReserved = in.readInt64();
                fileCacheUtilized = in.readInt64();
            }
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeOptionalString(path); // total aggregates do not have a path
            protobufStreamOutput.writeOptionalString(mount);
            protobufStreamOutput.writeOptionalString(type);
            out.writeInt64NoTag(total);
            out.writeInt64NoTag(free);
            out.writeInt64NoTag(available);
            if (protobufStreamOutput.getVersion().onOrAfter(Version.V_2_7_0)) {
                out.writeInt64NoTag(fileCacheReserved);
                out.writeInt64NoTag(fileCacheUtilized);
            }
        }

        public String getPath() {
            return path;
        }

        public String getMount() {
            return mount;
        }

        public String getType() {
            return type;
        }

        public ByteSizeValue getTotal() {
            return new ByteSizeValue(total);
        }

        public ByteSizeValue getFree() {
            return new ByteSizeValue(free);
        }

        public ByteSizeValue getAvailable() {
            return new ByteSizeValue(available);
        }

        public ByteSizeValue getFileCacheReserved() {
            return new ByteSizeValue(fileCacheReserved);
        }

        public ByteSizeValue getFileCacheUtilized() {
            return new ByteSizeValue(fileCacheUtilized);
        }

        private long addLong(long current, long other) {
            if (current == -1 && other == -1) {
                return 0;
            }
            if (other == -1) {
                return current;
            }
            if (current == -1) {
                return other;
            }
            return current + other;
        }

        public void add(Path path) {
            total = FsProbe.adjustForHugeFilesystems(addLong(total, path.total));
            free = FsProbe.adjustForHugeFilesystems(addLong(free, path.free));
            fileCacheReserved = FsProbe.adjustForHugeFilesystems(addLong(fileCacheReserved, path.fileCacheReserved));
            fileCacheUtilized = FsProbe.adjustForHugeFilesystems(addLong(fileCacheUtilized, path.fileCacheUtilized));
            available = FsProbe.adjustForHugeFilesystems(addLong(available, path.available));
        }

        static final class Fields {
            static final String PATH = "path";
            static final String MOUNT = "mount";
            static final String TYPE = "type";
            static final String TOTAL = "total";
            static final String TOTAL_IN_BYTES = "total_in_bytes";
            static final String FREE = "free";
            static final String FREE_IN_BYTES = "free_in_bytes";
            static final String AVAILABLE = "available";
            static final String AVAILABLE_IN_BYTES = "available_in_bytes";
            static final String CACHE_RESERVED = "cache_reserved";
            static final String CACHE_RESERVED_IN_BYTES = "cache_reserved_in_bytes";
            static final String CACHE_UTILIZED = "cache_utilized";
            static final String CACHE_UTILIZED_IN_BYTES = "cache_utilized_in_bytes";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (path != null) {
                builder.field(Fields.PATH, path);
            }
            if (mount != null) {
                builder.field(Fields.MOUNT, mount);
            }
            if (type != null) {
                builder.field(Fields.TYPE, type);
            }

            if (total != -1) {
                builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
            }
            if (free != -1) {
                builder.humanReadableField(Fields.FREE_IN_BYTES, Fields.FREE, getFree());
            }
            if (available != -1) {
                builder.humanReadableField(Fields.AVAILABLE_IN_BYTES, Fields.AVAILABLE, getAvailable());
            }
            if (fileCacheReserved != -1) {
                builder.humanReadableField(Fields.CACHE_RESERVED_IN_BYTES, Fields.CACHE_RESERVED, getFileCacheReserved());
            }
            if (fileCacheReserved != 0) {
                builder.humanReadableField(Fields.CACHE_UTILIZED, Fields.CACHE_UTILIZED_IN_BYTES, getFileCacheUtilized());
            }

            builder.endObject();
            return builder;
        }
    }

    /**
     * The device status.
    *
    * @opensearch.internal
    */
    public static class DeviceStats implements ProtobufWriteable, ToXContentFragment {

        final int majorDeviceNumber;
        final int minorDeviceNumber;
        final String deviceName;
        final long currentReadsCompleted;
        final long previousReadsCompleted;
        final long currentSectorsRead;
        final long previousSectorsRead;
        final long currentWritesCompleted;
        final long previousWritesCompleted;
        final long currentSectorsWritten;
        final long previousSectorsWritten;

        public DeviceStats(
            final int majorDeviceNumber,
            final int minorDeviceNumber,
            final String deviceName,
            final long currentReadsCompleted,
            final long currentSectorsRead,
            final long currentWritesCompleted,
            final long currentSectorsWritten,
            final DeviceStats previousDeviceStats
        ) {
            this(
                majorDeviceNumber,
                minorDeviceNumber,
                deviceName,
                currentReadsCompleted,
                previousDeviceStats != null ? previousDeviceStats.currentReadsCompleted : -1,
                currentSectorsWritten,
                previousDeviceStats != null ? previousDeviceStats.currentSectorsWritten : -1,
                currentSectorsRead,
                previousDeviceStats != null ? previousDeviceStats.currentSectorsRead : -1,
                currentWritesCompleted,
                previousDeviceStats != null ? previousDeviceStats.currentWritesCompleted : -1
            );
        }

        private DeviceStats(
            final int majorDeviceNumber,
            final int minorDeviceNumber,
            final String deviceName,
            final long currentReadsCompleted,
            final long previousReadsCompleted,
            final long currentSectorsWritten,
            final long previousSectorsWritten,
            final long currentSectorsRead,
            final long previousSectorsRead,
            final long currentWritesCompleted,
            final long previousWritesCompleted
        ) {
            this.majorDeviceNumber = majorDeviceNumber;
            this.minorDeviceNumber = minorDeviceNumber;
            this.deviceName = deviceName;
            this.currentReadsCompleted = currentReadsCompleted;
            this.previousReadsCompleted = previousReadsCompleted;
            this.currentWritesCompleted = currentWritesCompleted;
            this.previousWritesCompleted = previousWritesCompleted;
            this.currentSectorsRead = currentSectorsRead;
            this.previousSectorsRead = previousSectorsRead;
            this.currentSectorsWritten = currentSectorsWritten;
            this.previousSectorsWritten = previousSectorsWritten;
        }

        public DeviceStats(CodedInputStream in) throws IOException {
            majorDeviceNumber = in.readInt32();
            minorDeviceNumber = in.readInt32();
            deviceName = in.readString();
            currentReadsCompleted = in.readInt64();
            previousReadsCompleted = in.readInt64();
            currentWritesCompleted = in.readInt64();
            previousWritesCompleted = in.readInt64();
            currentSectorsRead = in.readInt64();
            previousSectorsRead = in.readInt64();
            currentSectorsWritten = in.readInt64();
            previousSectorsWritten = in.readInt64();
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeInt32NoTag(majorDeviceNumber);
            out.writeInt32NoTag(minorDeviceNumber);
            out.writeStringNoTag(deviceName);
            out.writeInt64NoTag(currentReadsCompleted);
            out.writeInt64NoTag(previousReadsCompleted);
            out.writeInt64NoTag(currentWritesCompleted);
            out.writeInt64NoTag(previousWritesCompleted);
            out.writeInt64NoTag(currentSectorsRead);
            out.writeInt64NoTag(previousSectorsRead);
            out.writeInt64NoTag(currentSectorsWritten);
            out.writeInt64NoTag(previousSectorsWritten);
        }

        public long operations() {
            if (previousReadsCompleted == -1 || previousWritesCompleted == -1) return -1;

            return (currentReadsCompleted - previousReadsCompleted) + (currentWritesCompleted - previousWritesCompleted);
        }

        public long readOperations() {
            if (previousReadsCompleted == -1) return -1;

            return (currentReadsCompleted - previousReadsCompleted);
        }

        public long writeOperations() {
            if (previousWritesCompleted == -1) return -1;

            return (currentWritesCompleted - previousWritesCompleted);
        }

        public long readKilobytes() {
            if (previousSectorsRead == -1) return -1;

            return (currentSectorsRead - previousSectorsRead) / 2;
        }

        public long writeKilobytes() {
            if (previousSectorsWritten == -1) return -1;

            return (currentSectorsWritten - previousSectorsWritten) / 2;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("device_name", deviceName);
            builder.field(IoStats.OPERATIONS, operations());
            builder.field(IoStats.READ_OPERATIONS, readOperations());
            builder.field(IoStats.WRITE_OPERATIONS, writeOperations());
            builder.field(IoStats.READ_KILOBYTES, readKilobytes());
            builder.field(IoStats.WRITE_KILOBYTES, writeKilobytes());
            return builder;
        }

    }

    /**
     * The I/O statistics.
    *
    * @opensearch.internal
    */
    public static class IoStats implements ProtobufWriteable, ToXContentFragment {

        private static final String OPERATIONS = "operations";
        private static final String READ_OPERATIONS = "read_operations";
        private static final String WRITE_OPERATIONS = "write_operations";
        private static final String READ_KILOBYTES = "read_kilobytes";
        private static final String WRITE_KILOBYTES = "write_kilobytes";

        final DeviceStats[] devicesStats;
        final long totalOperations;
        final long totalReadOperations;
        final long totalWriteOperations;
        final long totalReadKilobytes;
        final long totalWriteKilobytes;

        public IoStats(final DeviceStats[] devicesStats) {
            this.devicesStats = devicesStats;
            long totalOperations = 0;
            long totalReadOperations = 0;
            long totalWriteOperations = 0;
            long totalReadKilobytes = 0;
            long totalWriteKilobytes = 0;
            for (DeviceStats deviceStats : devicesStats) {
                totalOperations += deviceStats.operations() != -1 ? deviceStats.operations() : 0;
                totalReadOperations += deviceStats.readOperations() != -1 ? deviceStats.readOperations() : 0;
                totalWriteOperations += deviceStats.writeOperations() != -1 ? deviceStats.writeOperations() : 0;
                totalReadKilobytes += deviceStats.readKilobytes() != -1 ? deviceStats.readKilobytes() : 0;
                totalWriteKilobytes += deviceStats.writeKilobytes() != -1 ? deviceStats.writeKilobytes() : 0;
            }
            this.totalOperations = totalOperations;
            this.totalReadOperations = totalReadOperations;
            this.totalWriteOperations = totalWriteOperations;
            this.totalReadKilobytes = totalReadKilobytes;
            this.totalWriteKilobytes = totalWriteKilobytes;
        }

        public IoStats(CodedInputStream in) throws IOException {
            final int length = in.readInt32();
            final DeviceStats[] devicesStats = new DeviceStats[length];
            for (int i = 0; i < length; i++) {
                devicesStats[i] = new DeviceStats(in);
            }
            this.devicesStats = devicesStats;
            this.totalOperations = in.readInt64();
            this.totalReadOperations = in.readInt64();
            this.totalWriteOperations = in.readInt64();
            this.totalReadKilobytes = in.readInt64();
            this.totalWriteKilobytes = in.readInt64();
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeInt32NoTag(devicesStats.length);
            for (int i = 0; i < devicesStats.length; i++) {
                devicesStats[i].writeTo(out);
            }
            out.writeInt64NoTag(totalOperations);
            out.writeInt64NoTag(totalReadOperations);
            out.writeInt64NoTag(totalWriteOperations);
            out.writeInt64NoTag(totalReadKilobytes);
            out.writeInt64NoTag(totalWriteKilobytes);
        }

        public DeviceStats[] getDevicesStats() {
            return devicesStats;
        }

        public long getTotalOperations() {
            return totalOperations;
        }

        public long getTotalReadOperations() {
            return totalReadOperations;
        }

        public long getTotalWriteOperations() {
            return totalWriteOperations;
        }

        public long getTotalReadKilobytes() {
            return totalReadKilobytes;
        }

        public long getTotalWriteKilobytes() {
            return totalWriteKilobytes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (devicesStats.length > 0) {
                builder.startArray("devices");
                for (DeviceStats deviceStats : devicesStats) {
                    builder.startObject();
                    deviceStats.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();

                builder.startObject("total");
                builder.field(OPERATIONS, totalOperations);
                builder.field(READ_OPERATIONS, totalReadOperations);
                builder.field(WRITE_OPERATIONS, totalWriteOperations);
                builder.field(READ_KILOBYTES, totalReadKilobytes);
                builder.field(WRITE_KILOBYTES, totalWriteKilobytes);
                builder.endObject();
            }
            return builder;
        }

    }

    private final long timestamp;
    private final Path[] paths;
    private final IoStats ioStats;
    private final Path total;

    public ProtobufFsInfo(long timestamp, IoStats ioStats, Path[] paths) {
        this.timestamp = timestamp;
        this.ioStats = ioStats;
        this.paths = paths;
        this.total = total();
    }

    /**
     * Read from a stream.
    */
    public ProtobufFsInfo(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        timestamp = in.readInt64();
        ioStats = protobufStreamInput.readOptionalWriteable(IoStats::new);
        paths = new Path[in.readInt32()];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(in);
        }
        this.total = total();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt64NoTag(timestamp);
        protobufStreamOutput.writeOptionalWriteable(ioStats);
        out.writeInt32NoTag(paths.length);
        for (Path path : paths) {
            path.writeTo(out);
        }
    }

    public Path getTotal() {
        return total;
    }

    private Path total() {
        Path res = new Path();
        Set<String> seenDevices = new HashSet<>(paths.length);
        for (Path subPath : paths) {
            if (subPath.path != null) {
                if (!seenDevices.add(subPath.path)) {
                    continue; // already added numbers for this device;
                }
            }
            res.add(subPath);
        }
        return res;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public IoStats getIoStats() {
        return ioStats;
    }

    @Override
    public Iterator<Path> iterator() {
        return Arrays.stream(paths).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FS);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.TOTAL);
        total().toXContent(builder, params);

        builder.startArray(Fields.DATA);
        for (Path path : paths) {
            path.toXContent(builder, params);
        }
        builder.endArray();
        if (ioStats != null) {
            builder.startObject(Fields.IO_STATS);
            ioStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String FS = "fs";
        static final String TIMESTAMP = "timestamp";
        static final String DATA = "data";
        static final String TOTAL = "total";
        static final String IO_STATS = "io_stats";
    }
}
