/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Represents the Lucene Index (set of files on a single shard) involved
 * in the replication process.
 *
 * @opensearch.internal
 */
public final class ReplicationLuceneIndex extends ReplicationTimer implements ToXContentFragment, Writeable {
    private final FilesDetails filesDetails;

    public static final long UNKNOWN = -1L;

    private long sourceThrottlingInNanos = UNKNOWN;
    private long targetThrottleTimeInNanos = UNKNOWN;

    public ReplicationLuceneIndex() {
        this(new FilesDetails());
    }

    public ReplicationLuceneIndex(FilesDetails filesDetails) {
        this.filesDetails = filesDetails;
    }

    public ReplicationLuceneIndex(StreamInput in) throws IOException {
        super(in);
        filesDetails = new FilesDetails(in);
        sourceThrottlingInNanos = in.readLong();
        targetThrottleTimeInNanos = in.readLong();
    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        filesDetails.writeTo(out);
        out.writeLong(sourceThrottlingInNanos);
        out.writeLong(targetThrottleTimeInNanos);
    }

    public synchronized List<FileMetadata> fileDetails() {
        return Collections.unmodifiableList(new ArrayList<>(filesDetails.values()));
    }

    public synchronized void reset() {
        super.reset();
        filesDetails.clear();
        sourceThrottlingInNanos = UNKNOWN;
        targetThrottleTimeInNanos = UNKNOWN;
    }

    public synchronized void addFileDetail(String name, long length, boolean reused) {
        filesDetails.addFileDetails(name, length, reused);
    }

    public synchronized void setFileDetailsComplete() {
        filesDetails.setComplete();
    }

    public synchronized void addRecoveredBytesToFile(String name, long bytes) {
        filesDetails.addRecoveredBytesToFile(name, bytes);
    }

    public synchronized void addSourceThrottling(long timeInNanos) {
        if (sourceThrottlingInNanos == UNKNOWN) {
            sourceThrottlingInNanos = timeInNanos;
        } else {
            sourceThrottlingInNanos += timeInNanos;
        }
    }

    public synchronized void addTargetThrottling(long timeInNanos) {
        if (targetThrottleTimeInNanos == UNKNOWN) {
            targetThrottleTimeInNanos = timeInNanos;
        } else {
            targetThrottleTimeInNanos += timeInNanos;
        }
    }

    public synchronized TimeValue sourceThrottling() {
        return TimeValue.timeValueNanos(sourceThrottlingInNanos);
    }

    public synchronized TimeValue targetThrottling() {
        return TimeValue.timeValueNanos(targetThrottleTimeInNanos);
    }

    /**
     * total number of files that are part of this recovery, both re-used and recovered
     */
    public synchronized int totalFileCount() {
        return filesDetails.size();
    }

    /**
     * total number of files to be recovered (potentially not yet done)
     */
    public synchronized int totalRecoverFiles() {
        int total = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused() == false) {
                total++;
            }
        }
        return total;
    }

    /**
     * number of file that were recovered (excluding on ongoing files)
     */
    public synchronized int recoveredFileCount() {
        int count = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.fullyRecovered()) {
                count++;
            }
        }
        return count;
    }

    /**
     * percent of recovered (i.e., not reused) files out of the total files to be recovered
     */
    public synchronized float recoveredFilesPercent() {
        int total = 0;
        int recovered = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused() == false) {
                total++;
                if (file.fullyRecovered()) {
                    recovered++;
                }
            }
        }
        if (total == 0 && filesDetails.size() == 0) {      // indicates we are still in init phase
            return 0.0f;
        }
        if (total == recovered) {
            return 100.0f;
        } else {
            float result = 100.0f * (recovered / (float) total);
            return result;
        }
    }

    /**
     * total number of bytes in th shard
     */
    public synchronized long totalBytes() {
        long total = 0;
        for (FileMetadata file : filesDetails.values()) {
            total += file.length();
        }
        return total;
    }

    /**
     * total number of bytes recovered so far, including both existing and reused
     */
    public synchronized long recoveredBytes() {
        long recovered = 0;
        for (FileMetadata file : filesDetails.values()) {
            recovered += file.recovered();
        }
        return recovered;
    }

    /**
     * total bytes of files to be recovered (potentially not yet done)
     */
    public synchronized long totalRecoverBytes() {
        long total = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused() == false) {
                total += file.length();
            }
        }
        return total;
    }

    /**
     * @return number of bytes still to recover, i.e. {@link ReplicationLuceneIndex#totalRecoverBytes()} minus {@link ReplicationLuceneIndex#recoveredBytes()}, or
     * {@code -1} if the full set of files to recover is not yet known
     */
    public synchronized long bytesStillToRecover() {
        if (filesDetails.isComplete() == false) {
            return -1L;
        }
        long total = 0L;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused() == false) {
                total += file.length() - file.recovered();
            }
        }
        return total;
    }

    /**
     * percent of bytes recovered out of total files bytes *to be* recovered
     */
    public synchronized float recoveredBytesPercent() {
        long total = 0;
        long recovered = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused() == false) {
                total += file.length();
                recovered += file.recovered();
            }
        }
        if (total == 0 && filesDetails.size() == 0) {
            // indicates we are still in init phase
            return 0.0f;
        }
        if (total == recovered) {
            return 100.0f;
        } else {
            return 100.0f * recovered / total;
        }
    }

    public synchronized int reusedFileCount() {
        int reused = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused()) {
                reused++;
            }
        }
        return reused;
    }

    public synchronized long reusedBytes() {
        long reused = 0;
        for (FileMetadata file : filesDetails.values()) {
            if (file.reused()) {
                reused += file.length();
            }
        }
        return reused;
    }

    @Override
    public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // stream size first, as it matters more and the files section can be long
        builder.startObject(Fields.SIZE);
        builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, new ByteSizeValue(totalBytes()));
        builder.humanReadableField(Fields.REUSED_IN_BYTES, Fields.REUSED, new ByteSizeValue(reusedBytes()));
        builder.humanReadableField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, new ByteSizeValue(recoveredBytes()));
        builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredBytesPercent()));
        builder.endObject();

        builder.startObject(Fields.FILES);
        builder.field(Fields.TOTAL, totalFileCount());
        builder.field(Fields.REUSED, reusedFileCount());
        builder.field(Fields.RECOVERED, recoveredFileCount());
        builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredFilesPercent()));
        filesDetails.toXContent(builder, params);
        builder.endObject();
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
        builder.humanReadableField(Fields.SOURCE_THROTTLE_TIME_IN_MILLIS, Fields.SOURCE_THROTTLE_TIME, sourceThrottling());
        builder.humanReadableField(Fields.TARGET_THROTTLE_TIME_IN_MILLIS, Fields.TARGET_THROTTLE_TIME, targetThrottling());
        return builder;
    }

    @Override
    public synchronized String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public synchronized FileMetadata getFileDetails(String dest) {
        return filesDetails.get(dest);
    }

    /**
     * Details about the files
     *
     * @opensearch.internal
     */
    private static final class FilesDetails implements ToXContentFragment, Writeable {
        protected final Map<String, FileMetadata> fileMetadataMap = new HashMap<>();
        protected boolean complete;

        public FilesDetails() {}

        FilesDetails(StreamInput in) throws IOException {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                FileMetadata file = new FileMetadata(in);
                fileMetadataMap.put(file.name, file);
            }
            complete = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final FileMetadata[] files = values().toArray(new FileMetadata[0]);
            out.writeVInt(files.length);
            for (FileMetadata file : files) {
                file.writeTo(out);
            }
            out.writeBoolean(complete);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (params.paramAsBoolean("detailed", false)) {
                builder.startArray(Fields.DETAILS);
                for (FileMetadata file : values()) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }

            return builder;
        }

        public void addFileDetails(String name, long length, boolean reused) {
            assert complete == false : "addFileDetail for [" + name + "] when file details are already complete";
            FileMetadata existing = fileMetadataMap.put(name, new FileMetadata(name, length, reused));
            assert existing == null : "file [" + name + "] is already reported";
        }

        public void addRecoveredBytesToFile(String name, long bytes) {
            FileMetadata file = fileMetadataMap.get(name);
            assert file != null : "file [" + name + "] hasn't been reported";
            file.addRecoveredBytes(bytes);
        }

        public FileMetadata get(String name) {
            return fileMetadataMap.get(name);
        }

        public void setComplete() {
            complete = true;
        }

        public int size() {
            return fileMetadataMap.size();
        }

        public boolean isEmpty() {
            return fileMetadataMap.isEmpty();
        }

        public void clear() {
            fileMetadataMap.clear();
            complete = false;
        }

        public Collection<FileMetadata> values() {
            return fileMetadataMap.values();
        }

        public boolean isComplete() {
            return complete;
        }
    }

    /**
     * Metadata about a file
     *
     * @opensearch.internal
     */
    public static final class FileMetadata implements ToXContentObject, Writeable {
        private String name;
        private long length;
        private long recovered;
        private boolean reused;

        public FileMetadata(String name, long length, boolean reused) {
            assert name != null;
            this.name = name;
            this.length = length;
            this.reused = reused;
        }

        public FileMetadata(StreamInput in) throws IOException {
            name = in.readString();
            length = in.readVLong();
            recovered = in.readVLong();
            reused = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered);
            out.writeBoolean(reused);
        }

        public void addRecoveredBytes(long bytes) {
            assert reused == false : "file is marked as reused, can't update recovered bytes";
            assert bytes >= 0 : "can't recovered negative bytes. got [" + bytes + "]";
            recovered += bytes;
        }

        /**
         * file name
         */
        public String name() {
            return name;
        }

        /**
         * file length
         */
        public long length() {
            return length;
        }

        /**
         * number of bytes recovered for this file (so far). 0 if the file is reused
         */
        public long recovered() {
            return recovered;
        }

        /**
         * returns true if the file is reused from a local copy
         */
        public boolean reused() {
            return reused;
        }

        public boolean fullyRecovered() {
            return reused == false && length == recovered;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.humanReadableField(Fields.LENGTH_IN_BYTES, Fields.LENGTH, new ByteSizeValue(length));
            builder.field(Fields.REUSED, reused);
            builder.humanReadableField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, new ByteSizeValue(recovered));
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FileMetadata) {
                FileMetadata other = (FileMetadata) obj;
                return name.equals(other.name) && length == other.length() && reused == other.reused() && recovered == other.recovered();
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Long.hashCode(length);
            result = 31 * result + Long.hashCode(recovered);
            result = 31 * result + (reused ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "file (name [" + name + "], reused [" + reused + "], length [" + length + "], recovered [" + recovered + "])";
        }
    }

    /**
     * Duplicates many of Field names in {@link RecoveryState}
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String NAME = "name";
        static final String RECOVERED = "recovered";
        static final String RECOVERED_IN_BYTES = "recovered_in_bytes";
        static final String LENGTH = "length";
        static final String LENGTH_IN_BYTES = "length_in_bytes";
        static final String FILES = "files";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";
        static final String REUSED = "reused";
        static final String REUSED_IN_BYTES = "reused_in_bytes";
        static final String PERCENT = "percent";
        static final String DETAILS = "details";
        static final String SIZE = "size";
        static final String SOURCE_THROTTLE_TIME = "source_throttle_time";
        static final String SOURCE_THROTTLE_TIME_IN_MILLIS = "source_throttle_time_in_millis";
        static final String TARGET_THROTTLE_TIME = "target_throttle_time";
        static final String TARGET_THROTTLE_TIME_IN_MILLIS = "target_throttle_time_in_millis";
    }
}
