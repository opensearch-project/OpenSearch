/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.store.StoreStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RecoveryIndex extends Timer implements ToXContentFragment, Writeable {
    private final RecoveryFilesDetails fileDetails;

    public static final long UNKNOWN = -1L;

    private long sourceThrottlingInNanos = UNKNOWN;
    private long targetThrottleTimeInNanos = UNKNOWN;

    public RecoveryIndex() {
        this(new RecoveryFilesDetails());
    }

    public RecoveryIndex(RecoveryFilesDetails recoveryFilesDetails) {
        this.fileDetails = recoveryFilesDetails;
    }

    public RecoveryIndex(StreamInput in) throws IOException {
        super(in);
        fileDetails = new RecoveryFilesDetails(in);
        sourceThrottlingInNanos = in.readLong();
        targetThrottleTimeInNanos = in.readLong();
    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        fileDetails.writeTo(out);
        out.writeLong(sourceThrottlingInNanos);
        out.writeLong(targetThrottleTimeInNanos);
    }

    public synchronized List<FileDetail> fileDetails() {
        return Collections.unmodifiableList(new ArrayList<>(fileDetails.values()));
    }

    public synchronized void reset() {
        super.reset();
        fileDetails.clear();
        sourceThrottlingInNanos = UNKNOWN;
        targetThrottleTimeInNanos = UNKNOWN;
    }

    public synchronized void addFileDetail(String name, long length, boolean reused) {
        fileDetails.addFileDetails(name, length, reused);
    }

    public synchronized void setFileDetailsComplete() {
        fileDetails.setComplete();
    }

    public synchronized void addRecoveredBytesToFile(String name, long bytes) {
        fileDetails.addRecoveredBytesToFile(name, bytes);
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
        return fileDetails.size();
    }

    /**
     * total number of files to be recovered (potentially not yet done)
     */
    public synchronized int totalRecoverFiles() {
        int total = 0;
        for (FileDetail file : fileDetails.values()) {
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
        for (FileDetail file : fileDetails.values()) {
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
        for (FileDetail file : fileDetails.values()) {
            if (file.reused() == false) {
                total++;
                if (file.fullyRecovered()) {
                    recovered++;
                }
            }
        }
        if (total == 0 && fileDetails.size() == 0) {      // indicates we are still in init phase
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
        for (FileDetail file : fileDetails.values()) {
            total += file.length();
        }
        return total;
    }

    /**
     * total number of bytes recovered so far, including both existing and reused
     */
    public synchronized long recoveredBytes() {
        long recovered = 0;
        for (FileDetail file : fileDetails.values()) {
            recovered += file.recovered();
        }
        return recovered;
    }

    /**
     * total bytes of files to be recovered (potentially not yet done)
     */
    public synchronized long totalRecoverBytes() {
        long total = 0;
        for (FileDetail file : fileDetails.values()) {
            if (file.reused() == false) {
                total += file.length();
            }
        }
        return total;
    }

    /**
     * @return number of bytes still to recover, i.e. {@link RecoveryIndex#totalRecoverBytes()} minus {@link RecoveryIndex#recoveredBytes()}, or
     * {@code -1} if the full set of files to recover is not yet known
     */
    public synchronized long bytesStillToRecover() {
        if (fileDetails.isComplete() == false) {
            return -1L;
        }
        long total = 0L;
        for (FileDetail file : fileDetails.values()) {
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
        for (FileDetail file : fileDetails.values()) {
            if (file.reused() == false) {
                total += file.length();
                recovered += file.recovered();
            }
        }
        if (total == 0 && fileDetails.size() == 0) {
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
        for (FileDetail file : fileDetails.values()) {
            if (file.reused()) {
                reused++;
            }
        }
        return reused;
    }

    public synchronized long reusedBytes() {
        long reused = 0;
        for (FileDetail file : fileDetails.values()) {
            if (file.reused()) {
                reused += file.length();
            }
        }
        return reused;
    }

    @Override
    public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // stream size first, as it matters more and the files section can be long
        builder.startObject(RecoveryState.Fields.SIZE);
        builder.humanReadableField(RecoveryState.Fields.TOTAL_IN_BYTES, RecoveryState.Fields.TOTAL, new ByteSizeValue(totalBytes()));
        builder.humanReadableField(RecoveryState.Fields.REUSED_IN_BYTES, RecoveryState.Fields.REUSED, new ByteSizeValue(reusedBytes()));
        builder.humanReadableField(
            RecoveryState.Fields.RECOVERED_IN_BYTES,
            RecoveryState.Fields.RECOVERED,
            new ByteSizeValue(recoveredBytes())
        );
        builder.field(RecoveryState.Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredBytesPercent()));
        builder.endObject();

        builder.startObject(RecoveryState.Fields.FILES);
        builder.field(RecoveryState.Fields.TOTAL, totalFileCount());
        builder.field(RecoveryState.Fields.REUSED, reusedFileCount());
        builder.field(RecoveryState.Fields.RECOVERED, recoveredFileCount());
        builder.field(RecoveryState.Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredFilesPercent()));
        fileDetails.toXContent(builder, params);
        builder.endObject();
        builder.humanReadableField(RecoveryState.Fields.TOTAL_TIME_IN_MILLIS, RecoveryState.Fields.TOTAL_TIME, new TimeValue(time()));
        builder.humanReadableField(
            RecoveryState.Fields.SOURCE_THROTTLE_TIME_IN_MILLIS,
            RecoveryState.Fields.SOURCE_THROTTLE_TIME,
            sourceThrottling()
        );
        builder.humanReadableField(
            RecoveryState.Fields.TARGET_THROTTLE_TIME_IN_MILLIS,
            RecoveryState.Fields.TARGET_THROTTLE_TIME,
            targetThrottling()
        );
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

    public synchronized FileDetail getFileDetails(String dest) {
        return fileDetails.get(dest);
    }

    public static class FileDetail implements ToXContentObject, Writeable {
        private String name;
        private long length;
        private long recovered;
        private boolean reused;

        public FileDetail(String name, long length, boolean reused) {
            assert name != null;
            this.name = name;
            this.length = length;
            this.reused = reused;
        }

        public FileDetail(StreamInput in) throws IOException {
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

        void addRecoveredBytes(long bytes) {
            assert reused == false : "file is marked as reused, can't update recovered bytes";
            assert bytes >= 0 : "can't recovered negative bytes. got [" + bytes + "]";
            recovered += bytes;
        }

        /**
         * file name *
         */
        public String name() {
            return name;
        }

        /**
         * file length *
         */
        public long length() {
            return length;
        }

        /**
         * number of bytes recovered for this file (so far). 0 if the file is reused *
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

        boolean fullyRecovered() {
            return reused == false && length == recovered;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RecoveryState.Fields.NAME, name);
            builder.humanReadableField(RecoveryState.Fields.LENGTH_IN_BYTES, RecoveryState.Fields.LENGTH, new ByteSizeValue(length));
            builder.field(RecoveryState.Fields.REUSED, reused);
            builder.humanReadableField(
                RecoveryState.Fields.RECOVERED_IN_BYTES,
                RecoveryState.Fields.RECOVERED,
                new ByteSizeValue(recovered)
            );
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FileDetail) {
                FileDetail other = (FileDetail) obj;
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

    public static class RecoveryFilesDetails implements ToXContentFragment, Writeable {
        protected final Map<String, FileDetail> fileDetails = new HashMap<>();
        protected boolean complete;

        public RecoveryFilesDetails() {}

        RecoveryFilesDetails(StreamInput in) throws IOException {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                FileDetail file = new FileDetail(in);
                fileDetails.put(file.name, file);
            }
            if (in.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
                complete = in.readBoolean();
            } else {
                // This flag is used by disk-based allocation to decide whether the remaining bytes measurement is accurate or not; if not
                // then it falls back on an estimate. There's only a very short window in which the file details are present but incomplete
                // so this is a reasonable approximation, and the stats reported to the disk-based allocator don't hit this code path
                // anyway since they always use IndexShard#getRecoveryState which is never transported over the wire.
                complete = fileDetails.isEmpty() == false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final FileDetail[] files = values().toArray(new FileDetail[0]);
            out.writeVInt(files.length);
            for (FileDetail file : files) {
                file.writeTo(out);
            }
            if (out.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
                out.writeBoolean(complete);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (params.paramAsBoolean("detailed", false)) {
                builder.startArray(RecoveryState.Fields.DETAILS);
                for (FileDetail file : values()) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }

            return builder;
        }

        public void addFileDetails(String name, long length, boolean reused) {
            assert complete == false : "addFileDetail for [" + name + "] when file details are already complete";
            FileDetail existing = fileDetails.put(name, new FileDetail(name, length, reused));
            assert existing == null : "file [" + name + "] is already reported";
        }

        public void addRecoveredBytesToFile(String name, long bytes) {
            FileDetail file = fileDetails.get(name);
            assert file != null : "file [" + name + "] hasn't been reported";
            file.addRecoveredBytes(bytes);
        }

        public FileDetail get(String name) {
            return fileDetails.get(name);
        }

        public void setComplete() {
            complete = true;
        }

        public int size() {
            return fileDetails.size();
        }

        public boolean isEmpty() {
            return fileDetails.isEmpty();
        }

        public void clear() {
            fileDetails.clear();
            complete = false;
        }

        public Collection<FileDetail> values() {
            return fileDetails.values();
        }

        public boolean isComplete() {
            return complete;
        }
    }
}
