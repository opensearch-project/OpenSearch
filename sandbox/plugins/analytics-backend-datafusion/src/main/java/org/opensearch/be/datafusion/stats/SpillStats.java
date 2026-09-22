/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for the DataFusion spill directory — a node-local view of the disk volume
 * used by DataFusion's {@code DiskManager} to stage intermediate state when
 * operators (HashAggregate, Sort, TopK) exceed the in-memory pool.
 *
 * <p>Renders under the {@code "disk_spill"} key in the plugin stats endpoint.
 *
 * <p>{@code disk_used_bytes} is filesystem-level (total - available), not a
 * DataFusion-tracked counter. Correct for dedicated EBS volumes; would lie on
 * shared volumes. Swap to a {@code DiskManager}-tracked counter via FFI if
 * the deployment assumption changes.
 */
public class SpillStats implements Writeable, ToXContentFragment {

    private final String directory;
    private final long diskTotalBytes;
    private final long diskAvailableBytes;
    private final long diskUsedBytes;
    private final long diskReservedBytes;
    /**
     * Number of {@code *.stale} entries currently in the spill directory.
     * Each entry is an orphan from a prior boot whose async cleanup has not yet
     * removed it. A persistently non-zero value across boots indicates an
     * operator-actionable failure (immutable bit, broken perms, etc.) preventing
     * the background cleanup thread from completing.
     */
    private final long staleEntryCount;

    public SpillStats(
        String directory,
        long diskTotalBytes,
        long diskAvailableBytes,
        long diskUsedBytes,
        long diskReservedBytes,
        long staleEntryCount
    ) {
        this.directory = directory == null ? "" : directory;
        this.diskTotalBytes = diskTotalBytes;
        this.diskAvailableBytes = diskAvailableBytes;
        this.diskUsedBytes = diskUsedBytes;
        this.diskReservedBytes = diskReservedBytes;
        this.staleEntryCount = staleEntryCount;
    }

    public SpillStats(StreamInput in) throws IOException {
        this.directory = in.readString();
        this.diskTotalBytes = in.readVLong();
        this.diskAvailableBytes = in.readVLong();
        this.diskUsedBytes = in.readVLong();
        this.diskReservedBytes = in.readVLong();
        this.staleEntryCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(directory);
        out.writeVLong(diskTotalBytes);
        out.writeVLong(diskAvailableBytes);
        out.writeVLong(diskUsedBytes);
        out.writeVLong(diskReservedBytes);
        out.writeVLong(staleEntryCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("disk_spill");
        builder.field("directory", directory);
        builder.field("disk_total_bytes", diskTotalBytes);
        builder.field("disk_available_bytes", diskAvailableBytes);
        builder.field("disk_used_bytes", diskUsedBytes);
        builder.field("disk_reserved_bytes", diskReservedBytes);
        builder.field("stale_entry_count", staleEntryCount);
        builder.endObject();
        return builder;
    }

    public String getDirectory() {
        return directory;
    }

    public long getDiskTotalBytes() {
        return diskTotalBytes;
    }

    public long getDiskAvailableBytes() {
        return diskAvailableBytes;
    }

    public long getDiskUsedBytes() {
        return diskUsedBytes;
    }

    public long getDiskReservedBytes() {
        return diskReservedBytes;
    }

    public long getStaleEntryCount() {
        return staleEntryCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpillStats that = (SpillStats) o;
        return diskTotalBytes == that.diskTotalBytes
            && diskAvailableBytes == that.diskAvailableBytes
            && diskUsedBytes == that.diskUsedBytes
            && diskReservedBytes == that.diskReservedBytes
            && staleEntryCount == that.staleEntryCount
            && Objects.equals(directory, that.directory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directory, diskTotalBytes, diskAvailableBytes, diskUsedBytes, diskReservedBytes, staleEntryCount);
    }
}
