/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Tracks remote store segment download and upload stats
 *
 * @opensearch.internal
 */
public class RemoteSegmentStats implements Writeable, ToXContentFragment {
    private long uploadBytesStarted;
    private long uploadBytesFailed;
    private long uploadBytesSucceeded;
    private long downloadBytesStarted;
    private long downloadBytesFailed;
    private long downloadBytesSucceeded;
    private long maxRefreshTimeLag;
    private long maxRefreshBytesLag;

    public RemoteSegmentStats() {}

    public RemoteSegmentStats(StreamInput in) throws IOException {
        uploadBytesStarted = in.readLong();
        uploadBytesFailed = in.readLong();
        uploadBytesSucceeded = in.readLong();
        downloadBytesStarted = in.readLong();
        downloadBytesFailed = in.readLong();
        downloadBytesSucceeded = in.readLong();
        maxRefreshTimeLag = in.readLong();
        maxRefreshBytesLag = in.readLong();
    }

    public long getUploadBytesStarted() {
        return uploadBytesStarted;
    }

    public void addUploadBytesStarted(long uploadsStarted) {
        this.uploadBytesStarted += uploadsStarted;
    }

    public long getUploadBytesFailed() {
        return uploadBytesFailed;
    }

    public void addUploadBytesFailed(long uploadsFailed) {
        this.uploadBytesFailed += uploadsFailed;
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded;
    }

    public void addUploadBytesSucceeded(long uploadsSucceeded) {
        this.uploadBytesSucceeded += uploadsSucceeded;
    }

    public long getDownloadBytesStarted() {
        return downloadBytesStarted;
    }

    public void addDownloadBytesStarted(long downloadsStarted) {
        this.downloadBytesStarted += downloadsStarted;
    }

    public long getDownloadBytesFailed() {
        return downloadBytesFailed;
    }

    public void addDownloadBytesFailed(long downloadsFailed) {
        this.downloadBytesFailed += downloadsFailed;
    }

    public long getDownloadBytesSucceeded() {
        return downloadBytesSucceeded;
    }

    public void addDownloadBytesSucceeded(long downloadsSucceeded) {
        this.downloadBytesSucceeded += downloadsSucceeded;
    }

    public long getMaxRefreshTimeLag() {
        return maxRefreshTimeLag;
    }

    public void setMaxRefreshTimeLag(long maxRefreshTimeLag) {
        this.maxRefreshTimeLag = Math.max(this.maxRefreshTimeLag, maxRefreshTimeLag);
    }

    public long getMaxRefreshBytesLag() {
        return maxRefreshBytesLag;
    }

    public void setMaxRefreshBytesLag(long maxRefreshBytesLag) {
        this.maxRefreshBytesLag = maxRefreshBytesLag;
    }

    public void add(RemoteSegmentStats remoteSegmentStats) {
        if (remoteSegmentStats != null) {
            this.uploadBytesStarted += remoteSegmentStats.getUploadBytesStarted();
            this.uploadBytesSucceeded += remoteSegmentStats.getUploadBytesSucceeded();
            this.uploadBytesFailed += remoteSegmentStats.getUploadBytesFailed();
            this.downloadBytesStarted += remoteSegmentStats.getDownloadBytesStarted();
            this.downloadBytesFailed += remoteSegmentStats.getDownloadBytesFailed();
            this.downloadBytesSucceeded += remoteSegmentStats.getDownloadBytesSucceeded();
            this.maxRefreshTimeLag = Math.max(this.maxRefreshTimeLag, remoteSegmentStats.getMaxRefreshTimeLag());
            this.maxRefreshBytesLag = Math.max(this.maxRefreshBytesLag, remoteSegmentStats.getMaxRefreshBytesLag());
        }
    }

    public void buildRemoteSegmentStats(RemoteSegmentTransferTracker.Stats trackerStats) {
        this.uploadBytesStarted = trackerStats.uploadBytesStarted;
        this.uploadBytesFailed = trackerStats.uploadBytesFailed;
        this.uploadBytesSucceeded = trackerStats.uploadBytesSucceeded;
        this.downloadBytesSucceeded = trackerStats.directoryFileTransferTrackerStats.transferredBytesSucceeded;
        this.downloadBytesStarted = trackerStats.directoryFileTransferTrackerStats.transferredBytesStarted;
        this.downloadBytesFailed = trackerStats.directoryFileTransferTrackerStats.transferredBytesFailed;
        this.maxRefreshTimeLag = trackerStats.refreshTimeLagMs;
        this.maxRefreshBytesLag = trackerStats.bytesLag;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(uploadBytesStarted);
        out.writeLong(uploadBytesFailed);
        out.writeLong(uploadBytesSucceeded);
        out.writeLong(downloadBytesStarted);
        out.writeLong(downloadBytesFailed);
        out.writeLong(downloadBytesSucceeded);
        out.writeLong(maxRefreshTimeLag);
        out.writeLong(maxRefreshBytesLag);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REMOTE_STORE);
        builder.startObject(Fields.UPLOAD);
        builder.startObject(Fields.TOTAL_UPLOADS);
        builder.humanReadableField(Fields.STARTED_BYTES, Fields.STARTED, new ByteSizeValue(uploadBytesStarted));
        builder.humanReadableField(Fields.SUCCEEDED_BYTES, Fields.SUCCEEDED, new ByteSizeValue(uploadBytesSucceeded));
        builder.humanReadableField(Fields.FAILED_BYTES, Fields.FAILED, new ByteSizeValue(uploadBytesFailed));
        builder.endObject();
        builder.humanReadableField(Fields.MAX_REFRESH_TIME_LAG_IN_MILLIS, Fields.MAX_REFRESH_TIME_LAG, new TimeValue(maxRefreshTimeLag));
        builder.humanReadableField(Fields.MAX_REFRESH_SIZE_LAG_IN_MILLIS, Fields.MAX_REFRESH_SIZE_LAG, new ByteSizeValue(maxRefreshBytesLag));
        builder.endObject();
        builder.startObject(Fields.DOWNLOAD);
        builder.startObject(Fields.TOTAL_DOWNLOADS);
        builder.humanReadableField(Fields.STARTED_BYTES, Fields.STARTED, new ByteSizeValue(downloadBytesStarted));
        builder.humanReadableField(Fields.SUCCEEDED_BYTES, Fields.SUCCEEDED, new ByteSizeValue(downloadBytesSucceeded));
        builder.humanReadableField(Fields.FAILED_BYTES, Fields.FAILED, new ByteSizeValue(downloadBytesFailed));
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String REMOTE_STORE = "remote_store";
        static final String UPLOAD = "upload";
        static final String DOWNLOAD = "download";
        static final String TOTAL_UPLOADS = "total_uploads";
        static final String TOTAL_DOWNLOADS = "total_downloads";
        static final String STARTED = "started";
        static final String STARTED_BYTES = "started_bytes";
        static final String FAILED = "failed";
        static final String FAILED_BYTES = "failed_bytes";
        static final String SUCCEEDED = "succeeded";
        static final String SUCCEEDED_BYTES = "succeeded_bytes";
        static final String MAX_REFRESH_TIME_LAG = "max_refresh_time_lag";
        static final String MAX_REFRESH_TIME_LAG_IN_MILLIS = "max_refresh_time_lag_in_millis";
        static final String MAX_REFRESH_SIZE_LAG = "max_refresh_size_lag";
        static final String MAX_REFRESH_SIZE_LAG_IN_MILLIS = "max_refresh_size_lag_in_bytes";
    }
}
