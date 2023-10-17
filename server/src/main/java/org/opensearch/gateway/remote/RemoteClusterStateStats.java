/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Remote cluster state related stats.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateStats implements Writeable, ToXContentObject {
    private AtomicLong totalUploadTimeInMS = new AtomicLong(0);
    private AtomicLong uploadFailedCount = new AtomicLong(0);
    private AtomicLong uploadSuccessCount = new AtomicLong(0);
    private AtomicLong cleanupAttemptFailedCount = new AtomicLong(0);

    public RemoteClusterStateStats() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(true);
        out.writeVLong(uploadSuccessCount.get());
        out.writeVLong(uploadFailedCount.get());
        out.writeVLong(totalUploadTimeInMS.get());
        out.writeVLong(cleanupAttemptFailedCount.get());
    }

    public RemoteClusterStateStats(StreamInput in) throws IOException {
        this.uploadSuccessCount = new AtomicLong(in.readVLong());
        this.uploadFailedCount = new AtomicLong(in.readVLong());
        this.totalUploadTimeInMS = new AtomicLong(in.readVLong());
        this.cleanupAttemptFailedCount = new AtomicLong(in.readVLong());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REMOTE_UPLOAD);
        builder.field(Fields.UPDATE_COUNT, getUploadSuccessCount());
        builder.field(Fields.FAILED_COUNT, getUploadFailedCount());
        builder.field(Fields.TOTAL_TIME_IN_MILLIS, getTotalUploadTimeInMS());
        builder.field(Fields.CLEANUP_ATTEMPT_FAILED_COUNT, getCleanupAttemptFailedCount());
        builder.endObject();
        return builder;
    }

    public void stateUploadFailed() {
        uploadFailedCount.incrementAndGet();
    }

    public void stateUploaded() {
        uploadSuccessCount.incrementAndGet();
    }

    /**
     * Expects user to send time taken in milliseconds.
     *
     * @param timeTakenInUpload time taken in uploading the cluster state to remote
     */
    public void stateUploadTook(long timeTakenInUpload) {
        totalUploadTimeInMS.addAndGet(timeTakenInUpload);
    }

    public void cleanUpAttemptFailed() {
        cleanupAttemptFailedCount.incrementAndGet();
    }

    public long getTotalUploadTimeInMS() {
        return totalUploadTimeInMS.get();
    }

    public long getUploadFailedCount() {
        return uploadFailedCount.get();
    }

    public long getUploadSuccessCount() {
        return uploadSuccessCount.get();
    }

    public long getCleanupAttemptFailedCount() {
        return cleanupAttemptFailedCount.get();
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String REMOTE_UPLOAD = "remote_upload";
        static final String UPDATE_COUNT = "update_count";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String FAILED_COUNT = "failed_count";
        static final String CLEANUP_ATTEMPT_FAILED_COUNT = "cleanup_attempt_failed_count";
    }
}
