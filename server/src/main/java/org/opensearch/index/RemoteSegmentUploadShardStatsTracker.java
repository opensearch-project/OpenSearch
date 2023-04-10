/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import java.util.HashMap;
import java.util.Map;

/**
 * Remote upload stats.
 *
 * @opensearch.internal
 */
public class RemoteSegmentUploadShardStatsTracker {

    public static final long UNASSIGNED = 0L;

    public RemoteSegmentUploadShardStatsTracker() {
        latestUploadFileNameLengthMap = new HashMap<>();
    }

    private volatile long refreshSeqNo = UNASSIGNED;

    private volatile long refreshTime = UNASSIGNED;

    private volatile long uploadBytesStarted = UNASSIGNED;

    private volatile long uploadBytesFailed = UNASSIGNED;

    private volatile long uploadBytesSucceeded = UNASSIGNED;

    private volatile long totalUploadsStarted = UNASSIGNED;

    private volatile long totalUploadsFailed = UNASSIGNED;

    private volatile long totalUploadsSucceeded = UNASSIGNED;

    /**
     * Keeps map of filename to bytes length of the most recent segments upload as part of refresh.
     */
    private final Map<String, Long> latestUploadFileNameLengthMap;

    public void incrementUploadBytesStarted(long bytes) {
        uploadBytesStarted += bytes;
    }

    public void incrementUploadBytesFailed(long bytes) {
        uploadBytesFailed += bytes;
    }

    public void incrementUploadBytesSucceeded(long bytes) {
        uploadBytesSucceeded += bytes;
    }

    public void incrementTotalUploadsStarted() {
        totalUploadsStarted += 1;
    }

    public void incrementTotalUploadsFailed() {
        totalUploadsFailed += 1;
    }

    public void incrementTotalUploadsSucceeded() {
        totalUploadsSucceeded += 1;
    }

}
