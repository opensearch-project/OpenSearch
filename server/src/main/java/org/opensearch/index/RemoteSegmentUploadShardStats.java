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
public class RemoteSegmentUploadShardStats {

    public static final long UNASSIGNED = -1L;

    public RemoteSegmentUploadShardStats() {
        latestUploadFileNameLengthMap = new HashMap<>();
    }

    private long refreshSeqNo = UNASSIGNED;

    private long refreshTime = UNASSIGNED;

    private long uploadBytesStarted = UNASSIGNED;

    private long uploadBytesFailed = UNASSIGNED;

    private long uploadBytesSucceeded = UNASSIGNED;

    private long totalUploadsStarted = UNASSIGNED;

    private long totalUploadsFailed = UNASSIGNED;

    private long totalUploadsSucceeded = UNASSIGNED;

    /**
     * Keeps map of filename to bytes length of the most recent segments upload as part of refresh.
     */
    private final Map<String, Long> latestUploadFileNameLengthMap;
}
