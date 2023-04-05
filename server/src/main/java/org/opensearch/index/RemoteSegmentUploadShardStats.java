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

    public RemoteSegmentUploadShardStats() {
        latestUploadFileNameLengthMap = new HashMap<>();
    }

    private long refreshSeqNo;

    private long refreshTime;

    private long uploadBytesStarted;

    private long uploadBytesFailed;

    private long uploadBytesSucceeded;

    private long totalUploadsStarted;

    private long totalUploadsFailed;

    private long totalUploadsSucceeded;

    /**
     * Keeps map of filename to bytes length of the most recent segments upload as part of refresh.
     */
    private final Map<String, Long> latestUploadFileNameLengthMap;
}
