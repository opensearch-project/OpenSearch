/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * Interface for populating download stats of segments from remote store
 *
 * @opensearch.internal
 */
public interface SegmentDownloadListener {
    void beforeSync();

    void afterSync(long downloadedFilesSize, long startTimeInMs);

    void beforeFileDownload(long fileSize);

    void afterFileDownload(long fileSize);

    void fileDownloadFailed(long fileSize);
}
