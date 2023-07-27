/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

/**
 * Interface with hooks executed on segment download events
 *
 * @opensearch.internal
 */
public interface SegmentDownloadListener {
    /**
     * Executed before segment download starts
     */
    void beforeDownload();

    /**
     * Executed after segment download completes
     */
    void afterDownload();

    /**
     * Executed if segment download fails
     */
    void downloadFailed();
}
