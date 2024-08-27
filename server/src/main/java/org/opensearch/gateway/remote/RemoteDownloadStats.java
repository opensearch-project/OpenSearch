/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.coordination.PersistedStateStats;

import java.util.concurrent.atomic.AtomicLong;

public class RemoteDownloadStats extends PersistedStateStats {
    static final String REMOTE_DOWNLOAD = "remote_download";
    static final String DIFF_DOWNLOAD = "diff_download";
    private AtomicLong diffDownloadCount = new AtomicLong(0);
    static final String FULL_DOWNLOAD = "full_download";
    private AtomicLong fullDownloadCount = new AtomicLong(0);

    public RemoteDownloadStats() {
        super(REMOTE_DOWNLOAD);
        addToExtendedFields(DIFF_DOWNLOAD, diffDownloadCount);
        addToExtendedFields(FULL_DOWNLOAD, fullDownloadCount);
    }

    public void diffDownloadState() {
        diffDownloadCount.incrementAndGet();
    }

    public void fullDownloadState() {
        fullDownloadCount.incrementAndGet();
    }

}
