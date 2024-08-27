/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.coordination.PersistedStateStats;

/**
 * Remote state related extended stats.
 *
 * @opensearch.internal
 */
public class RemotePersistenceStats {

    RemoteUploadStats remoteUploadStats;
    RemoteDownloadStats remoteDownloadStats;

    public RemotePersistenceStats() {
        remoteUploadStats = new RemoteUploadStats();
        remoteDownloadStats = new RemoteDownloadStats();
    }

    public void cleanUpAttemptFailed() {
        remoteUploadStats.cleanUpAttemptFailed();
    }

    public long getCleanupAttemptFailedCount() {
        return remoteUploadStats.getCleanupAttemptFailedCount();
    }

    public void indexRoutingFilesCleanupAttemptFailed() {
        remoteUploadStats.indexRoutingFilesCleanupAttemptFailed();
    }

    public long getIndexRoutingFilesCleanupAttemptFailedCount() {
        return remoteUploadStats.getIndexRoutingFilesCleanupAttemptFailedCount();
    }

    public void indicesRoutingDiffFileCleanupAttemptFailed() {
        remoteUploadStats.indicesRoutingDiffFileCleanupAttemptFailed();
    }

    public long getIndicesRoutingDiffFileCleanupAttemptFailedCount() {
        return remoteUploadStats.getIndicesRoutingDiffFileCleanupAttemptFailedCount();
    }

    public void stateUploadSucceeded() {
        remoteUploadStats.stateSucceeded();
    }

    public void stateUploadTook(long durationMillis) {
        remoteUploadStats.stateTook(durationMillis);
    }

    public void stateUploadFailed() {
        remoteUploadStats.stateFailed();
    }

    public void stateDownloadSucceeded() {
        remoteDownloadStats.stateSucceeded();
    }

    public void stateDownloadTook(long durationMillis) {
        remoteDownloadStats.stateTook(durationMillis);
    }

    public void stateDownloadFailed() {
        remoteDownloadStats.stateFailed();
    }

    public PersistedStateStats getUploadStats() {
        return remoteUploadStats;
    }

    public PersistedStateStats getDownloadStats() {
        return remoteDownloadStats;
    }

    public void diffDownloadState() {
        remoteDownloadStats.diffDownloadState();
    }

    public void fullDownloadState() {
        remoteDownloadStats.fullDownloadState();
    }

}
