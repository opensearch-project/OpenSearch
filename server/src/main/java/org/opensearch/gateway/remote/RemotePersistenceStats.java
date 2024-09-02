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
    PersistedStateStats remoteDiffDownloadStats;
    PersistedStateStats remoteFullDownloadStats;

    final String FULL_DOWNLOAD_STATS = "remote_full_download";
    final String DIFF_DOWNLOAD_STATS = "remote_diff_download";

    public RemotePersistenceStats() {
        remoteUploadStats = new RemoteUploadStats();
        remoteDiffDownloadStats = new PersistedStateStats(DIFF_DOWNLOAD_STATS);
        remoteFullDownloadStats = new PersistedStateStats(FULL_DOWNLOAD_STATS);
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

    public void stateFullDownloadSucceeded() {
        remoteFullDownloadStats.stateSucceeded();
    }

    public void stateDiffDownloadSucceeded() {
        remoteDiffDownloadStats.stateSucceeded();
    }

    public void stateFullDownloadTook(long durationMillis) {
        remoteFullDownloadStats.stateTook(durationMillis);
    }

    public void stateDiffDownloadTook(long durationMillis) {
        remoteDiffDownloadStats.stateTook(durationMillis);
    }

    public void stateFullDownloadFailed() {
        remoteFullDownloadStats.stateFailed();
    }

    public void stateDiffDownloadFailed() {
        remoteDiffDownloadStats.stateFailed();
    }

    public PersistedStateStats getUploadStats() {
        return remoteUploadStats;
    }

    public PersistedStateStats getRemoteDiffDownloadStats() {
        return remoteDiffDownloadStats;
    }

    public PersistedStateStats getRemoteFullDownloadStats() {
        return remoteFullDownloadStats;
    }

}
