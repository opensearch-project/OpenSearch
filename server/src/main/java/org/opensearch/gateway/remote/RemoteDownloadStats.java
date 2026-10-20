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

/**
 * Download stats for remote state
 *
 * @opensearch.internal
 */
public class RemoteDownloadStats extends PersistedStateStats {
    static final String CHECKSUM_VALIDATION_FAILED_COUNT = "checksum_validation_failed_count";
    private AtomicLong checksumValidationFailedCount = new AtomicLong(0);
    public static final String INCOMING_PUBLICATION_FAILED_COUNT = "incoming_publication_failed_count";
    private AtomicLong incomingPublicationFailedCount = new AtomicLong(0);
    static final String CURRENT_APPLICATION_DURATION_MS = "current_application_duration_ms";
    private AtomicLong currentApplicationDurationMs = new AtomicLong(0);

    public RemoteDownloadStats(String statsName) {
        super(statsName);
        addToExtendedFields(CHECKSUM_VALIDATION_FAILED_COUNT, checksumValidationFailedCount);
        addToExtendedFields(INCOMING_PUBLICATION_FAILED_COUNT, incomingPublicationFailedCount);
        addToExtendedFields(CURRENT_APPLICATION_DURATION_MS, currentApplicationDurationMs);
    }

    public void checksumValidationFailedCount() {
        checksumValidationFailedCount.incrementAndGet();
    }

    public long getChecksumValidationFailedCount() {
        return checksumValidationFailedCount.get();
    }

    public void incomingPublicationFailedCount() {
        incomingPublicationFailedCount.incrementAndGet();
    }

    public long getIncomingPublicationFailedCount() {
        return incomingPublicationFailedCount.get();
    }

    public void setCurrentApplicationDurationMs(long durationMs) {
        currentApplicationDurationMs.set(durationMs);
    }

    public long getCurrentApplicationDurationMs() {
        return currentApplicationDurationMs.get();
    }
}
