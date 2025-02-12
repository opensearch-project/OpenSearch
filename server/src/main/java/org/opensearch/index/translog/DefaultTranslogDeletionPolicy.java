/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;
import java.util.List;

/**
 * Default implementation for the {@link TranslogDeletionPolicy}. Plugins can override the default behaviour
 * via the {@link org.opensearch.plugins.EnginePlugin#getCustomTranslogDeletionPolicyFactory()}.
 * <p>
 * The default policy uses total number, size in bytes and maximum age for files.
 *
 * @opensearch.internal
 */

public class DefaultTranslogDeletionPolicy extends TranslogDeletionPolicy {
    private long retentionSizeInBytes;

    private long retentionAgeInMillis;

    private int retentionTotalFiles;

    public DefaultTranslogDeletionPolicy(long retentionSizeInBytes, long retentionAgeInMillis, int retentionTotalFiles) {
        super();
        this.retentionSizeInBytes = retentionSizeInBytes;
        this.retentionAgeInMillis = retentionAgeInMillis;
        this.retentionTotalFiles = retentionTotalFiles;
    }

    @Override
    public synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long minByLocks = getMinTranslogGenRequiredByLocks();
        long minByAge = getMinTranslogGenByAge(readers, writer, retentionAgeInMillis, currentTime());
        long minBySize = getMinTranslogGenBySize(readers, writer, retentionSizeInBytes);
        final long minByAgeAndSize;
        if (minBySize == Long.MIN_VALUE && minByAge == Long.MIN_VALUE) {
            // both size and age are disabled;
            minByAgeAndSize = Long.MAX_VALUE;
        } else {
            minByAgeAndSize = Math.max(minByAge, minBySize);
        }
        long minByNumFiles = getMinTranslogGenByTotalFiles(readers, writer, retentionTotalFiles);
        return Math.min(Math.max(minByAgeAndSize, minByNumFiles), minByLocks);
    }

    @Override
    public synchronized void setRetentionSizeInBytes(long bytes) {
        retentionSizeInBytes = bytes;
    }

    @Override
    public synchronized void setRetentionAgeInMillis(long ageInMillis) {
        retentionAgeInMillis = ageInMillis;
    }

    @Override
    protected synchronized void setRetentionTotalFiles(int retentionTotalFiles) {
        this.retentionTotalFiles = retentionTotalFiles;
    }
}
