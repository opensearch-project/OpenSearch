/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.index.seqno.RetentionLeases;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public class DefaultTranslogDeletionPolicy extends TranslogDeletionPolicy {
    public DefaultTranslogDeletionPolicy(long retentionSizeInBytes, long retentionAgeInMillis, int retentionTotalFiles) {
        super(retentionSizeInBytes, retentionAgeInMillis, retentionTotalFiles);
    }

    public DefaultTranslogDeletionPolicy(
        long retentionSizeInBytes,
        long retentionAgeInMillis,
        int retentionTotalFiles,
        Supplier<RetentionLeases> retentionLeasesSupplier
    ) {
        super(retentionSizeInBytes, retentionAgeInMillis, retentionTotalFiles, retentionLeasesSupplier);
    }

    @Override
    public synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long minByLocks = getMinTranslogGenRequiredByLocks();
        long minByAge = getMinTranslogGenByAge(readers, writer, retentionAgeInMillis, currentTime());
        long minBySize = getMinTranslogGenBySize(readers, writer, retentionSizeInBytes);
        long minByRetentionLeasesAndSize = Long.MAX_VALUE;
        if (shouldPruneTranslogByRetentionLease) {
            // If retention size is specified, size takes precedence.
            long minByRetentionLeases = getMinTranslogGenByRetentionLease(readers, writer, retentionLeasesSupplier);
            minByRetentionLeasesAndSize = Math.max(minBySize, minByRetentionLeases);
        }
        final long minByAgeAndSize;
        if (minBySize == Long.MIN_VALUE && minByAge == Long.MIN_VALUE) {
            // both size and age are disabled;
            minByAgeAndSize = Long.MAX_VALUE;
        } else {
            minByAgeAndSize = Math.max(minByAge, minBySize);
        }
        long minByNumFiles = getMinTranslogGenByTotalFiles(readers, writer, retentionTotalFiles);
        long minByTranslogGenSettings = Math.min(Math.max(minByAgeAndSize, minByNumFiles), minByLocks);
        return Math.min(minByTranslogGenSettings, minByRetentionLeasesAndSize);
    }

    private long getMinTranslogGenRequiredByLocks() {
        return translogRefCounts.keySet().stream().reduce(Math::min).orElse(Long.MAX_VALUE);
    }
}
