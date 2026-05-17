/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Pre-merge disk space guard.
 *
 * <p>Before a per-format merge writes its output file(s), the merger calls
 * {@link #check(IndexSettings, Path, long, String)} to confirm that the target
 * file system has enough usable space to hold the projected merged output (with a
 * configurable safety multiplier).
 *
 * <p>The projected output size is approximated by {@code estimatedInputBytes}
 * (the sum of input file sizes for the merge). The merged output for a pluggable
 * data format like Parquet or Lucene is bounded above by the input size in the
 * worst case, but transient working files (e.g., temp files, partial writes) can
 * push the actual peak usage higher. The safety multiplier provides headroom for
 * those transients.
 *
 * <p>If the check fails, an {@link InsufficientDiskSpaceException} is thrown and
 * the merge is aborted before any native or Lucene work is performed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class MergePreflightChecker {

    private static final Logger logger = LogManager.getLogger(MergePreflightChecker.class);

    private MergePreflightChecker() {}

    /**
     * Verifies that {@code targetDirectory} has enough usable space for a merge
     * that consumes {@code estimatedInputBytes} of input.
     *
     * <p>If {@link IndexSettings#INDEX_MERGE_DISK_SPACE_CHECK_ENABLED} is {@code false},
     * this method is a no-op.
     *
     * @param indexSettings        index settings providing the toggle and safety multiplier
     * @param targetDirectory      directory where the merged output will be written
     * @param estimatedInputBytes  sum of input file sizes (used as an upper-bound estimate)
     * @param formatName           data format name, used only for diagnostic messages
     * @throws InsufficientDiskSpaceException if usable space is below the required threshold
     * @throws IOException                    if querying the file store fails
     */
    public static void check(IndexSettings indexSettings, Path targetDirectory, long estimatedInputBytes, String formatName)
        throws IOException {
        Objects.requireNonNull(indexSettings, "indexSettings must not be null");
        Objects.requireNonNull(targetDirectory, "targetDirectory must not be null");
        Objects.requireNonNull(formatName, "formatName must not be null");

        if (indexSettings.getValue(IndexSettings.INDEX_MERGE_DISK_SPACE_CHECK_ENABLED) == false) {
            return;
        }

        if (estimatedInputBytes <= 0) {
            // Nothing to check — caller could not determine input size.
            return;
        }

        double safetyMultiplier = indexSettings.getValue(IndexSettings.INDEX_MERGE_DISK_SPACE_CHECK_SAFETY_MULTIPLIER);
        long requiredBytes = (long) Math.ceil(estimatedInputBytes * safetyMultiplier);

        FileStore fileStore = Files.getFileStore(resolveExistingAncestor(targetDirectory));
        long usableBytes = fileStore.getUsableSpace();

        if (usableBytes < requiredBytes) {
            String message = String.format(
                java.util.Locale.ROOT,
                "Aborting [%s] merge for index [%s] in [%s]: usable disk space [%d bytes] is below required [%d bytes] "
                    + "(estimated input [%d bytes] * safety multiplier [%.2f])",
                formatName,
                indexSettings.getIndex().getName(),
                targetDirectory,
                usableBytes,
                requiredBytes,
                estimatedInputBytes,
                safetyMultiplier
            );
            logger.warn(message);
            throw new InsufficientDiskSpaceException(message);
        }

        logger.debug(
            "Pre-merge disk space check passed for [{}]: usable={} bytes, required={} bytes (input={} bytes, multiplier={})",
            formatName,
            usableBytes,
            requiredBytes,
            estimatedInputBytes,
            safetyMultiplier
        );
    }

    /**
     * Resolves the closest existing ancestor of {@code path}. The merge target directory
     * may not exist yet (the merger creates it on the fly), but {@link Files#getFileStore(Path)}
     * requires an existing path. Walking up to the closest parent that exists gives us the
     * correct file store for the same mount point.
     */
    private static Path resolveExistingAncestor(Path path) {
        Path current = path.toAbsolutePath();
        while (current != null && Files.exists(current) == false) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IllegalArgumentException("No existing ancestor found for path [" + path + "]");
        }
        return current;
    }
}
