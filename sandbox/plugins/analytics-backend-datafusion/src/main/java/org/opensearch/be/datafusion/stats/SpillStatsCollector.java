/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.env.Environment;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Builds a {@link SpillStats} snapshot from the configured spill directory and
 * memory-limit setting. Stateless — every call hits {@link Environment#getFileStore}
 * to read filesystem capacity. Cost: one syscall per call (~1µs).
 *
 * <p>{@code disk_used_bytes} is computed as {@code total - available}. This is
 * filesystem-level usage, not DataFusion-tracked usage; it's correct for
 * dedicated spill volumes (the current production deployment) but would
 * mis-attribute capacity on shared volumes. If the deployment assumption
 * changes, replace this with a {@code DiskManager}-tracked counter exposed via
 * FFI.
 */
public final class SpillStatsCollector {

    private static final Logger logger = LogManager.getLogger(SpillStatsCollector.class);

    private SpillStatsCollector() {}

    /**
     * Collect a spill-stats snapshot, threading the writability signal from
     * {@code SpillDirectoryHealthMonitor}.
     *
     * @param spillDirectory    the configured value of {@code datafusion.spill_directory};
     *                          empty string means spill is disabled
     * @param spillMemoryLimit  the resolved value of {@code datafusion.spill_memory_limit_bytes}
     *                          in bytes (the user-facing setting value)
     * @param directoryWritable the most recent result of the runtime writability probe.
     *                          When spill is disabled or the monitor is absent, callers
     *                          should pass {@code true} (vacuously writable).
     * @return a populated {@link SpillStats}; on error, all byte fields are 0 but the
     *         {@code directory} and {@code disk_reserved_bytes} fields are preserved.
     *         {@code directory_writable} is forced to {@code true} when {@code spillDirectory}
     *         is empty (spill disabled — vacuously writable); otherwise the passed
     *         {@code directoryWritable} flag is propagated.
     */
    public static SpillStats collect(String spillDirectory, long spillMemoryLimit, boolean directoryWritable) {
        if (spillDirectory == null || spillDirectory.isEmpty()) {
            // Spill disabled — vacuously writable; ignore caller's flag.
            return new SpillStats("", 0L, 0L, 0L, 0L, true);
        }

        // SecurityManager: spill_directory is operator-configured and not on any path the
        // core security policy grants by default. Elevate via doPrivileged using the plugin's
        // FilePermission grant in plugin-security.policy.
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<SpillStats>) () -> {
                Path path = Path.of(spillDirectory);
                if (!Files.exists(path)) {
                    return new SpillStats(spillDirectory, 0L, 0L, 0L, spillMemoryLimit, directoryWritable);
                }
                FileStore fs = Environment.getFileStore(path);
                // Clamp negatives from FileStore — JDK-8162520: getTotalSpace/getUsableSpace can
                // overflow into negative values on filesystems > 8 EiB. Mirrors FsProbe.adjustForHugeFilesystems
                // and prevents writeVLong from rejecting negatives during stats serialization.
                long total = clampNonNegative(fs.getTotalSpace());
                long available = clampNonNegative(fs.getUsableSpace());
                long used = Math.max(0L, total - available);
                return new SpillStats(spillDirectory, total, available, used, spillMemoryLimit, directoryWritable);
            });
        } catch (PrivilegedActionException pae) {
            Throwable cause = pae.getCause() != null ? pae.getCause() : pae;
            logger.warn("Failed to read filesystem stats for spill directory [{}]: {}", spillDirectory, cause.getMessage());
            return new SpillStats(spillDirectory, 0L, 0L, 0L, spillMemoryLimit, directoryWritable);
        } catch (RuntimeException e) {
            logger.warn("Unexpected error reading spill stats for [{}]: {}", spillDirectory, e.getMessage());
            return new SpillStats(spillDirectory, 0L, 0L, 0L, spillMemoryLimit, directoryWritable);
        }
    }

    /** See {@code FsProbe#adjustForHugeFilesystems} and JDK-8162520. */
    private static long clampNonNegative(long bytes) {
        return bytes < 0 ? Long.MAX_VALUE : bytes;
    }
}
