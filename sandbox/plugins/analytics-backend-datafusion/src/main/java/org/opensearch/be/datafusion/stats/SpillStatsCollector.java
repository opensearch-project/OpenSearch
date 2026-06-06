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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Builds a {@link SpillStats} snapshot from the configured spill directory and
 * memory-limit setting. Stateless — every call hits {@link Files#getFileStore}
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
     * Collect a spill-stats snapshot.
     *
     * @param spillDirectory   the configured value of {@code datafusion.spill_directory};
     *                         empty string means spill is disabled
     * @param spillMemoryLimit the resolved value of {@code datafusion.spill_memory_limit_bytes}
     *                         in bytes (the user-facing setting value)
     * @return a populated {@link SpillStats}; on error, all byte fields are 0 but the
     *         {@code directory} and {@code disk_reserved_bytes} fields are preserved
     */
    public static SpillStats collect(String spillDirectory, long spillMemoryLimit) {
        if (spillDirectory == null || spillDirectory.isEmpty()) {
            return new SpillStats("", 0L, 0L, 0L, 0L);
        }

        try {
            Path path = Path.of(spillDirectory);
            if (!Files.exists(path)) {
                // Directory may be created later by a host boot script (first-boot mount).
                // Don't fail; report zeros for capacity but keep directory + reserve visible.
                return new SpillStats(spillDirectory, 0L, 0L, 0L, spillMemoryLimit);
            }
            FileStore fs = Files.getFileStore(path);
            long total = fs.getTotalSpace();
            long available = fs.getUsableSpace();
            long used = total - available;
            return new SpillStats(spillDirectory, total, available, used, spillMemoryLimit);
        } catch (IOException e) {
            logger.warn("Failed to read filesystem stats for spill directory [{}]: {}", spillDirectory, e.getMessage());
            return new SpillStats(spillDirectory, 0L, 0L, 0L, spillMemoryLimit);
        } catch (RuntimeException e) {
            logger.warn("Unexpected error reading spill stats for [{}]: {}", spillDirectory, e.getMessage());
            return new SpillStats(spillDirectory, 0L, 0L, 0L, spillMemoryLimit);
        }
    }
}
