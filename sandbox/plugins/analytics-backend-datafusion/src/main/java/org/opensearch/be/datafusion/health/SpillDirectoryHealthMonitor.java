/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.health;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.common.UUIDs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Periodically probes the configured DataFusion spill directory by writing and
 * deleting a small file. Exposes a {@code volatile} writable flag for the spill
 * stats endpoint and logs once on each healthy/unhealthy transition.
 *
 * <p>Probe is intentionally minimal — write+delete only, no fsync. Spill is
 * transient working state, not durable cluster state, so detecting permission /
 * mount / capacity failures is sufficient.
 *
 * <p>Lifecycle: constructed by {@code DataFusionPlugin.createComponents} when
 * {@code datafusion.spill_directory} is non-empty; cancelled in
 * {@code DataFusionPlugin.close()}.
 */
public final class SpillDirectoryHealthMonitor implements Runnable {

    private static final Logger logger = LogManager.getLogger(SpillDirectoryHealthMonitor.class);

    private static final String PROBE_FILE_PREFIX = ".opensearch_df_spill_probe_";

    private final Path spillDirectory;
    private final String probeFileName;
    private final byte[] probePayload;

    /**
     * Default {@code true} — vacuously writable until the first probe runs. Avoids a
     * spurious unhealthy report during the 60s window between node start and the first
     * probe iteration. The boot probe in {@code DataFusionPlugin} has already verified
     * writability before this monitor is constructed, so the optimistic default is safe.
     */
    private volatile boolean writable = true;

    public SpillDirectoryHealthMonitor(String spillDirectory) {
        if (spillDirectory == null || spillDirectory.isEmpty()) {
            throw new IllegalArgumentException("spillDirectory must be non-empty; the monitor must not be created when spill is disabled");
        }
        this.spillDirectory = Path.of(spillDirectory);
        String uuid = UUIDs.randomBase64UUID();
        this.probeFileName = PROBE_FILE_PREFIX + uuid;
        this.probePayload = uuid.getBytes(StandardCharsets.UTF_8);
    }

    /** True if the most recent probe (or absence of any probe) reports writable. */
    public boolean isWritable() {
        return writable;
    }

    /** Probe filename — exposed for test cleanup verification only. */
    public String probeFileName() {
        return probeFileName;
    }

    /** Run a single probe iteration synchronously. Public for direct unit-test invocation. */
    public void probeOnce() {
        Path tempPath = spillDirectory.resolve(probeFileName);
        boolean nowWritable;
        String error = "";
        // SecurityManager: spill_directory is operator-configured and not on any path the
        // core security policy grants by default. Elevate via doPrivileged using the plugin's
        // FilePermission grant in plugin-security.policy.
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                Files.deleteIfExists(tempPath);
                Files.write(tempPath, probePayload, StandardOpenOption.CREATE_NEW);
                Files.delete(tempPath);
                return null;
            });
            nowWritable = true;
        } catch (PrivilegedActionException pae) {
            Throwable cause = pae.getCause() != null ? pae.getCause() : pae;
            nowWritable = false;
            error = cause.getClass().getSimpleName() + ": " + cause.getMessage();
        } catch (Exception e) {
            nowWritable = false;
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        boolean previous = this.writable;
        this.writable = nowWritable;

        if (previous && !nowWritable) {
            logger.warn("DataFusion spill directory [{}] became unwritable: {}", spillDirectory, error);
        } else if (!previous && nowWritable) {
            logger.info("DataFusion spill directory [{}] is writable again", spillDirectory);
        }
    }

    @Override
    public void run() {
        try {
            probeOnce();
        } catch (Exception e) {
            // Defensive: probeOnce() catches Exception internally. This catch only
            // fires if probeOnce()'s implementation changes and an unchecked
            // Exception escapes — keep the scheduled task alive by recording
            // unwritable state. We intentionally do NOT catch Throwable: a true
            // VM-fatal Error (e.g. OutOfMemoryError) should propagate so the JVM
            // can fail fast rather than mask cascading damage in a periodic task.
            logger.error("Unexpected error in spill directory probe; marking unwritable", e);
            this.writable = false;
        }
    }
}
