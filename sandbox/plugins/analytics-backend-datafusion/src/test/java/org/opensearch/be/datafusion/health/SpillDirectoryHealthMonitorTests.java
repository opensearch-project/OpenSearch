/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.health;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.env.Environment;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class SpillDirectoryHealthMonitorTests extends OpenSearchTestCase {

    public void testProbeReportsWritableForNewTempDir() throws IOException {
        Path dir = createTempDir();
        SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(dir.toString());
        monitor.probeOnce();
        assertTrue(monitor.isWritable());
    }

    public void testProbeReportsNotWritableForReadOnlyDir() throws IOException {
        assumeTrue("POSIX permissions required", isPosixFs());
        Path dir = createTempDir();
        Set<PosixFilePermission> readOnly = PosixFilePermissions.fromString("r-xr-xr-x");
        Files.setPosixFilePermissions(dir, readOnly);
        try {
            SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(dir.toString());
            monitor.probeOnce();
            assertFalse(monitor.isWritable());
        } finally {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    public void testProbeReportsNotWritableForMissingDir() {
        Path missing = createTempDir().resolve("does-not-exist-" + randomAlphaOfLength(8));
        SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(missing.toString());
        monitor.probeOnce();
        assertFalse(monitor.isWritable());
    }

    public void testRecoveryFlipsWritableBack() throws IOException {
        assumeTrue("POSIX permissions required", isPosixFs());
        Path dir = createTempDir();
        SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(dir.toString());

        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
        try {
            monitor.probeOnce();
            assertFalse(monitor.isWritable());

            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
            monitor.probeOnce();
            assertTrue(monitor.isWritable());
        } finally {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    public void testProbeCleansUpLeakedFile() throws IOException {
        Path dir = createTempDir();
        SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(dir.toString());
        // Simulate a leaked probe file from a prior crashed iteration with the SAME UUID
        // by reading the path from the monitor (probe filename is per-instance constant).
        Path leaked = dir.resolve(monitor.probeFileName());
        Files.write(leaked, new byte[] { 1, 2, 3 });
        assertTrue(Files.exists(leaked));

        monitor.probeOnce();
        assertTrue(monitor.isWritable());
        assertFalse("probe file must not be left behind", Files.exists(leaked));
    }

    public void testIsWritableDefaultsTrueBeforeFirstProbe() {
        Path dir = createTempDir();
        SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(dir.toString());
        // Default before any probe runs: vacuously writable, so a stats query during
        // the first 60s window doesn't show a spurious unhealthy state.
        assertTrue(monitor.isWritable());
    }

    public void testTransitionLogsFireOncePerTransition() throws Exception {
        assumeTrue("POSIX permissions required", isPosixFs());
        Path dir = createTempDir();
        SpillDirectoryHealthMonitor monitor = new SpillDirectoryHealthMonitor(dir.toString());
        Logger monitorLogger = LogManager.getLogger(SpillDirectoryHealthMonitor.class);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(monitorLogger)) {
            // First healthy probe: previous=true, now=true => no log expected.
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no log on first healthy", monitorLogger.getName(), Level.INFO, "*")
            );
            monitor.probeOnce();
            appender.assertAllExpectationsMatched();
        }

        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
        try {
            try (MockLogAppender appender = MockLogAppender.createForLoggers(monitorLogger)) {
                // Healthy -> unhealthy: exactly one WARN.
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "warn on healthy->unhealthy",
                        monitorLogger.getName(),
                        Level.WARN,
                        "DataFusion spill directory*became unwritable*"
                    )
                );
                monitor.probeOnce();
                appender.assertAllExpectationsMatched();
            }

            try (MockLogAppender appender = MockLogAppender.createForLoggers(monitorLogger)) {
                // Subsequent unhealthy probe: previous=false, now=false => no log expected.
                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation("no log on consecutive unhealthy", monitorLogger.getName(), Level.WARN, "*")
                );
                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation("no info on consecutive unhealthy", monitorLogger.getName(), Level.INFO, "*")
                );
                monitor.probeOnce();
                appender.assertAllExpectationsMatched();
            }
        } finally {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
        }

        try (MockLogAppender appender = MockLogAppender.createForLoggers(monitorLogger)) {
            // Unhealthy -> healthy: exactly one INFO.
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "info on unhealthy->healthy",
                    monitorLogger.getName(),
                    Level.INFO,
                    "DataFusion spill directory*is writable again"
                )
            );
            monitor.probeOnce();
            appender.assertAllExpectationsMatched();
        }
    }

    private static boolean isPosixFs() {
        try {
            Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
            return Environment.getFileStore(tmp).supportsFileAttributeView("posix");
        } catch (IOException e) {
            return false;
        }
    }
}
