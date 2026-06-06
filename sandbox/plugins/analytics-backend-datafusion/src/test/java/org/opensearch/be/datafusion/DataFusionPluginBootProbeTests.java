/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.hamcrest.Matchers.containsString;

public class DataFusionPluginBootProbeTests extends OpenSearchTestCase {

    public void testProbeAcceptsEmptyDirectoryAsNoop() {
        // Empty path means spill is disabled — boot probe must skip without throwing.
        DataFusionPlugin.probeSpillDirectoryWritable("");
    }

    public void testProbeAcceptsNullAsNoop() {
        DataFusionPlugin.probeSpillDirectoryWritable(null);
    }

    public void testProbeSucceedsForWritableDirectory() throws IOException {
        Path dir = createTempDir();
        DataFusionPlugin.probeSpillDirectoryWritable(dir.toString()); // no exception
    }

    public void testProbeThrowsForMissingDirectory() {
        Path missing = createTempDir().resolve("nope-" + randomAlphaOfLength(8));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataFusionPlugin.probeSpillDirectoryWritable(missing.toString())
        );
        assertThat(e.getMessage(), containsString(missing.toString()));
        assertThat(e.getMessage(), containsString("not writable"));
        assertNotNull("expected cause to be chained", e.getCause());
    }

    public void testProbeThrowsForReadOnlyDirectory() throws IOException {
        assumeTrue("POSIX permissions required", isPosixFs());
        Path dir = createTempDir();
        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
        try {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> DataFusionPlugin.probeSpillDirectoryWritable(dir.toString())
            );
            assertThat(e.getMessage(), containsString(dir.toString()));
            assertThat(e.getMessage(), containsString("not writable"));
        } finally {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    private static boolean isPosixFs() {
        try {
            Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
            return Environment.getFileStore(tmp).supportsFileAttributeView("posix");
        } catch (IOException ex) {
            return false;
        }
    }
}
