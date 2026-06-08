/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FilePermission;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Permissions;
import java.util.Enumeration;

/**
 * Verifies the core-side FilePermission ledger entry for the operator-configured
 * {@code datafusion.spill_directory} setting. This grant is required because
 * {@link org.opensearch.node.Node#assertCanWritePluginHealthPaths} and
 * {@link org.opensearch.monitor.fs.FsHealthService} probe the spill directory from
 * core code (not plugin code), so the plugin policy alone is insufficient.
 *
 * <p>The behavior under test:
 * <ul>
 *   <li>Setting unset/empty: no grant added; no exception.</li>
 *   <li>Setting points at an existing directory: grant added covering both the
 *       directory itself and its recursive contents.</li>
 *   <li>Setting points at a nonexistent path: boot fails loudly with a clear message
 *       (no auto-create — the spill directory must be on a pre-mounted volume).</li>
 *   <li>Setting points at a regular file: same boot-failure behavior.</li>
 * </ul>
 */
@SuppressWarnings("removal")
public class SecuritySpillDirectoryGrantTests extends OpenSearchTestCase {

    public void testSpillSettingUnsetAddsNoGrant() throws Exception {
        Permissions policy = runAddFilePermissions(Settings.EMPTY);
        assertFalse(
            "no spill-directory FilePermission should be granted when the setting is unset",
            hasFilePermissionMatching(policy, "datafusion-spill-marker-not-present")
        );
    }

    public void testSpillSettingEmptyStringAddsNoGrant() throws Exception {
        Settings settings = Settings.builder().put("datafusion.spill_directory", "").build();
        Permissions policy = runAddFilePermissions(settings);
        assertFalse(
            "no spill-directory FilePermission should be granted when the setting is empty",
            hasFilePermissionMatching(policy, "datafusion-spill-marker-not-present")
        );
    }

    public void testSpillSettingPointsToExistingDirectoryAddsBothGrants() throws Exception {
        Path spillDir = createTempDir();
        Settings settings = Settings.builder().put("datafusion.spill_directory", spillDir.toString()).build();
        Permissions policy = runAddFilePermissions(settings);
        assertTrue(
            "expected FilePermission on the directory itself (" + spillDir + ")",
            hasFilePermissionExact(policy, spillDir.toString())
        );
        assertTrue(
            "expected recursive FilePermission on files under the directory (" + spillDir + "/-)",
            hasFilePermissionExact(policy, spillDir.toString() + spillDir.getFileSystem().getSeparator() + "-")
        );
    }

    public void testSpillSettingMissingDirectoryFailsBoot() {
        Path missing = createTempDir().resolve("does_not_exist_subdir");
        Settings settings = Settings.builder().put("datafusion.spill_directory", missing.toString()).build();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> runAddFilePermissions(settings));
        assertTrue(
            "expected error message to reference the missing path; got: " + ex.getMessage(),
            ex.getMessage().contains(missing.toString())
        );
        assertTrue(
            "expected error message to mention mounting the spill volume; got: " + ex.getMessage(),
            ex.getMessage().contains("spill volume")
        );
        // Confirm no auto-create happened on the wrong filesystem.
        assertFalse("Security.addFilePermissions must NOT auto-create the spill directory", Files.exists(missing));
    }

    public void testSpillSettingPointsToRegularFileFailsBoot() throws Exception {
        Path file = createTempDir().resolve("regular_file");
        Files.write(file, new byte[] { 0x00 });
        Settings settings = Settings.builder().put("datafusion.spill_directory", file.toString()).build();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> runAddFilePermissions(settings));
        assertTrue("expected error message to reference the bad path; got: " + ex.getMessage(), ex.getMessage().contains(file.toString()));
    }

    /**
     * Build a minimal {@link Environment} from a {@link Settings} object and run
     * {@link Security#addFilePermissions} into a fresh {@link Permissions}.
     */
    private Permissions runAddFilePermissions(Settings extraSettings) throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(extraSettings)
            .build();
        Environment environment = new Environment(nodeSettings, null);
        Permissions policy = new Permissions();
        Security.addFilePermissions(policy, environment);
        return policy;
    }

    private static boolean hasFilePermissionExact(Permissions policy, String name) {
        Enumeration<Permission> e = policy.elements();
        while (e.hasMoreElements()) {
            Permission p = e.nextElement();
            if (p instanceof FilePermission && p.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /** Returns true iff any FilePermission's name *contains* the given marker — used for negative assertions. */
    private static boolean hasFilePermissionMatching(Permissions policy, String marker) {
        Enumeration<Permission> e = policy.elements();
        while (e.hasMoreElements()) {
            Permission p = e.nextElement();
            if (p instanceof FilePermission && p.getName().contains(marker)) {
                return true;
            }
        }
        return false;
    }
}
