/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

public class DataFusionPluginAdditionalHealthPathsTests extends OpenSearchTestCase {

    public void testReturnsEmptyWhenSpillDisabled() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        assertEquals(List.of(), plugin.getAdditionalHealthPaths(Settings.EMPTY));
    }

    public void testReturnsConfiguredSpillDirectory() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        Path dir = createTempDir();
        Settings settings = Settings.builder().put(DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.getKey(), dir.toString()).build();
        // Compare by string form: createTempDir() may return a Path on a randomized FileSystem,
        // while the SPI returns Path.of(dir.toString()) on the default FileSystem — equal as strings,
        // not equal via Path.equals().
        java.util.List<Path> result = plugin.getAdditionalHealthPaths(settings);
        assertEquals(1, result.size());
        assertEquals(dir.toString(), result.get(0).toString());
    }

    public void testPluginPolicyUsesDoubleBraceSpillDirectorySubstitution() {
        // Guards against a regression where someone reverts the FilePermission grant from
        // double-brace ${{...}} (generic substitution, expanded by PolicyFile.expandPermissionName)
        // to single-brace ${...} (only an allowlist of properties is expanded; arbitrary names
        // are left as literals, granting nothing). See PR #22025 review.
        //
        // The policy file is not on the test classpath and the test JVM's SecurityManager denies
        // reads outside the test sandbox, so the file's contents are read at Gradle configuration
        // time and injected via the test.policy.contents system property (see build.gradle). Skip
        // the assertion gracefully if the property is absent (e.g. running from an IDE without
        // the gradle config).
        String content = System.getProperty("test.policy.contents");
        assumeTrue("test.policy.contents system property not set (set in build.gradle)", content != null && !content.isEmpty());
        assertPolicyContents(content);
    }

    private static void assertPolicyContents(String content) {
        assertTrue(
            "plugin-security.policy must use double-brace ${{opensearch.datafusion.spill_directory}} "
                + "for FilePermission substitution; single-brace ${...} is hardcoded to a narrow "
                + "allowlist in PolicyFile and silently grants nothing for arbitrary property names.\n"
                + "Actual content:\n"
                + content,
            content.contains("${{opensearch.datafusion.spill_directory}}")
        );
        // Both grants must be present: the directory itself (so Files.exists / getFileStore work)
        // AND the recursive form (so probe files and spill files can be written).
        assertTrue(
            "plugin-security.policy must grant FilePermission on the spill directory itself "
                + "(\"${{opensearch.datafusion.spill_directory}}\" without /-).\n"
                + "Actual content:\n"
                + content,
            content.contains("\"${{opensearch.datafusion.spill_directory}}\"")
        );
        assertTrue(
            "plugin-security.policy must grant FilePermission on files under the spill directory "
                + "(\"${{opensearch.datafusion.spill_directory}}/-\").\n"
                + "Actual content:\n"
                + content,
            content.contains("\"${{opensearch.datafusion.spill_directory}}/-\"")
        );
        // Defense-in-depth: explicitly forbid single-brace usage of the same property,
        // since a regex like contains("${opensearch.datafusion.spill_directory}") would also match
        // the double-brace form (which contains the single-brace substring). We instead assert
        // the policy does not contain the *exact* single-brace token bracketed by non-{ chars.
        int idx = 0;
        while ((idx = content.indexOf("${opensearch.datafusion.spill_directory}", idx)) != -1) {
            // If the character before this match is also '{', it's part of a ${{...}} sequence — fine.
            boolean precededByOpenBrace = idx > 0 && content.charAt(idx - 1) == '{';
            // If the character after the matching '}' is also '}', it's part of a ${{...}}}} sequence — fine.
            int closeIdx = idx + "${opensearch.datafusion.spill_directory}".length() - 1;
            boolean followedByCloseBrace = closeIdx + 1 < content.length() && content.charAt(closeIdx + 1) == '}';
            if (!precededByOpenBrace || !followedByCloseBrace) {
                fail(
                    "Found single-brace ${opensearch.datafusion.spill_directory} at offset "
                        + idx
                        + "; must use double-brace ${{...}} form.\nContent:\n"
                        + content
                );
            }
            idx = closeIdx + 1;
        }
    }

    public void testGetAdditionalHealthPathsHandlesExplicitlyEmptyString() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        Settings settings = Settings.builder().put(DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.getKey(), "").build();
        assertEquals(List.of(), plugin.getAdditionalHealthPaths(settings));
    }
}
