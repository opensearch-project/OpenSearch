/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FilePermission;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.Permission;
import java.security.Policy;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Enumeration;

/**
 * End-to-end test for the policy substitution mechanism that backs the DataFusion plugin's
 * narrowed FilePermission grant. Verifies that:
 *  - When opensearch.datafusion.spill_directory is set, PolicyFile resolves
 *    {@code ${{opensearch.datafusion.spill_directory}}/-} to the configured path + "/-".
 *  - When the property is unset, the resolver leaves the literal {@code ${{...}}/-} in place,
 *    granting no real filesystem path.
 *
 * This is the test that would catch a regression where the plugin policy is written with
 * single-brace ${...} instead of double-brace ${{...}} (which is hardcoded to a small allowlist
 * in PolicyFile and would silently produce a useless permission).
 */
@SuppressWarnings("removal")
@SuppressForbidden(reason = "https://github.com/opensearch-project/OpenSearch/issues/19640")
public class SecurityDataFusionSpillDirSubstitutionTests extends OpenSearchTestCase {

    private static final String PROPERTY = "opensearch.datafusion.spill_directory";

    public void testFilePermissionResolvesWhenSpillDirectoryConfigured() throws Exception {
        Path policy = writePluginStylePolicy();
        Settings settings = Settings.builder().put("datafusion.spill_directory", "/tmp/test-spill").build();
        Policy parsed = Security.readPolicy(policy.toUri().toURL(), Collections.emptyMap(), settings);
        FilePermission resolved = firstFilePermission(parsed);
        assertEquals("/tmp/test-spill/-", resolved.getName());
        // The property must not leak past readPolicy.
        assertNull(System.getProperty(PROPERTY));
    }

    public void testFilePermissionLeavesLiteralWhenSpillDirectoryEmpty() throws Exception {
        Path policy = writePluginStylePolicy();
        Policy parsed = Security.readPolicy(policy.toUri().toURL(), Collections.emptyMap(), Settings.EMPTY);
        FilePermission resolved = firstFilePermission(parsed);
        assertEquals("${{" + PROPERTY + "}}/-", resolved.getName());
        assertNull(System.getProperty(PROPERTY));
    }

    private Path writePluginStylePolicy() throws Exception {
        Path policy = createTempDir().resolve("plugin.policy");
        Files.writeString(
            policy,
            "grant {\n" + "  permission java.io.FilePermission \"${{" + PROPERTY + "}}/-\", \"read,write,delete\";\n" + "};\n",
            StandardCharsets.UTF_8
        );
        return policy;
    }

    private static FilePermission firstFilePermission(Policy policy) {
        CodeSource cs = new CodeSource(null, (Certificate[]) null);
        Enumeration<Permission> permissions = policy.getPermissions(cs).elements();
        while (permissions.hasMoreElements()) {
            Permission p = permissions.nextElement();
            if (p instanceof FilePermission) {
                return (FilePermission) p;
            }
        }
        throw new AssertionError("No FilePermission found in parsed policy");
    }
}
