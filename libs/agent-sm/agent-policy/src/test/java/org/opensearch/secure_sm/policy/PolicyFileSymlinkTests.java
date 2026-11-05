/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.FilePermission;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeNoException;

public class PolicyFileSymlinkTests {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testSymlinkedCodebaseWithSpaceMatchesRealPath() throws IOException {
        assertGrantMatchesThroughSymlink("real dir", "link dir");
    }

    @Test
    public void testSymlinkedCodebaseWithoutSpaceMatchesRealPath() throws IOException {
        assertGrantMatchesThroughSymlink("real", "link");
    }

    @Test
    public void testSymlinkedCodebaseWithPlusMatchesRealPath() throws IOException {
        assertGrantMatchesThroughSymlink("real+dir", "link+dir");
    }

    @Test
    public void testCodebaseWithoutPathDoesNotFail() throws IOException {
        final Path policyFile = temp.getRoot().toPath().resolve("nopath.policy");
        Files.writeString(
            policyFile,
            "grant codeBase \"file:%2Fsome%2Fdir/-\" {\n  permission java.io.FilePermission \"/granted\", \"read\";\n};\n",
            StandardCharsets.UTF_8
        );

        new PolicyFile(policyFile.toUri().toURL());
    }

    @SuppressWarnings("removal")
    private void assertGrantMatchesThroughSymlink(String realName, String linkName) throws IOException {
        final Path root = temp.getRoot().toPath().toRealPath();
        final Path realDir = Files.createDirectory(root.resolve(realName));
        final Path jar = Files.createFile(realDir.resolve("test.jar"));
        final Path linkDir = root.resolve(linkName);
        try {
            Files.createSymbolicLink(linkDir, realDir);
        } catch (UnsupportedOperationException | IOException e) {
            assumeNoException("symbolic links are not supported here", e);
        }

        final String codebase = linkDir.resolve("test.jar").toUri().toString();
        final Path policyFile = root.resolve("test.policy");
        Files.writeString(
            policyFile,
            "grant codeBase \"" + codebase + "\" {\n  permission java.io.FilePermission \"/granted\", \"read\";\n};\n",
            StandardCharsets.UTF_8
        );

        final java.security.Policy policy = new PolicyFile(policyFile.toUri().toURL());

        final CodeSource codeSource = new CodeSource(jar.toUri().toURL(), (Certificate[]) null);
        final PermissionCollection permissions = policy.getPermissions(new ProtectionDomain(codeSource, null));

        final long granted = permissions.elementsAsStream()
            .filter(FilePermission.class::isInstance)
            .filter(p -> p.getName().equals("/granted"))
            .count();
        assertThat("grant for " + codebase + " should apply to " + codeSource.getLocation(), granted, equalTo(1L));
    }
}
