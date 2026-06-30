/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.UUID;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("removal")
public class FileInterceptorNegativeIntegTests {
    private static Path getTestDir() {
        Path baseDir = Path.of(System.getProperty("user.dir"));
        Path integTestFiles = baseDir.resolve("integ-test-files").normalize();
        return integTestFiles;
    }

    private String randomAlphaOfLength(int length) {
        // Using UUID to generate random string and taking first 'length' characters
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, length);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Policy policy = new Policy() {
            @Override
            public PermissionCollection getPermissions(ProtectionDomain domain) {
                Permissions permissions = new Permissions();
                return permissions;
            }
        };
        AgentPolicy.setPolicy(policy);
    }

    @Test
    public void testFileInputStream() {
        Path tmpDir = getTestDir();

        // Create a unique file name
        String fileName = "test-" + randomAlphaOfLength(8) + ".txt";
        Path tempPath = tmpDir.resolve(fileName);

        // Verify error when writing Content
        String content = "test content";
        SecurityException exception = assertThrows("", SecurityException.class, () -> {
            Files.write(tempPath, content.getBytes(StandardCharsets.UTF_8));
        });
        assertTrue(exception.getMessage().contains("Denied WRITE access to file"));
    }

    @Test
    public void testOpenForReadAndWrite() {
        Path tmpDir = getTestDir();

        // Create a unique file name
        String fileName = "test-" + randomAlphaOfLength(8) + ".txt";
        Path tempPath = tmpDir.resolve(fileName);

        // Verify error when opening file
        SecurityException exception = assertThrows("", SecurityException.class, () -> {
            FileChannel.open(tempPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        });
        assertTrue(exception.getMessage().contains("Denied OPEN (read/write) access to file"));
    }

    @Test
    public void testCopy() {
        Path tmpDir = getTestDir();
        Path sourcePath = tmpDir.resolve("test-source-" + randomAlphaOfLength(8) + ".txt");
        Path targetPath = tmpDir.resolve("test-target-" + randomAlphaOfLength(8) + ".txt");

        // Verify Error when copying file
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.copy(sourcePath, targetPath));
        assertTrue(exception.getMessage().contains("Denied COPY (read) access to file"));
    }

    @Test
    public void testCreateFile() {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-create-" + randomAlphaOfLength(8) + ".txt");

        // Verify Error when creating file
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.createFile(tempPath));
        assertTrue(exception.getMessage().contains("Denied WRITE access to file"));
    }

    @Test
    public void testMove() {
        Path tmpDir = getTestDir();
        Path sourcePath = tmpDir.resolve("test-source-" + randomAlphaOfLength(8) + ".txt");
        Path targetPath = tmpDir.resolve("test-target-" + randomAlphaOfLength(8) + ".txt");

        // Verify Error when moving file
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.move(sourcePath, targetPath));
        assertTrue(exception.getMessage().contains("Denied WRITE access to file"));
    }

    @Test
    public void testCreateLink() throws Exception {
        Path tmpDir = getTestDir();
        Path originalPath = tmpDir.resolve("test-original-" + randomAlphaOfLength(8) + ".txt");
        Path linkPath = tmpDir.resolve("test-link-" + randomAlphaOfLength(8) + ".txt");

        // Verify Error when creating link
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.createLink(linkPath, originalPath));
        assertTrue(exception.getMessage().contains("Denied WRITE access to file"));
    }

    @Test
    public void testDelete() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-delete-" + randomAlphaOfLength(8) + ".txt");

        // Verify Error when deleting file
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.delete(tempPath));
        assertTrue(exception.getMessage().contains("Denied DELETE access to file"));
    }

    @Test
    public void testReadAllLines() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-readAllLines-" + randomAlphaOfLength(8) + ".txt");

        // Verify Error when reading file
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.readAllLines(tempPath));
        assertTrue(exception.getMessage().contains("Denied OPEN (read) access to file"));
    }

    @Test
    public void testNewOutputStream() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-readAllLines-" + randomAlphaOfLength(8) + ".txt");

        // Verify error when calling newOutputStream
        SecurityException exception = assertThrows(SecurityException.class, () -> Files.newOutputStream(tempPath));
        assertTrue(exception.getMessage().contains("Denied OPEN (read/write) access to file"));
    }
}
