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

import java.io.File;
import java.io.FileInputStream;
import java.io.FilePermission;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("removal")
public class FileInterceptorIntegTests {

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
                permissions.add(new FilePermission(System.getProperty("user.dir") + "/-", "read,write,delete"));
                return permissions;
            }

            @Override
            public boolean implies(ProtectionDomain domain, Permission permission) {
                final PermissionCollection pc = getPermissions(domain);

                if (pc == null) {
                    return false;
                }

                return pc.implies(permission);
            }
        };
        AgentPolicy.setPolicy(policy);
        Files.createDirectories(getTestDir());
    }

    @Test
    public void testFileInputStream() throws Exception {
        Path tmpDir = getTestDir();
        assertTrue("Tmp directory should exist", Files.exists(tmpDir));
        assertTrue("Tmp directory should be writable", Files.isWritable(tmpDir));

        // Create a unique file name
        String fileName = "test-" + randomAlphaOfLength(8) + ".txt";
        Path tempPath = tmpDir.resolve(fileName);

        // Ensure the file doesn't exist
        Files.deleteIfExists(tempPath);

        // Write content
        String content = "test content";
        Files.write(tempPath, content.getBytes(StandardCharsets.UTF_8));

        // Verify file creation
        assertTrue("File should exist", Files.exists(tempPath));
        assertTrue("File should be readable", Files.isReadable(tempPath));
        assertEquals("File should have correct content", content, Files.readString(tempPath, StandardCharsets.UTF_8));

        File tempFile = tempPath.toFile();

        try {
            try (FileInputStream fis = new FileInputStream(tempFile)) {
                byte[] buffer = new byte[100];
                int bytesRead = fis.read(buffer);
                String readContent = new String(buffer, 0, bytesRead);
                assertEquals("test content", readContent.trim());
            }
        } finally {
            // Clean up
            Files.deleteIfExists(tempPath);
        }
    }

    @Test
    public void testOpenForReadAndWrite() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-open-rw-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Test open for read and write
            try (
                FileChannel channel = FileChannel.open(
                    tempPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
                )
            ) {

                // Write content
                String content = "test content";
                ByteBuffer writeBuffer = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
                channel.write(writeBuffer);

                // Read content back
                channel.position(0); // Reset position to start
                ByteBuffer readBuffer = ByteBuffer.allocate(100);
                channel.read(readBuffer);
                readBuffer.flip();

                String readContent = StandardCharsets.UTF_8.decode(readBuffer).toString();
                assertEquals("Content should match", content, readContent);
            }
        } finally {
            Files.deleteIfExists(tempPath);
        }
    }

    @Test
    public void testCopy() throws Exception {
        Path tmpDir = getTestDir();
        Path sourcePath = tmpDir.resolve("test-source-" + randomAlphaOfLength(8) + ".txt");
        Path targetPath = tmpDir.resolve("test-target-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Create source file
            String content = "test content";
            Files.write(sourcePath, content.getBytes(StandardCharsets.UTF_8));

            // Test copy operation
            Files.copy(sourcePath, targetPath);
            assertThrows(
                SecurityException.class,
                () -> Files.copy(sourcePath, tmpDir.getRoot().resolve("test-target-" + randomAlphaOfLength(8) + ".txt"))
            );

            // Verify copy
            assertTrue("Target file should exist", Files.exists(targetPath));
            assertEquals("Content should match", Files.readString(sourcePath), Files.readString(targetPath));
        } finally {
            Files.deleteIfExists(sourcePath);
            Files.deleteIfExists(targetPath);
        }
    }

    @Test
    public void testCreateFile() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-create-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Test createFile operation
            Files.createFile(tempPath);

            // Verify file creation
            assertTrue("File should exist", Files.exists(tempPath));
            assertTrue("Should be a regular file", Files.isRegularFile(tempPath));
        } finally {
            Files.deleteIfExists(tempPath);
        }
    }

    @Test
    public void testMove() throws Exception {
        Path tmpDir = getTestDir();
        Path sourcePath = tmpDir.resolve("test-source-" + randomAlphaOfLength(8) + ".txt");
        Path targetPath = tmpDir.resolve("test-target-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Create source file
            String content = "test content";
            Files.write(sourcePath, content.getBytes(StandardCharsets.UTF_8));

            // Test move operation
            Files.move(sourcePath, targetPath);

            // Verify move
            assertFalse("Source file should not exist", Files.exists(sourcePath));
            assertTrue("Target file should exist", Files.exists(targetPath));
            assertEquals("Content should match", content, Files.readString(targetPath));
        } finally {
            Files.deleteIfExists(sourcePath);
            Files.deleteIfExists(targetPath);
        }
    }

    @Test
    public void testCreateLink() throws Exception {
        Path tmpDir = getTestDir();
        Path originalPath = tmpDir.resolve("test-original-" + randomAlphaOfLength(8) + ".txt");
        Path linkPath = tmpDir.resolve("test-link-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Create source file
            Files.write(originalPath, "test content".getBytes(StandardCharsets.UTF_8));

            // Test createLink operation
            Files.createLink(linkPath, originalPath);

            // Verify link creation
            assertTrue("Link should exist", Files.exists(linkPath));
            assertEquals("File contents should be same", Files.readString(originalPath), Files.readString(linkPath));
        } finally {
            Files.deleteIfExists(linkPath);
            Files.deleteIfExists(originalPath);
        }
    }

    @Test
    public void testDelete() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-delete-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Create a file with some content
            String content = "test content";
            Files.write(tempPath, content.getBytes(StandardCharsets.UTF_8));

            // Verify file exists before deletion
            assertTrue("File should exist before deletion", Files.exists(tempPath));
            assertEquals("File should have correct content", content, Files.readString(tempPath, StandardCharsets.UTF_8));

            // Test delete operation - FileInterceptor should intercept this
            Files.delete(tempPath);

            // Verify deletion
            assertFalse("File should not exist after deletion", Files.exists(tempPath));

        } finally {
            // Cleanup in case test fails
            Files.deleteIfExists(tempPath);
        }
    }

    @Test
    public void testReadAllLines() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-readAllLines-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Create a file with some content
            String content = "test content";
            Files.write(tempPath, content.getBytes(StandardCharsets.UTF_8));

            // Verify file exists before deletion
            assertTrue("File should exist before deletion", Files.exists(tempPath));
            assertEquals("File should have correct content", content, Files.readString(tempPath, StandardCharsets.UTF_8));

            // Test delete operation - FileInterceptor should intercept this
            String line = Files.readAllLines(tempPath).getFirst();

            // Verify readAllLines
            assertEquals("File contents should be returned", "test content", line);

        } finally {
            // Cleanup in case test fails
            Files.deleteIfExists(tempPath);
        }
    }

    @Test
    public void testNewOutputStream() throws Exception {
        Path tmpDir = getTestDir();
        Path tempPath = tmpDir.resolve("test-readAllLines-" + randomAlphaOfLength(8) + ".txt");

        try {
            // Create an empty file
            String content = "";
            Files.write(tempPath, content.getBytes(StandardCharsets.UTF_8));

            // Verify file exists before deletion
            assertTrue("File should exist before deletion", Files.exists(tempPath));
            assertEquals("File should have correct content", content, Files.readString(tempPath, StandardCharsets.UTF_8));

            // Test for new OutputStream
            try (OutputStream os = Files.newOutputStream(tempPath)) {
                os.write("test content".getBytes(StandardCharsets.UTF_8));
            }
            String line = Files.readAllLines(tempPath).getFirst();
            assertEquals("File contents should be returned", "test content", line);

        } finally {
            // Cleanup in case test fails
            Files.deleteIfExists(tempPath);
        }
    }
}
