/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.url;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.Matchers.containsString;

/**
 * Exercises URL blob path resolution over {@code file:} and {@code jar:} bases.
 *
 * <p>A repository-derived blob name is handed to {@link URLBlobContainer#readBlob(String)}, which
 * resolves it against the repository base URL.
 */
@SuppressForbidden(reason = "uses file:// URLs and local files to exercise blob path resolution")
public class URLBlobContainerTraversalReproTests extends OpenSearchTestCase {

    public void testFileUrlOutOfRootTraversalRepro() throws Exception {
        Path parent = createTempDir();
        Path repoBase = Files.createDirectories(parent.resolve("repo-base"));

        // A legitimate in-root blob (the control case).
        Files.write(repoBase.resolve("legit.dat"), "in-root-blob".getBytes(StandardCharsets.UTF_8));

        // The out-of-root target is a sibling of the repository root.
        byte[] sentinel = "OUT-OF-ROOT-SENTINEL".getBytes(StandardCharsets.UTF_8);
        Files.write(parent.resolve("secret.dat"), sentinel);

        URLBlobStore blobStore = new URLBlobStore(Settings.EMPTY, repoBase.toUri().toURL());
        BlobContainer container = blobStore.blobContainer(BlobPath.cleanPath());

        try (InputStream in = container.readBlob("legit.dat")) {
            assertEquals("in-root-blob", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        for (String invalidBlobName : List.of("../secret.dat", "../does-not-exist.dat", "//localhost/secret.dat")) {
            IOException exception = expectThrows(IOException.class, () -> container.readBlob(invalidBlobName));
            assertEquals("invalid blob name [" + invalidBlobName + "]", exception.getMessage());
        }
    }

    public void testFileUrlReadsSafePercentEncodedBlobNames() throws Exception {
        Path repoBase = createTempDir();
        Files.writeString(repoBase.resolve("name with space.dat"), "space", StandardCharsets.UTF_8);
        Files.writeString(repoBase.resolve("caf\u00E9.dat"), "unicode", StandardCharsets.UTF_8);
        Files.writeString(repoBase.resolve("plus+name.dat"), "plus", StandardCharsets.UTF_8);

        URLBlobStore blobStore = new URLBlobStore(Settings.EMPTY, repoBase.toUri().toURL());
        BlobContainer container = blobStore.blobContainer(BlobPath.cleanPath());

        assertBlobContents(container, "name%20with%20space.dat", "space");
        assertBlobContents(container, "caf%C3%A9.dat", "unicode");
        assertBlobContents(container, "caf%c3%a9.dat", "unicode");
        assertBlobContents(container, "plus+name.dat", "plus");

        for (String invalidBlobName : List.of("%2e%2e%2Fsecret.dat", "%252e%252e%252Fsecret.dat")) {
            IOException exception = expectThrows(IOException.class, () -> container.readBlob(invalidBlobName));
            assertThat(exception.getMessage(), containsString("invalid blob name"));
        }
    }

    public void testJarUrlReadsBlobWithinRoot() throws Exception {
        Path jarPath = createTempDir().resolve("repository.jar");
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(jarPath))) {
            writeEntry(output, "repository/legit.dat", "root");
            writeEntry(output, "repository/nested/legit.dat", "nested");
        }

        URLBlobStore blobStore = new URLBlobStore(
            Settings.EMPTY,
            URI.create("jar:" + jarPath.toUri() + "!/repository#fr\u00E1gment").toURL()
        );
        assertBlobContents(blobStore.blobContainer(BlobPath.cleanPath()), "legit.dat", "root");
        assertBlobContents(blobStore.blobContainer(BlobPath.cleanPath().add("nested")), "legit.dat", "nested");
    }

    private static void assertBlobContents(BlobContainer container, String blobName, String expected) throws IOException {
        try (InputStream in = container.readBlob(blobName)) {
            assertEquals(expected, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static void writeEntry(ZipOutputStream output, String name, String contents) throws IOException {
        output.putNextEntry(new ZipEntry(name));
        output.write(contents.getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
    }
}
