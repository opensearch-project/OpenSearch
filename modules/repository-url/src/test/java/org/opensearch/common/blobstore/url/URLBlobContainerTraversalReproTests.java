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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Exercises URL blob path resolution over a {@code file://} base.
 *
 * <p>A repository-derived blob name is handed to {@link URLBlobContainer#readBlob(String)}, which
 * resolves it against the repository base URL. The test logs the outcome for both in-root and
 * out-of-root paths so path validation behavior can be inspected directly.
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

        logger.info("base URL = {}", blobStore.path());

        // 1) Control: legitimate in-root read must keep working.
        try (InputStream in = container.readBlob("legit.dat")) {
            String got = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            logger.info("in-root readBlob(legit.dat) -> RETURNED [{}]", got);
        } catch (Exception e) {
            logger.info("in-root readBlob(legit.dat) -> THREW {}: {}", e.getClass().getSimpleName(), e.getMessage());
        }

        // 2) Out-of-root traversal: a ../ path resolves outside the repository root without validation.
        String traversal = "../secret.dat";
        try (InputStream in = container.readBlob(traversal)) {
            String got = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            logger.info("out-of-root readBlob({}) -> RETURNED [{}]", traversal, got);
        } catch (Exception e) {
            logger.info("out-of-root readBlob({}) -> THREW {}: {}", traversal, e.getClass().getSimpleName(), e.getMessage());
        }

        // 3) Existing and missing out-of-root paths.
        String missing = "../does-not-exist.dat";
        try (InputStream in = container.readBlob(missing)) {
            in.readAllBytes();
            logger.info("oracle readBlob({}) -> RETURNED (unexpected)", missing);
        } catch (Exception e) {
            logger.info("oracle readBlob({}) -> THREW {}: {}", missing, e.getClass().getSimpleName(), e.getMessage());
        }
    }
}
