/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class NodeAdditionalHealthPathsTests extends OpenSearchTestCase {

    public void testAssertCanWritePluginHealthPathsHappyPath() throws Exception {
        Path dir = createTempDir();
        Node.assertCanWritePluginHealthPaths(List.of(dir)); // must not throw
    }

    public void testAssertCanWritePluginHealthPathsRejectsNonexistent() {
        Path missing = createTempDir().resolve("does_not_exist");
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> Node.assertCanWritePluginHealthPaths(List.of(missing)));
        assertTrue(ex.getMessage(), ex.getMessage().contains(missing.toString()));
    }

    public void testAssertCanWritePluginHealthPathsRejectsRegularFile() throws Exception {
        Path file = createTempDir().resolve("plain_file");
        Files.write(file, new byte[] { 0x00 });
        expectThrows(IllegalStateException.class, () -> Node.assertCanWritePluginHealthPaths(List.of(file)));
    }

    public void testEmptyListIsNoOp() {
        Node.assertCanWritePluginHealthPaths(List.of()); // must not throw
    }
}
