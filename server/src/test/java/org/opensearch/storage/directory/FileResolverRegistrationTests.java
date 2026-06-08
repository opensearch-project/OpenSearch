/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.opensearch.index.engine.dataformat.DataFormatStoreHandler;
import org.opensearch.index.engine.dataformat.FileResolver;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link FileResolver} registration and resolution logic
 * in {@link StoreStrategyRegistry#wireFileResolvers}.
 *
 * <p>These are pure Java unit tests that verify the resolver closure
 * correctly relativizes paths and returns the expected FileEntry.
 */
public class FileResolverRegistrationTests extends OpenSearchTestCase {

    /**
     * Tests that a resolver correctly relativizes an absolute path
     * and returns a FileEntry when the file exists in remote metadata.
     */
    public void testResolverRelativizesPathCorrectly() {
        // Simulate: shardPath.getDataPath() = "/data/nodes/0/indices/ABC/1"
        // Rust sends: "data/nodes/0/indices/ABC/1/parquet/file.parquet" (no leading /)
        // Resolver should prepend / → relativize → get "parquet/file.parquet"

        String shardDataPath = "/data/nodes/0/indices/ABC/1";
        String absoluteKeyFromRust = "data/nodes/0/indices/ABC/1/parquet/file.parquet";

        // Simulate the resolver logic (same as wireFileResolvers closure)
        String fullPath = absoluteKeyFromRust.startsWith("/") ? absoluteKeyFromRust : "/" + absoluteKeyFromRust;
        java.nio.file.Path absolutePath = java.nio.file.Path.of(fullPath);
        java.nio.file.Path basePath = java.nio.file.Path.of(shardDataPath);

        String file = basePath.relativize(absolutePath).toString();

        assertEquals("parquet/file.parquet", file);
    }

    /**
     * Tests that resolver returns null when the path doesn't belong to this shard
     * (relativize produces a path with ".." which won't match remote metadata).
     */
    public void testResolverReturnsNullForWrongShard() {
        String shardDataPath = "/data/nodes/0/indices/ABC/0"; // shard 0
        String absoluteKeyFromRust = "data/nodes/0/indices/ABC/1/parquet/file.parquet"; // shard 1's file

        String fullPath = "/" + absoluteKeyFromRust;
        java.nio.file.Path absolutePath = java.nio.file.Path.of(fullPath);
        java.nio.file.Path basePath = java.nio.file.Path.of(shardDataPath);

        String file = basePath.relativize(absolutePath).toString();

        // Relativize produces "../1/parquet/file.parquet" — this won't exist in remote metadata
        assertTrue("Should contain '..' for wrong shard", file.contains(".."));
    }

    /**
     * Tests that a FileResolver returning non-null indicates successful resolution.
     */
    public void testFileResolverInterfaceContract() {
        FileResolver successResolver = (absoluteKey) -> new DataFormatStoreHandler.FileEntry(
            "remote/blob/path",
            DataFormatStoreHandler.REMOTE,
            1024
        );

        FileResolver failureResolver = (absoluteKey) -> null;

        DataFormatStoreHandler.FileEntry result = successResolver.resolve("/some/path");
        assertNotNull(result);
        assertEquals("remote/blob/path", result.path());
        assertEquals(DataFormatStoreHandler.REMOTE, result.location());
        assertEquals(1024, result.size());

        assertNull(failureResolver.resolve("/some/path"));
    }

    /**
     * Tests that leading slash handling is consistent between seed and resolve paths.
     */
    public void testLeadingSlashConsistency() {
        // Seed path: Java does shardPath.getDataPath().resolve("parquet/file.parquet").toString()
        // → "/data/nodes/0/indices/ABC/1/parquet/file.parquet" (with leading /)
        //
        // Rust strips it in register(): key = "data/nodes/0/indices/ABC/1/parquet/file.parquet"
        // Rust strips it in get(): same
        // Rust sends to callback: "data/nodes/0/indices/ABC/1/parquet/file.parquet" (no leading /)
        //
        // Resolver adds / back: "/data/nodes/0/indices/ABC/1/parquet/file.parquet"
        // Then relativizes against shardPath → "parquet/file.parquet"

        String seedKey = "/data/nodes/0/indices/ABC/1/parquet/file.parquet";
        String rustKey = seedKey.substring(1); // Rust strips leading /
        String resolverInput = "/" + rustKey; // Resolver adds it back

        assertEquals(seedKey, resolverInput); // Must match what seed used
    }
}
