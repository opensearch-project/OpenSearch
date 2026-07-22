/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for the layer-3 correctness primitive (content-addressed warehouse paths).
 * Verifies the resolver is deterministic and passes through the remote-store filename
 * unchanged — the remote store already guarantees per-upload uniqueness via its UUID
 * suffix, so any further mangling on our side would break retry idempotency.
 */
public class WarehousePathResolverTests extends OpenSearchTestCase {

    public void testResolvesDeterministically() {
        String path = WarehousePathResolver.resolve("uuid-abc", 3, "_0.cfe__gX7bNIIBrs0AUNsR2yEG");
        assertEquals("data/uuid-abc/3/_0.cfe__gX7bNIIBrs0AUNsR2yEG", path);
    }

    public void testSameInputsProduceSameOutput() {
        String a = WarehousePathResolver.resolve("uuid-x", 0, "_5.parquet__AAA");
        String b = WarehousePathResolver.resolve("uuid-x", 0, "_5.parquet__AAA");
        assertEquals("resolver must be deterministic", a, b);
    }

    public void testRemoteFilenamePassedThroughUnchanged() {
        // Remote store uses base64-ish UUIDs with underscores/mixed case; must not be
        // URL-encoded, lowercased, or otherwise transformed.
        String weirdName = "_99.cfs__aB_0-xyz+AAA==";
        String path = WarehousePathResolver.resolve("u", 7, weirdName);
        assertTrue("filename is present verbatim", path.endsWith("/" + weirdName));
    }

    public void testPartitionPrefix() {
        assertEquals("data/uuid-1/0/", WarehousePathResolver.partitionPrefix("uuid-1", 0));
    }

    public void testRejectsNullIndexUuid() {
        expectThrows(NullPointerException.class, () -> WarehousePathResolver.resolve(null, 0, "f"));
    }

    public void testRejectsEmptyIndexUuid() {
        expectThrows(IllegalArgumentException.class, () -> WarehousePathResolver.resolve("", 0, "f"));
    }

    public void testRejectsNullFilename() {
        expectThrows(NullPointerException.class, () -> WarehousePathResolver.resolve("u", 0, null));
    }

    public void testRejectsEmptyFilename() {
        expectThrows(IllegalArgumentException.class, () -> WarehousePathResolver.resolve("u", 0, ""));
    }

    public void testRejectsNegativeShardId() {
        expectThrows(IllegalArgumentException.class, () -> WarehousePathResolver.resolve("u", -1, "f"));
    }
}
