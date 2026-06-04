/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexMetadataGenerationsTests extends OpenSearchTestCase {

    private final int MAX_TEST_INDICES = 10;
    private final String SNAPSHOT = "snapshot";
    private final String INDEX_PREFIX = "index-";
    private final String BLOB_ID_PREFIX = "blob-";
    private IndexMetaDataGenerations indexMetaDataGenerations;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        final int numIndices = randomIntBetween(1, MAX_TEST_INDICES);
        Map<IndexId, String> indexMap = createIndexMetadataMap(1, numIndices);
        Map<String, String> identifierMap = createIdentifierMapFromIndexMetadata(indexMap, BLOB_ID_PREFIX);
        Map<SnapshotId, Map<IndexId, String>> lookupMap = Collections.singletonMap(new SnapshotId(SNAPSHOT, SNAPSHOT), indexMap);
        indexMetaDataGenerations = new IndexMetaDataGenerations(lookupMap, identifierMap);
    }

    public void testEmpty() {
        assertTrue(IndexMetaDataGenerations.EMPTY.isEmpty());
        assertNull(IndexMetaDataGenerations.EMPTY.getIndexMetaBlobId("test"));
    }

    public void testBaseCase() {
        assertFalse(indexMetaDataGenerations.isEmpty());
        assertEquals(BLOB_ID_PREFIX + 1, indexMetaDataGenerations.getIndexMetaBlobId(String.valueOf(1)));
    }

    public void testIndexMetaBlobId() {
        SnapshotId snapshotId = new SnapshotId(SNAPSHOT, SNAPSHOT);
        IndexId indexId = new IndexId(INDEX_PREFIX + 1, INDEX_PREFIX + 1);
        assertEquals(BLOB_ID_PREFIX + 1, indexMetaDataGenerations.indexMetaBlobId(snapshotId, indexId));
    }

    public void testIndexMetaBlobIdFallback() {
        SnapshotId snapshotId = new SnapshotId(SNAPSHOT, SNAPSHOT);
        IndexId indexId = new IndexId("missingIndex", "missingIndex");
        assertEquals(SNAPSHOT, indexMetaDataGenerations.indexMetaBlobId(snapshotId, indexId));

        final String randomString = randomAlphaOfLength(8);
        snapshotId = new SnapshotId(randomString, randomString);
        assertEquals(randomString, indexMetaDataGenerations.indexMetaBlobId(snapshotId, indexId));
    }

    public void testWithAddedSnapshot() {
        // Construct a new snapshot
        SnapshotId newSnapshot = new SnapshotId("newSnapshot", "newSnapshot");
        final String newIndexMetadataPrefix = "newIndexMetadata-";
        final String newBlobIdPrefix = "newBlob-";
        final int numIndices = randomIntBetween(2, MAX_TEST_INDICES);
        Map<IndexId, String> newLookupMap = createIndexMetadataMap(2, numIndices);
        Map<String, String> identifierMap = createIdentifierMapFromIndexMetadata(newLookupMap, "newBlob-");

        // Add the snapshot and verify that values have been updated as expected
        IndexMetaDataGenerations updated = indexMetaDataGenerations.withAddedSnapshot(newSnapshot, newLookupMap, identifierMap);
        assertEquals(newBlobIdPrefix + 2, updated.getIndexMetaBlobId(String.valueOf(2)));
        assertEquals(newBlobIdPrefix + 2, updated.indexMetaBlobId(newSnapshot, new IndexId(INDEX_PREFIX + 2, INDEX_PREFIX + 2)));
        // The first index should remain unchanged
        assertEquals(BLOB_ID_PREFIX + 1, updated.getIndexMetaBlobId(String.valueOf(1)));
    }

    public void testWithRemovedSnapshot() {
        Set<SnapshotId> snapshotToRemove = Collections.singleton(new SnapshotId(SNAPSHOT, SNAPSHOT));
        assertEquals(IndexMetaDataGenerations.EMPTY, indexMetaDataGenerations.withRemovedSnapshots(snapshotToRemove));
    }

    private Map<IndexId, String> createIndexMetadataMap(int indexCountLowerBound, int numIndices) {
        final int indexCountUpperBound = indexCountLowerBound + numIndices;
        Map<IndexId, String> map = new HashMap<>();
        for (int i = indexCountLowerBound; i <= indexCountUpperBound; i++) {
            map.put(new IndexId(INDEX_PREFIX + i, INDEX_PREFIX + i), String.valueOf(i));
        }
        return map;
    }

    private Map<String, String> createIdentifierMapFromIndexMetadata(Map<IndexId, String> indexMetadataMap, String blobIdPrefix) {
        return indexMetadataMap.values().stream().collect(Collectors.toMap(k -> k, v -> blobIdPrefix + v));
    }
}
