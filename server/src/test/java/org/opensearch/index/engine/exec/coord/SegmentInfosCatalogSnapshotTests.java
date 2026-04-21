/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link SegmentInfosCatalogSnapshot}.
 */
public class SegmentInfosCatalogSnapshotTests extends OpenSearchTestCase {

    public void testDelegation() {
        for (int iter = 0; iter < 100; iter++) {
            SegmentInfos segmentInfos = randomSegmentInfos();
            SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

            assertEquals(segmentInfos.getGeneration(), snapshot.getId());
            assertEquals(segmentInfos.getGeneration(), snapshot.getGeneration());
            assertEquals(segmentInfos.getVersion(), snapshot.getVersion());
            assertEquals(segmentInfos.getUserData(), snapshot.getUserData());
            assertEquals(-1L, snapshot.getLastWriterGeneration());
            assertSame(segmentInfos, snapshot.getSegmentInfos());

            expectThrows(UnsupportedOperationException.class, snapshot::getSegments);
            expectThrows(UnsupportedOperationException.class, () -> snapshot.getSearchableFiles(randomAlphaOfLength(5)));
            expectThrows(UnsupportedOperationException.class, snapshot::getDataFormats);
            expectThrows(UnsupportedOperationException.class, snapshot::serializeToString);

            // setUserData is a no-op, should not throw
            snapshot.setUserData(Map.of("key", "value"), false);
        }
    }

    public void testClone() {
        for (int iter = 0; iter < 100; iter++) {
            SegmentInfos segmentInfos = randomSegmentInfos();
            SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

            SegmentInfosCatalogSnapshot cloned = snapshot.clone();
            assertNotSame(snapshot, cloned);
            assertNotSame(segmentInfos, cloned.getSegmentInfos());
            assertEquals(snapshot.getGeneration(), cloned.getGeneration());
            assertEquals(snapshot.getVersion(), cloned.getVersion());
        }
    }

    public void testCopyWriteable() throws Exception {
        SegmentInfos segmentInfos = randomSegmentInfos();
        SegmentInfosCatalogSnapshot original = new SegmentInfosCatalogSnapshot(segmentInfos);

        SegmentInfosCatalogSnapshot copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            SegmentInfosCatalogSnapshot::new
        );

        assertEquals(original.getGeneration(), copy.getGeneration());
        assertEquals(original.getVersion(), copy.getVersion());
        assertEquals(original.getUserData(), copy.getUserData());
    }

    public void testGetFiles() throws Exception {
        SegmentInfos segmentInfos = randomSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        Collection<String> uploadNames = snapshot.getFiles(true);
        assertEquals(segmentInfos.files(true), new java.util.HashSet<>(uploadNames));
    }

    public void testSerializeProducesValidBytes() throws Exception {
        SegmentInfos segmentInfos = randomSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        byte[] bytes = snapshot.serialize();
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    public void testGetFormatVersionForUnmappedFileDefaultsToLatest() {
        SegmentInfos segmentInfos = randomSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        // File not in segmentFileVersionMap and not the segments file → falls through to LATEST.
        Version version = snapshot.getFormatVersionForFile("nonexistent_file.xyz");
        assertEquals(Version.LATEST, version);
    }

    public void testSetUserDataDelegatesToSegmentInfos() {
        SegmentInfos segmentInfos = randomSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        Map<String, String> newData = Map.of("key1", "val1", "key2", "val2");
        snapshot.setUserData(newData, false);
        assertEquals(newData, segmentInfos.getUserData());
        assertEquals(newData, snapshot.getUserData());
    }

    public void testCloneNoAcquireReturnsIndependentCopy() {
        SegmentInfos segmentInfos = randomSegmentInfos();
        SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);
        CatalogSnapshot cloned = snapshot.cloneNoAcquire();
        assertNotSame(snapshot, cloned);
        assertNotSame(segmentInfos, ((SegmentInfosCatalogSnapshot) cloned).getSegmentInfos());
        assertEquals(snapshot.getGeneration(), cloned.getGeneration());
        assertEquals(snapshot.getVersion(), cloned.getVersion());
    }

    // --- helpers ---

    private SegmentInfos randomSegmentInfos() {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        int userDataEntries = randomIntBetween(0, 5);
        Map<String, String> userData = new HashMap<>();
        for (int i = 0; i < userDataEntries; i++) {
            userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        segmentInfos.setUserData(userData, false);
        return segmentInfos;
    }
}
