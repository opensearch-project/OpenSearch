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
            snapshot.setUserData(Map.of("key", "value"));
        }
    }

    public void testClone() {
        for (int iter = 0; iter < 100; iter++) {
            SegmentInfos segmentInfos = randomSegmentInfos();
            SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

            SegmentInfosCatalogSnapshot cloned = snapshot.clone();
            assertNotSame(snapshot, cloned);
            assertSame(segmentInfos, cloned.getSegmentInfos());
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
