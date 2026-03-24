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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Property-based tests for {@link SegmentInfosCatalogSnapshot}.
 * Uses OpenSearch randomization utilities to generate random inputs across many iterations.
 */
public class SegmentInfosCatalogSnapshotTests extends OpenSearchTestCase {

    // Feature: catalog-snapshot-manager, Property 10: SegmentInfosCatalogSnapshot delegates to SegmentInfos

    /**
     * Creates a random {@link SegmentInfos} instance with random user data.
     */
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

    /**
     * Property 10: SegmentInfosCatalogSnapshot delegates to SegmentInfos.
     * For any valid Lucene SegmentInfos instance, constructing a SegmentInfosCatalogSnapshot
     * and querying its accessors should return consistent results delegated from SegmentInfos.
     *
     * Validates: Requirements 7.1, 7.2, 7.3, 7.4
     */
    public void testSegmentInfosDelegation() {
        for (int iter = 0; iter < 100; iter++) {
            SegmentInfos segmentInfos = randomSegmentInfos();
            SegmentInfosCatalogSnapshot snapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

            try {
                // Verify getId() returns the SegmentInfos generation
                assertEquals("getId() should return the SegmentInfos generation", segmentInfos.getGeneration(), snapshot.getId());

                // Verify getGeneration() returns the SegmentInfos generation
                assertEquals(
                    "getGeneration() should return the SegmentInfos generation",
                    segmentInfos.getGeneration(),
                    snapshot.getGeneration()
                );

                // Verify getVersion() returns the SegmentInfos version
                assertEquals("getVersion() should return the SegmentInfos version", segmentInfos.getVersion(), snapshot.getVersion());

                // Verify getUserData() returns the SegmentInfos user data
                assertEquals("getUserData() should return the SegmentInfos user data", segmentInfos.getUserData(), snapshot.getUserData());

                // Verify getLastWriterGeneration() returns -1
                assertEquals("getLastWriterGeneration() should return -1", -1L, snapshot.getLastWriterGeneration());

                // Verify getSegmentInfos() returns the wrapped instance
                assertSame("getSegmentInfos() should return the wrapped SegmentInfos", segmentInfos, snapshot.getSegmentInfos());

                // Verify getSegments() throws UnsupportedOperationException
                expectThrows(UnsupportedOperationException.class, () -> snapshot.getSegments());

                // Verify getSearchableFiles() throws UnsupportedOperationException
                String randomFormat = randomAlphaOfLength(5);
                expectThrows(UnsupportedOperationException.class, () -> snapshot.getSearchableFiles(randomFormat));

                // Verify getDataFormats() throws UnsupportedOperationException
                expectThrows(UnsupportedOperationException.class, () -> snapshot.getDataFormats());

                // Verify serializeToString() throws UnsupportedOperationException
                expectThrows(UnsupportedOperationException.class, () -> snapshot.serializeToString());

                // Verify writeTo() throws UnsupportedOperationException
                expectThrows(UnsupportedOperationException.class, () -> snapshot.writeTo(new BytesStreamOutput()));

                // Verify clone() returns a new instance wrapping the same SegmentInfos
                SegmentInfosCatalogSnapshot cloned = snapshot.clone();
                try {
                    assertNotSame("clone() should return a new instance", snapshot, cloned);
                    assertSame("clone() should wrap the same SegmentInfos", segmentInfos, cloned.getSegmentInfos());
                    assertEquals("clone() should have the same generation", snapshot.getGeneration(), cloned.getGeneration());
                    assertEquals("clone() should have the same version", snapshot.getVersion(), cloned.getVersion());
                } finally {
                    cloned.decRef();
                }

                // Verify setUserData() is a no-op (does not throw)
                snapshot.setUserData(Map.of("key", "value"));
            } finally {
                snapshot.decRef();
            }
        }
    }
}
