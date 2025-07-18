/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ActiveMergesRegistryTests extends OpenSearchTestCase {

    private ActiveMergesRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new ActiveMergesRegistry();
    }

    public void testRegisterAndContains() {
        String segmentName = "segment_1";
        assertFalse(registry.contains(segmentName));

        registry.register(segmentName);
        assertTrue(registry.contains(segmentName));

        ActiveMergesRegistry.SegmentEntry entry = registry.get(segmentName);
        assertNotNull(entry);
        assertEquals(ActiveMergesRegistry.SegmentState.REGISTERED, entry.getState());
        assertNull(entry.getMetadata());
    }

    public void testRegisterWithMetadata() {
        String segmentName = "segment_2";
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(segmentName, "remote_segment_2", "123", 456L);

        registry.register(segmentName, metadata);
        assertTrue(registry.contains(segmentName));

        ActiveMergesRegistry.SegmentEntry entry = registry.get(segmentName);
        assertNotNull(entry);
        assertEquals(ActiveMergesRegistry.SegmentState.REGISTERED, entry.getState());
        assertNotNull(entry.getMetadata());
        assertEquals(metadata, entry.getMetadata());
    }

    public void testUnregister() {
        String segmentName = "segment_3";
        registry.register(segmentName);
        assertTrue(registry.contains(segmentName));

        registry.unregister(segmentName);
        assertFalse(registry.contains(segmentName));
        assertNull(registry.get(segmentName));
    }

    public void testSetStateProcessed() {
        String segmentName = "segment_4";
        registry.register(segmentName);

        registry.setStateProcessed(segmentName);

        ActiveMergesRegistry.SegmentEntry entry = registry.get(segmentName);
        assertNotNull(entry);
        assertEquals(ActiveMergesRegistry.SegmentState.PROCESSED, entry.getState());
    }

    public void testSetStateProcessedNonExistentSegment() {
        String segmentName = "non_existent_segment";
        registry.setStateProcessed(segmentName);
        // Should not throw an exception
        assertFalse(registry.contains(segmentName));
    }

    public void testGetRegisteredSegments() {
        assertTrue(registry.isEmpty());
        assertEquals(0, registry.getRegisteredSegments().size());

        registry.register("segment_5");
        registry.register("segment_6");
        registry.register("segment_7");

        Set<String> registeredSegments = registry.getRegisteredSegments();
        assertEquals(3, registeredSegments.size());
        assertTrue(registeredSegments.contains("segment_5"));
        assertTrue(registeredSegments.contains("segment_6"));
        assertTrue(registeredSegments.contains("segment_7"));
    }

    public void testIsEmpty() {
        assertTrue(registry.isEmpty());

        registry.register("segment_8");
        assertFalse(registry.isEmpty());

        registry.unregister("segment_8");
        assertTrue(registry.isEmpty());
    }

    public void testSegmentEntryToString() {
        String segmentName = "segment_9";
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(segmentName, "remote_segment_9", "123", 456L);

        ActiveMergesRegistry.SegmentEntry entry = new ActiveMergesRegistry.SegmentEntry(
            segmentName,
            ActiveMergesRegistry.SegmentState.REGISTERED,
            metadata
        );

        String toString = entry.toString();
        assertTrue(toString.contains(segmentName));
        assertTrue(toString.contains(metadata.toString()));
        assertTrue(toString.contains(ActiveMergesRegistry.SegmentState.REGISTERED.toString()));
    }
}
