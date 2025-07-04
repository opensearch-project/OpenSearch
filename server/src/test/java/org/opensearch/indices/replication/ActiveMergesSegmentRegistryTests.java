/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ActiveMergesSegmentRegistryTests {

    private ActiveMergesSegmentRegistry registry;
    private UploadedSegmentMetadata mockMetadata;

    @Before
    public void setUp() {
        registry = ActiveMergesSegmentRegistry.getInstance();
        // Clear registry state before each test
        clearRegistry();

        mockMetadata = mock(UploadedSegmentMetadata.class);
        when(mockMetadata.getUploadedFilename()).thenReturn("remote_segment_1.si");
    }

    private void clearRegistry() {
        // Clear all registered segments
        Map<String, UploadedSegmentMetadata> metadataMap = registry.segmentMetadataMap();
        metadataMap.keySet().forEach(registry::unregister);
        registry.filenameRegistry.clear();
    }

    @Test
    public void testSingletonInstance() {
        ActiveMergesSegmentRegistry instance1 = ActiveMergesSegmentRegistry.getInstance();
        ActiveMergesSegmentRegistry instance2 = ActiveMergesSegmentRegistry.getInstance();
        assertSame(instance1, instance2);
    }

    @Test
    public void testRegisterSegment() {
        String filename = "segment_1.si";
        registry.register(filename);
        assertTrue(registry.contains(filename));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterDuplicateSegment() {
        String filename = "segment_1.si";
        registry.register(filename);
        registry.register(filename); // Should throw exception
    }

    @Test
    public void testUpdateMetadata() {
        String filename = "segment_1.si";
        registry.register(filename);
        registry.updateMetadata(filename, mockMetadata);

        assertEquals(mockMetadata, registry.getMetadata(filename));
        assertTrue(registry.contains("remote_segment_1.si"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateMetadataUnregisteredSegment() {
        registry.updateMetadata("unregistered_segment.si", mockMetadata);
    }

    @Test
    public void testUnregisterSegment() {
        String filename = "segment_1.si";
        registry.register(filename);
        registry.updateMetadata(filename, mockMetadata);

        registry.unregister(filename);

        assertFalse(registry.contains(filename));
        assertFalse(registry.contains("remote_segment_1.si"));
        assertNull(registry.getMetadata(filename));
    }

    @Test
    public void testUnregisterNonExistentSegment() {
        // Should not throw exception
        registry.unregister("non_existent.si");
    }

    @Test
    public void testGetExistingRemoteSegmentFilename() {
        String filename = "segment_1.si";
        registry.register(filename);
        registry.updateMetadata(filename, mockMetadata);

        assertEquals("remote_segment_1.si", registry.getExistingRemoteSegmentFilename(filename));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetExistingRemoteSegmentFilenameNoMetadata() {
        String filename = "segment_1.si";
        registry.register(filename);
        registry.getExistingRemoteSegmentFilename(filename); // Metadata not available
    }

    @Test
    public void testCanDelete() {
        String filename = "segment_1.si";
        assertTrue(registry.canDelete(filename)); // Not registered

        registry.register(filename);
        assertFalse(registry.canDelete(filename)); // Registered

        registry.unregister(filename);
        assertTrue(registry.canDelete(filename)); // Unregistered
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        String filename = "segment_" + threadId + "_" + j + ".si";
                        String remoteFilename = "remote_" + filename;
                        UploadedSegmentMetadata metadata = mock(UploadedSegmentMetadata.class);
                        when(metadata.getUploadedFilename()).thenReturn(remoteFilename);
                        registry.register(filename);
                        assertTrue(registry.contains(filename));
                        registry.updateMetadata(filename, metadata);
                        assertEquals(registry.getExistingRemoteSegmentFilename(filename), remoteFilename);
                        registry.unregister(filename);
                        assertFalse(registry.contains(filename));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
    }

    @Test
    public void testMultipleSegmentsLifecycle() {
        String[] filenames = {"seg1.si", "seg2.si", "seg3.si"};
        UploadedSegmentMetadata[] metadatas = new UploadedSegmentMetadata[3];

        // Setup mocks
        for (int i = 0; i < 3; i++) {
            metadatas[i] = mock(UploadedSegmentMetadata.class);
            when(metadatas[i].getUploadedFilename()).thenReturn("remote_" + filenames[i]);
        }

        // Register all
        for (String filename : filenames) {
            registry.register(filename);
            assertTrue(registry.contains(filename));
        }

        // Update metadata
        for (int i = 0; i < 3; i++) {
            registry.updateMetadata(filenames[i], metadatas[i]);
            assertEquals(metadatas[i], registry.getMetadata(filenames[i]));
        }

        // Verify all are tracked
        assertEquals(3, registry.segmentMetadataMap().size());

        // Unregister one
        registry.unregister(filenames[1]);
        assertFalse(registry.contains(filenames[1]));
        assertEquals(2, registry.segmentMetadataMap().size());

        // Others still exist
        assertTrue(registry.contains(filenames[0]));
        assertTrue(registry.contains(filenames[2]));
    }
}
