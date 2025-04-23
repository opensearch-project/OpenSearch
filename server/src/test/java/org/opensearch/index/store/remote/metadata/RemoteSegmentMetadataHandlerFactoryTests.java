/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.opensearch.common.io.IndexIOStreamHandler;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

/**
 * Unit tests for {@link org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactoryTests}.
 */
public class RemoteSegmentMetadataHandlerFactoryTests extends OpenSearchTestCase {

    private RemoteSegmentMetadataHandlerFactory segmentMetadataHandlerFactory;

    @Before
    public void setup() {
        segmentMetadataHandlerFactory = new RemoteSegmentMetadataHandlerFactory();
    }

    public void testGetHandlerReturnsBasedOnVersion() {
        IndexIOStreamHandler<RemoteSegmentMetadata> versionOneHandler = segmentMetadataHandlerFactory.getHandler(1);
        assertTrue(versionOneHandler instanceof RemoteSegmentMetadataHandler);
        IndexIOStreamHandler<RemoteSegmentMetadata> versionTwoHandler = segmentMetadataHandlerFactory.getHandler(2);
        assertTrue(versionTwoHandler instanceof RemoteSegmentMetadataHandler);
    }

    public void testGetHandlerWhenCalledMultipleTimesReturnsCachedHandler() {
        IndexIOStreamHandler<RemoteSegmentMetadata> versionTwoHandlerOne = segmentMetadataHandlerFactory.getHandler(2);
        IndexIOStreamHandler<RemoteSegmentMetadata> versionTwoHandlerTwo = segmentMetadataHandlerFactory.getHandler(2);
        assertEquals(versionTwoHandlerOne, versionTwoHandlerTwo);
    }

    public void testGetHandlerWhenHandlerNotProvidedThrowsException() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> { segmentMetadataHandlerFactory.getHandler(3); });
        assertEquals("Unsupported RemoteSegmentMetadata version: 3", throwable.getMessage());
    }
}
