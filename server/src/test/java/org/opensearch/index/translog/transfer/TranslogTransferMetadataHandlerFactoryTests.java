/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.io.IndexIOStreamHandler;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

/**
 * Unit tests for {@link org.opensearch.index.translog.transfer.TranslogTransferMetadataHandlerFactoryTests}.
 */
public class TranslogTransferMetadataHandlerFactoryTests extends OpenSearchTestCase {

    private TranslogTransferMetadataHandlerFactory translogTransferMetadataHandlerFactory;

    @Before
    public void setup() {
        translogTransferMetadataHandlerFactory = new TranslogTransferMetadataHandlerFactory();
    }

    public void testGetHandlerReturnsBasedOnVersion() {
        IndexIOStreamHandler<TranslogTransferMetadata> versionOneHandler = translogTransferMetadataHandlerFactory.getHandler(1);
        assertTrue(versionOneHandler instanceof TranslogTransferMetadataHandler);
    }

    public void testGetHandlerWhenCalledMultipleTimesReturnsCachedHandler() {
        IndexIOStreamHandler<TranslogTransferMetadata> versionTwoHandlerOne = translogTransferMetadataHandlerFactory.getHandler(1);
        IndexIOStreamHandler<TranslogTransferMetadata> versionTwoHandlerTwo = translogTransferMetadataHandlerFactory.getHandler(1);
        assertEquals(versionTwoHandlerOne, versionTwoHandlerTwo);
    }

    public void testGetHandlerWhenHandlerNotProvidedThrowsException() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> { translogTransferMetadataHandlerFactory.getHandler(2); });
        assertEquals("Unsupported TranslogTransferMetadata version: 2", throwable.getMessage());
    }
}
