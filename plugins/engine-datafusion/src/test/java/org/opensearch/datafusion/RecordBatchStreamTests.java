/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

public class RecordBatchStreamTests extends OpenSearchTestCase {

    private DataFusionService service;
    private RootAllocator allocator;

    @Before
    public void setup() {
        service = new DataFusionService(java.util.Collections.emptyMap(), null);
        service.doStart();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void cleanUp() {
        allocator.close();
        service.doStop();
    }

    public void testConstructorIsNonBlocking() {
        long streamId = 1L;
        long runtimePtr = service.getRuntimePointer();

        long startTime = System.nanoTime();
        RecordBatchStream stream = new RecordBatchStream(streamId, runtimePtr, allocator);
        long duration = System.nanoTime() - startTime;

        assertNotNull(stream);
        assertTrue("Constructor should be non-blocking", duration < 100_000_000); // < 100ms
    }

    public void testIsInitializedBlocksUntilSchemaReady() throws Exception {
        long streamId = createMockStream();
        long runtimePtr = service.getRuntimePointer();

        RecordBatchStream stream = new RecordBatchStream(streamId, runtimePtr, allocator);

        boolean initialized = stream.isInitialized();
        assertTrue(initialized);
        assertNotNull(stream.getVectorSchemaRoot());

        stream.close();
    }

    public void testGetVectorSchemaRootInitializesAutomatically() throws Exception {
        long streamId = createMockStream();
        long runtimePtr = service.getRuntimePointer();

        RecordBatchStream stream = new RecordBatchStream(streamId, runtimePtr, allocator);

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertNotNull(root);

        stream.close();
    }

    public void testLoadNextBatchInitializesAutomatically() throws Exception {
        long streamId = createMockStream();
        long runtimePtr = service.getRuntimePointer();

        RecordBatchStream stream = new RecordBatchStream(streamId, runtimePtr, allocator);

        Boolean hasMore = stream.loadNextBatch().join();
        assertNotNull(hasMore);

        stream.close();
    }

    public void testCloseBeforeInitialization() throws Exception {
        long streamId = 1L;
        long runtimePtr = service.getRuntimePointer();

        RecordBatchStream stream = new RecordBatchStream(streamId, runtimePtr, allocator);
        stream.close(); // Should not throw
    }

    private long createMockStream() {
        // This would need actual stream creation logic
        // For now, return a placeholder
        return 1L;
    }
}
