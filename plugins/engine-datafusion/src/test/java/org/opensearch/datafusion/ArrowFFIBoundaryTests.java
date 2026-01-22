/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tests Arrow FFI boundary between Rust and Java.
 * Verifies that sliced arrays (from LIMIT OFFSET queries) work correctly
 * with Arrow 18.3.0's FFI handling.
 * 
 * Source array: ["zero", "one", "two", "three", "four"]
 */
public class ArrowFFIBoundaryTests extends OpenSearchTestCase {

    // head 2 from 1 - skip first, take 2
    public void testSliceOffset1Length2() throws Exception {
        assertSlicedArray(1, 2, new String[]{"one", "two"});
    }

    // head 1 from 0 - no offset (baseline)
    public void testSliceOffset0Length1() throws Exception {
        assertSlicedArray(0, 1, new String[]{"zero"});
    }

    // head 2 from 3 - larger offset
    public void testSliceOffset3Length2() throws Exception {
        assertSlicedArray(3, 2, new String[]{"three", "four"});
    }

    // head 1 from 4 - last element only
    public void testSliceOffset4Length1() throws Exception {
        assertSlicedArray(4, 1, new String[]{"four"});
    }

    // head 3 from 1 - middle section
    public void testSliceOffset1Length3() throws Exception {
        assertSlicedArray(1, 3, new String[]{"one", "two", "three"});
    }

    private void assertSlicedArray(int offset, int length, String[] expected) throws Exception {
        CompletableFuture<long[]> future = new CompletableFuture<>();
        
        NativeBridge.createTestSlicedArray(offset, length, new ActionListener<long[]>() {
            @Override
            public void onResponse(long[] pointers) {
                future.complete(pointers);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });

        long[] pointers = future.get(10, TimeUnit.SECONDS);

        try (BufferAllocator allocator = new RootAllocator();
             ArrowSchema arrowSchema = ArrowSchema.wrap(pointers[0]);
             ArrowArray arrowArray = ArrowArray.wrap(pointers[1])) {
            
            try (VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)) {
                assertEquals("Row count mismatch", expected.length, root.getRowCount());
                
                VarCharVector dataVector = (VarCharVector) root.getVector("data");
                for (int i = 0; i < expected.length; i++) {
                    assertEquals("Value mismatch at index " + i, expected[i], new String(dataVector.get(i)));
                }
            }
        }
    }
}
