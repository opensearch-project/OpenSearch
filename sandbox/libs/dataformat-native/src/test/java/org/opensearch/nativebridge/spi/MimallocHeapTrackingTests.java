/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Unit tests for per-plugin mimalloc heap tracking.
 *
 * <p>Calls Rust FFM functions directly via {@link SymbolLookup} — no OpenSearch
 * cluster needed. SymbolLookup is required because plugin-specific functions
 * (parquet_init_heap, df_allocate_test_buffer, etc.) are not exposed through
 * {@link NativeLibraryLoader}'s Java API, which only wraps the generic native_* functions.
 *
 * <p>Covers: heap registration, stats correctness, allocation tracking,
 * cross-plugin isolation, and global committed invariant.
 */
public class MimallocHeapTrackingTests extends OpenSearchTestCase {

    private static final MethodHandle PARQUET_INIT_HEAP;
    private static final MethodHandle DF_INIT_HEAP;
    /** Test-only: allocate a buffer on parquet's heap */
    private static final MethodHandle PARQUET_ALLOC;
    /** Test-only: free a buffer allocated by parquet */
    private static final MethodHandle PARQUET_FREE;
    /** Test-only: allocate a buffer on datafusion's heap */
    private static final MethodHandle DF_ALLOC;
    /** Test-only: free a buffer allocated by datafusion */
    private static final MethodHandle DF_FREE;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        PARQUET_INIT_HEAP = linker.downcallHandle(lib.find("parquet_init_heap").orElseThrow(), FunctionDescriptor.ofVoid());
        DF_INIT_HEAP = linker.downcallHandle(lib.find("df_init_heap").orElseThrow(), FunctionDescriptor.ofVoid());
        PARQUET_ALLOC = linker.downcallHandle(
            lib.find("parquet_allocate_test_buffer").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        PARQUET_FREE = linker.downcallHandle(
            lib.find("parquet_free_test_buffer").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        DF_ALLOC = linker.downcallHandle(
            lib.find("df_allocate_test_buffer").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        DF_FREE = linker.downcallHandle(
            lib.find("df_free_test_buffer").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initHeap(PARQUET_INIT_HEAP);
        initHeap(DF_INIT_HEAP);
    }

    public void testBothPluginHeapsAreRegistered() {
        int count = NativeLibraryLoader.heapCount();
        assertTrue("Expected at least 2 heaps, got " + count, count >= 2);
        assertTrue("parquet heap should be registered", findHeapIndex("parquet") >= 0);
        assertTrue("datafusion heap should be registered", findHeapIndex("datafusion") >= 0);
    }

    public void testHeapStatsAreNonNegative() {
        int count = NativeLibraryLoader.heapCount();
        for (int i = 0; i < count; i++) {
            String name = NativeLibraryLoader.heapName(i);
            assertTrue(name + " used >= 0", NativeLibraryLoader.heapUsed(i) >= 0);
            assertTrue(name + " committed >= 0", NativeLibraryLoader.heapCommitted(i) >= 0);
        }
    }

    public void testParquetAllocationIncreasesHeapUsage() throws Throwable {
        int idx = findHeapIndex("parquet");
        long before = NativeLibraryLoader.heapUsed(idx);

        long ptr = (long) PARQUET_ALLOC.invokeExact(256L * 1024);
        assertTrue("allocate should return non-zero", ptr != 0);

        long after = NativeLibraryLoader.heapUsed(idx);
        assertTrue("parquet used should increase: before=" + before + " after=" + after, after > before);

        PARQUET_FREE.invokeExact(ptr, 256L * 1024);
    }

    public void testDatafusionAllocationIncreasesHeapUsage() throws Throwable {
        int idx = findHeapIndex("datafusion");
        long before = NativeLibraryLoader.heapUsed(idx);

        long ptr = (long) DF_ALLOC.invokeExact(128L * 1024);
        assertTrue("allocate should return non-zero", ptr != 0);

        long after = NativeLibraryLoader.heapUsed(idx);
        assertTrue("datafusion used should increase: before=" + before + " after=" + after, after > before);

        DF_FREE.invokeExact(ptr, 128L * 1024);
    }

    public void testSumOfPluginHeapsDoesNotExceedGlobal() {
        int count = NativeLibraryLoader.heapCount();
        long sumUsed = 0, sumCommitted = 0;
        for (int i = 0; i < count; i++) {
            sumUsed += NativeLibraryLoader.heapUsed(i);
            sumCommitted += NativeLibraryLoader.heapCommitted(i);
        }
        assertTrue("sum used (" + sumUsed + ") <= sum committed (" + sumCommitted + ")", sumUsed <= sumCommitted);

        long global = NativeLibraryLoader.globalCommitted();
        assertTrue("sum committed (" + sumCommitted + ") <= global (" + global + ")", sumCommitted <= global);
    }

    public void testCrossPluginIsolation() throws Throwable {
        int pIdx = findHeapIndex("parquet");
        int dIdx = findHeapIndex("datafusion");
        long dfBefore = NativeLibraryLoader.heapUsed(dIdx);
        long pqBefore = NativeLibraryLoader.heapUsed(pIdx);

        // Allocate on parquet
        long ptr = (long) PARQUET_ALLOC.invokeExact(256L * 1024);

        // Datafusion heap should not grow from parquet's allocation.
        // mimalloc may retain small amounts of metadata per-thread, so allow up to 64KB slack.
        long dfAfter = NativeLibraryLoader.heapUsed(dIdx);
        long delta = dfAfter - dfBefore;
        assertTrue("datafusion should not grow from parquet alloc, delta=" + delta, delta < 64 * 1024);

        // Parquet heap should have grown
        long pqAfterAlloc = NativeLibraryLoader.heapUsed(pIdx);
        assertTrue("parquet used should increase after alloc", pqAfterAlloc > pqBefore);

        // Free parquet's buffer via datafusion's free — mimalloc resolves the owning
        // heap from the pointer's segment metadata, so this frees from parquet's heap.
        DF_FREE.invokeExact(ptr, 256L * 1024);

        // Parquet memory should decrease after free
        long pqAfterFree = NativeLibraryLoader.heapUsed(pIdx);
        assertTrue(
            "parquet used should decrease after free: afterAlloc=" + pqAfterAlloc + " afterFree=" + pqAfterFree,
            pqAfterFree < pqAfterAlloc
        );

        long dfAfterFree = NativeLibraryLoader.heapUsed(dIdx);
        long freeDelta = dfAfterFree - dfBefore;
        assertTrue("datafusion should not change from freeing parquet memory, delta=" + freeDelta, freeDelta < 64 * 1024);
    }

    private static void initHeap(MethodHandle handle) {
        try {
            handle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("initHeap failed", t);
        }
    }

    private int findHeapIndex(String name) {
        int count = NativeLibraryLoader.heapCount();
        for (int i = 0; i < count; i++) {
            if (name.equals(NativeLibraryLoader.heapName(i))) return i;
        }
        return -1;
    }
}
