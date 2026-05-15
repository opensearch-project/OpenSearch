/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Smoke test for the DataFusion JNI bridge.
 * Verifies native library loading, runtime creation, and reader lifecycle.
 */
public class DataFusionNativeBridgeTests extends OpenSearchTestCase {

    // Note: initTokioRuntimeManager uses OnceLock and can only be initialized once per JVM.
    // Do NOT call shutdownTokioRuntimeManager() here — it permanently kills the shared
    // executor and other test classes (DatafusionSearchExecEngineTests, etc.) will fail
    // with "Worker gone" if they run after this class.

    public void testRuntimeLifecycle() {
        // Init tokio runtime (no-op if already initialized by another test class)
        NativeBridge.initTokioRuntimeManager(2);

        // Create global runtime with small memory pool
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(
            64 * 1024 * 1024, // 64MB
            0L,
            spillDir.toString(),
            32 * 1024 * 1024 // 32MB spill
        );
        assertTrue("Runtime pointer should be non-zero", runtimePtr != 0);

        // Clean up the per-test runtime only
        NativeBridge.closeGlobalRuntime(runtimePtr);
    }

    public void testReaderLifecycle() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);

        // Copy test parquet to a temp dir
        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));

        // Create reader
        ReaderHandle readerHandle = new ReaderHandle(
            dataDir.toString(),
            java.util.List.of(MonoFileWriterSet.of(".", 0L, "test.parquet", 0L)),
            null
        );
        assertTrue("Reader pointer should be non-zero", readerHandle.getPointer() != 0);

        // Close reader
        readerHandle.close();

        NativeBridge.closeGlobalRuntime(runtimePtr);
    }

    public void testSessionContextCreationAndTableRegistration() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));

        ReaderHandle readerHandle = new ReaderHandle(
            dataDir.toString(),
            java.util.List.of(MonoFileWriterSet.of(".", 0L, "test.parquet", 0L)),
            null
        );
        // Create session context with table registered
        long queryConfigPtr;
        Arena arena = Arena.ofConfined();
        MemorySegment configSegment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
        WireConfigSnapshot.builder().build().writeTo(configSegment);
        queryConfigPtr = configSegment.address();

        SessionContextHandle sessionCtx = NativeBridge.createSessionContext(
            readerHandle.getPointer(),
            runtimeHandle.get(),
            "test_table",
            0L,
            queryConfigPtr
        );
        arena.close();
        assertTrue("SessionContext pointer should be non-zero", sessionCtx.getPointer() != 0);

        // Execute a simple query to verify the session context is properly configured
        byte[] substrait = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(),
            "test_table",
            "SELECT message FROM test_table",
            runtimeHandle.get()
        );
        // Capture the pointer value BEFORE execute — after execute the handle is marked consumed
        // (which closes the Java wrapper), so getPointer() would throw IllegalStateException.
        long sessionCtxPtrBefore = sessionCtx.getPointer();
        assertTrue("SessionContext pointer should be live before execute", NativeHandle.isLivePointer(sessionCtxPtrBefore));

        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeWithContextAsync(sessionCtx, substrait, new ActionListener<>() {
            @Override
            public void onResponse(Long streamPtr) {
                future.complete(streamPtr);
            }

            @Override
            public void onFailure(Exception exception) {
                future.completeExceptionally(exception);
            }
        });
        long streamPtr = future.join();
        assertTrue("Stream pointer should be non-zero", streamPtr != 0);

        // executeWithContextAsync marks the handle consumed (which closes the Java wrapper).
        // Verify the pointer is no longer in the live registry and the wrapper rejects getPointer().
        assertFalse("SessionContextHandle pointer must no longer be live after execute", NativeHandle.isLivePointer(sessionCtxPtrBefore));
        expectThrows(IllegalStateException.class, sessionCtx::getPointer);

        NativeBridge.streamClose(streamPtr);
        readerHandle.close();
        runtimeHandle.close();
    }

    /**
     * End-to-end test: create a real TieredObjectStore via FFM, wrap it in NativeStoreHandle,
     * and pass it to ReaderHandle. Proves the full native wiring works — Java → FFM → Rust
     * tiered storage → DataFusion reader with a real object store backing reads.
     */
    public void testReaderWithRealNativeStoreHandle() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);

        // Copy test parquet to a temp dir
        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));

        // Create a real TieredObjectStore via FFM (local-only, no remote)
        // ts_create_tiered_object_store(0, 0) → default LocalFileSystem, no remote
        long tieredStorePtr = NativeStoreTestHelper.createTieredObjectStore(0L, 0L);
        assertTrue("TieredObjectStore pointer should be non-zero", tieredStorePtr > 0);

        // Get a Box<Arc<dyn ObjectStore>> pointer for DataFusion
        long boxPtr = NativeStoreTestHelper.getObjectStoreBoxPtr(tieredStorePtr);
        assertTrue("Box pointer should be non-zero", boxPtr > 0);

        // Wrap in NativeStoreHandle with proper cleanup
        NativeStoreHandle storeHandle = new NativeStoreHandle(boxPtr, NativeStoreTestHelper::destroyObjectStoreBoxPtr);
        assertTrue("NativeStoreHandle should be live", storeHandle.isLive());

        // Create reader with the real store handle — this proves the full wiring
        ReaderHandle readerHandle = new ReaderHandle(
            dataDir.toString(),
            List.of(MonoFileWriterSet.of(dataDir.toString(), 1L, "test.parquet", 0L)),
            storeHandle
        );
        assertTrue("Reader pointer should be non-zero", readerHandle.getPointer() != 0);

        // Clean up in reverse order
        readerHandle.close();
        storeHandle.close();
        NativeStoreTestHelper.destroyTieredObjectStore(tieredStorePtr);
        NativeBridge.closeGlobalRuntime(runtimePtr);
    }

    // ═══════════════════════════════════════════════════════════════
    // FFM helpers for TieredObjectStore (inline — avoids dependency on parquet-data-format)
    // ═══════════════════════════════════════════════════════════════

    private static final java.lang.invoke.MethodHandle TS_CREATE;
    private static final java.lang.invoke.MethodHandle TS_DESTROY;
    private static final java.lang.invoke.MethodHandle TS_GET_BOX_PTR;
    private static final java.lang.invoke.MethodHandle TS_DESTROY_BOX_PTR;

    static {
        var lib = org.opensearch.nativebridge.spi.NativeLibraryLoader.symbolLookup();
        var linker = java.lang.foreign.Linker.nativeLinker();
        TS_CREATE = linker.downcallHandle(
            lib.find("ts_create_tiered_object_store").orElseThrow(),
            java.lang.foreign.FunctionDescriptor.of(
                java.lang.foreign.ValueLayout.JAVA_LONG,
                java.lang.foreign.ValueLayout.JAVA_LONG,
                java.lang.foreign.ValueLayout.JAVA_LONG
            )
        );
        TS_DESTROY = linker.downcallHandle(
            lib.find("ts_destroy_tiered_object_store").orElseThrow(),
            java.lang.foreign.FunctionDescriptor.of(java.lang.foreign.ValueLayout.JAVA_LONG, java.lang.foreign.ValueLayout.JAVA_LONG)
        );
        TS_GET_BOX_PTR = linker.downcallHandle(
            lib.find("ts_get_object_store_box_ptr").orElseThrow(),
            java.lang.foreign.FunctionDescriptor.of(java.lang.foreign.ValueLayout.JAVA_LONG, java.lang.foreign.ValueLayout.JAVA_LONG)
        );
        TS_DESTROY_BOX_PTR = linker.downcallHandle(
            lib.find("ts_destroy_object_store_box_ptr").orElseThrow(),
            java.lang.foreign.FunctionDescriptor.of(java.lang.foreign.ValueLayout.JAVA_LONG, java.lang.foreign.ValueLayout.JAVA_LONG)
        );
    }

    private static long createTieredObjectStore(long localPtr, long remotePtr) {
        try {
            return org.opensearch.nativebridge.spi.NativeLibraryLoader.checkResult((long) TS_CREATE.invokeExact(localPtr, remotePtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create TieredObjectStore", t);
        }
    }

    private static void destroyTieredObjectStore(long ptr) {
        try {
            org.opensearch.nativebridge.spi.NativeLibraryLoader.checkResult((long) TS_DESTROY.invokeExact(ptr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy TieredObjectStore", t);
        }
    }

    private static long getObjectStoreBoxPtr(long tieredStorePtr) {
        try {
            return org.opensearch.nativebridge.spi.NativeLibraryLoader.checkResult((long) TS_GET_BOX_PTR.invokeExact(tieredStorePtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get object store box ptr", t);
        }
    }

    private static void destroyObjectStoreBoxPtr(long ptr) {
        try {
            org.opensearch.nativebridge.spi.NativeLibraryLoader.checkResult((long) TS_DESTROY_BOX_PTR.invokeExact(ptr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy object store box ptr", t);
        }
    }
}
