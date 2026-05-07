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
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
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
        ReaderHandle readerHandle = new ReaderHandle(dataDir.toString(), new String[] { "test.parquet" });
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

        ReaderHandle readerHandle = new ReaderHandle(dataDir.toString(), new String[] { "test.parquet" });

        // Create session context with table registered
        SessionContextHandle sessionCtx = NativeBridge.createSessionContext(
            readerHandle.getPointer(),
            runtimeHandle.get(),
            "test_table",
            0L
        );
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
     * Regression test for use-after-free in execute_with_context.
     * Without the fix, the SessionContext is dropped when execute_with_context returns,
     * but the stream still references SessionState internals (optimizer rules, RuntimeEnv).
     * Draining the stream after execution triggers a SIGSEGV when the freed memory is
     * accessed — typically in drop_in_place
     * With the fix, the SessionContext is moved into the QueryStreamHandle and stays
     * alive until the stream is closed.
     */
    public void testSessionContextSurvivesStreamDrain() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        NativeRuntimeHandle runtimeHandle = new NativeRuntimeHandle(runtimePtr);

        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));

        ReaderHandle readerHandle = new ReaderHandle(dataDir.toString(), new String[] { "test.parquet" });

        SessionContextHandle sessionCtx = NativeBridge.createSessionContext(
            readerHandle.getPointer(),
            runtimeHandle.get(),
            "test_table",
            0L
        );

        byte[] substrait = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(),
            "test_table",
            "SELECT message FROM test_table",
            runtimeHandle.get()
        );

        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeWithContextAsync(sessionCtx.getPointer(), substrait, new ActionListener<>() {
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
        // Register the stream pointer with NativeHandle's live-handle registry so
        // streamNext()'s validatePointer() accepts it. Mirrors the production path
        // in DatafusionSearcher#searchWithSessionContext.
        StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
        sessionCtx.close();

        // Drain the stream — this is where the SIGSEGV occurred before the fix.
        // streamNext returns 0 when exhausted.
        int batchCount = 0;
        while (true) {
            CompletableFuture<Long> nextFuture = new CompletableFuture<>();
            NativeBridge.streamNext(runtimePtr, streamHandle.getPointer(), new ActionListener<>() {
                @Override
                public void onResponse(Long batchPtr) {
                    nextFuture.complete(batchPtr);
                }

                @Override
                public void onFailure(Exception exception) {
                    nextFuture.completeExceptionally(exception);
                }
            });
            long batchPtr = nextFuture.join();
            if (batchPtr == 0) {
                break;
            }
            batchCount++;
        }
        assertTrue("Should have drained at least one batch", batchCount > 0);

        streamHandle.close();
        readerHandle.close();
        runtimeHandle.close();
    }
}
