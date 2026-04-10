/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Smoke test for the DataFusion JNI bridge.
 * Verifies native library loading, runtime creation, and reader lifecycle.
 */
public class DataFusionJniBridgeTests extends OpenSearchTestCase {

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
        long readerPtr = NativeBridge.createDatafusionReader(dataDir.toString(), new String[] { "test.parquet" });
        assertTrue("Reader pointer should be non-zero", readerPtr != 0);

        // Close reader and per-test runtime
        NativeBridge.closeDatafusionReader(readerPtr);
        NativeBridge.closeGlobalRuntime(runtimePtr);
    }
}
