/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Smoke test for the DataFusion JNI bridge.
 * Verifies native library loading, runtime creation, and reader lifecycle.
 */
public class DataFusionNativeBridgeTests extends OpenSearchTestCase {

    public void testRuntimeLifecycle() {
        // Init tokio runtime
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

        // Clean up
        NativeBridge.closeGlobalRuntime(runtimePtr);
        NativeBridge.shutdownTokioRuntimeManager();
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
        NativeBridge.shutdownTokioRuntimeManager();
    }
}
