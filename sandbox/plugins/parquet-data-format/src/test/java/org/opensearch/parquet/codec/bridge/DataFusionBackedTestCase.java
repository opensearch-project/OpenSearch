/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Base class for tests that open a real Parquet doc-values cursor.
 *
 * <p>Opening a cursor needs the DataFusion runtime manager and the global file-metadata cache the
 * analytics-backend-datafusion plugin owns; the reader has no private fallback, so a test that skips
 * this setup fails with "DataFusion runtime manager is not initialized" unless another test class
 * happened to start it first in the same JVM. Extending this class removes that ordering dependency.
 *
 * <p>Thread-leak detection is off because the Tokio runtime manager is a per-JVM singleton whose
 * threads outlive any one test class.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class DataFusionBackedTestCase extends OpenSearchTestCase {

    /** Arrow allocator for fixtures that export vectors to the native writer. */
    protected BufferAllocator allocator;

    private long globalRuntimePtr;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        // Safe to repeat: the native side replaces the manager under a write lock, so a second test
        // class simply installs a fresh one. Deliberately never shut down - the manager is process-wide
        // and tearing it down would break every later test in this JVM.
        DataFusionRuntimeFixture.initRuntimeManager(2);
        globalRuntimePtr = DataFusionRuntimeFixture.createGlobalRuntime(createTempDir("datafusion-spill"));
        assertNotEquals("global runtime must start before a cursor can be opened", 0L, globalRuntimePtr);
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        // Guarded so a failure in setUp surfaces itself rather than an NPE from tear-down.
        if (allocator != null) {
            allocator.close();
        }
        if (globalRuntimePtr != 0L) {
            DataFusionRuntimeFixture.closeGlobalRuntime(globalRuntimePtr);
        }
        super.tearDown();
    }
}
